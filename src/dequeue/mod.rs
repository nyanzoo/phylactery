use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{Read, Write},
    os::unix::prelude::FileExt,
    path::Path,
    ptr::NonNull,
    sync::{
        atomic::{AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
};

use crate::{buffer::InMemBuffer, entry::Version};

mod error;
pub use error::Error;

use self::node::DequeueNode;

mod node;

pub struct BackingDequeueNodeGenerator<S>
where
    S: AsRef<str>,
{
    read_index: AtomicU64,
    write_index: AtomicU64,
    dir: S,
}

impl<S> BackingDequeueNodeGenerator<S>
where
    S: AsRef<str>,
{
    pub fn new(dir: S) -> Result<Self, Error> {
        let last = {
            let dir = Path::new(dir.as_ref());
            if !dir.exists() {
                create_dir_all(dir)?;
            }

            if let Some(entry) = dir.read_dir()?.last() {
                entry?
                    .file_name()
                    .to_str()
                    .expect("valid file name string")
                    .parse::<u64>()
                    .expect("valid file name")
            } else {
                0
            }
        };

        Ok(Self {
            read_index: AtomicU64::new(0),
            write_index: AtomicU64::new(last),
            dir,
        })
    }

    pub fn next_read(&self) -> Result<File, Error> {
        let index = self.read_index.load(Ordering::Acquire);
        let paths = format!("{}/{}.bin", self.dir.as_ref(), index);
        let path = Path::new(&paths);
        if path.exists() {
            let file = OpenOptions::new().read(true).open(path)?;

            self.read_index.store(index + 1, Ordering::Release);

            Ok(file)
        } else {
            Err(Error::FileDoesNotExist(paths))
        }
    }

    pub fn next_write(&self) -> Result<File, Error> {
        let index = self.write_index.fetch_add(1, Ordering::AcqRel);
        let path = format!("{}/{}.bin", self.dir.as_ref(), index);
        let file = OpenOptions::new().append(true).create(true).open(path)?;

        Ok(file)
    }

    pub fn init_write(&self, buffer: &mut InMemBuffer) -> Result<(), Error> {
        let index = self.write_index.load(Ordering::Acquire);
        let path = format!("{}/{}.bin", self.dir.as_ref(), index);
        let path = Path::new(&path);
        if path.exists() {
            let file = OpenOptions::new().read(true).open(path)?;
            file.read_at(buffer.as_mut(), 0)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum Pop {
    Popped,
    WaitForFlush,
}

// Good idea: treat the file not found err for read as a special case for yielding for writes.
// Also add a flush method to be called at end of writes.
pub struct Dequeue<S>
where
    S: AsRef<str>,
{
    read: AtomicPtr<DequeueNode<InMemBuffer>>,
    write: AtomicPtr<DequeueNode<InMemBuffer>>,
    backing_generator: BackingDequeueNodeGenerator<S>,
    node_size: u64,
    version: Version,
}

impl<S> Drop for Dequeue<S>
where
    S: AsRef<str>,
{
    fn drop(&mut self) {
        let read = self.read.load(Ordering::Acquire);
        let write = self.write.load(Ordering::Acquire);

        // Make sure pointers are not null and contents dropped!

        if !read.is_null() {
            // Sound because we checked for null above.
            let _ = unsafe { Box::from_raw(read) };
        }

        // Only drop write if it is not the same as read.
        if !write.is_null() && write != read {
            // Sound because we checked for null above.
            let _ = unsafe { Box::from_raw(write) };
        }
    }
}

// should read from files on init, write to files from push, and pop should go through list nodes.
// we should also have max in-mem limit and max disk limit. if we hit the in-mem limit, we should
// write to disk. if we hit the disk limit, we should error.
// pop needs to read from disk and fall back to read from next node if node is empty.
// this is because we could be writing in mem and not have written to disk yet.
impl<S> Dequeue<S>
where
    S: AsRef<str>,
{
    pub fn new(dir: S, node_size: u64, version: Version) -> Result<Self, Error> {
        let backing_generator = BackingDequeueNodeGenerator::new(dir)?;

        // Need to read first and last file to get the metadata and correct ptrs.
        let mut buffer = InMemBuffer::new(node_size);
        backing_generator.init_write(&mut buffer)?;

        let node = DequeueNode::new(buffer, version)?;
        let node = Box::into_raw(Box::new(node));

        Ok(Self {
            read: AtomicPtr::new(std::ptr::null_mut()),
            write: AtomicPtr::new(node),
            backing_generator,
            node_size,
            version,
        })
    }

    pub fn pop(&self, buf: &mut [u8]) -> Result<Pop, Error> {
        let mut read_ptr = self.read.load(Ordering::Acquire);

        if read_ptr.is_null() {
            match self.next_read_ptr() {
                Ok(ptr) => read_ptr = ptr,
                Err(Error::FileDoesNotExist(_)) => return Ok(Pop::WaitForFlush),
                Err(e) => return Err(e),
            }
            self.read.store(read_ptr, Ordering::Release);
        }

        let read = NonNull::new(read_ptr).expect("valid ptr");
        let read = unsafe { read.as_ref() };

        match read.pop(buf) {
            Ok(_) => Ok(Pop::Popped),
            // We caught up to the write ptr in node.
            Err(Error::NodeFull) => {
                let next_ptr = match self.next_read_ptr() {
                    Ok(ptr) => ptr,
                    Err(Error::FileDoesNotExist(_)) => return Ok(Pop::WaitForFlush),
                    Err(e) => return Err(e),
                };

                let read = NonNull::new(next_ptr).expect("valid ptr");
                let read = unsafe { read.as_ref() };

                read.pop(buf)?;

                // drop old node.
                unsafe { Box::from_raw(read_ptr) };
                // put in new node.
                self.read.store(next_ptr, Ordering::Release);

                Ok(Pop::Popped)
            }
            Err(e) => Err(e),
        }
    }

    pub fn push(&self, buf: &[u8]) -> Result<(), Error> {
        // Should never be null!
        let write_ptr = self.write.load(Ordering::Acquire);
        let write = NonNull::new(write_ptr)
            .expect("write ptr should never be null, it is initialized in new");

        let write = unsafe { write.as_ref() };
        match write.push(buf) {
            Ok(_) => Ok(()),
            Err(Error::NodeFull) => {
                let mut next = self.backing_generator.next_write()?;
                next.write_all((*write).as_ref())?;
                next.flush()?;

                let node = DequeueNode::new(InMemBuffer::new(self.node_size), self.version)?;

                node.push(buf)?;

                let node = Box::into_raw(Box::new(node));

                self.write.store(node, Ordering::Release);

                // allow for `write_ptr` to be dropped if not also pointed to by read half.
                if write_ptr != self.read.load(Ordering::Acquire) {
                    unsafe { Box::from_raw(write_ptr) };
                }

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn flush(&self) -> Result<(), Error> {
        let write_ptr = self.write.load(Ordering::Acquire);
        let write = NonNull::new(write_ptr)
            .expect("write ptr should never be null, it is initialized in new");

        let write = unsafe { write.as_ref() };

        let mut next = self.backing_generator.next_write()?;
        next.write_all((*write).as_ref())?;
        next.flush()?;

        let node = DequeueNode::new(InMemBuffer::new(self.node_size), self.version)?;
        let node = Box::into_raw(Box::new(node));

        self.write.store(node, Ordering::Release);

        Ok(())
    }

    fn next_read_ptr(&self) -> Result<*mut DequeueNode<InMemBuffer>, Error> {
        // We have to try to read from disk.
        let mut next = self.backing_generator.next_read()?;
        let mut buffer = InMemBuffer::new(self.node_size);

        _ = next.read(buffer.as_mut())?;
        let node = DequeueNode::new(buffer, self.version)?;

        Ok(Box::into_raw(Box::new(node)))
    }
}

pub struct DequeuePusher<S>
where
    S: AsRef<str>,
{
    dequeue: Arc<Dequeue<S>>,
}

impl<S> DequeuePusher<S>
where
    S: AsRef<str>,
{
    pub fn new(dequeue: Arc<Dequeue<S>>) -> Self {
        Self { dequeue }
    }

    pub fn push(&self, buf: &[u8]) -> Result<(), Error> {
        self.dequeue.push(buf)
    }

    pub fn flush(&self) -> Result<(), Error> {
        self.dequeue.flush()
    }
}

pub struct DequeuePopper<S>
where
    S: AsRef<str>,
{
    dequeue: Arc<Dequeue<S>>,
}

impl<S> DequeuePopper<S>
where
    S: AsRef<str>,
{
    pub fn new(dequeue: Arc<Dequeue<S>>) -> Self {
        Self { dequeue }
    }

    pub fn pop(&self, buf: &mut [u8]) -> Result<Pop, Error> {
        self.dequeue.pop(buf)
    }
}

pub fn dequeue<S>(
    dir: S,
    node_size: u64,
    version: Version,
) -> Result<(DequeuePusher<S>, DequeuePopper<S>), Error>
where
    S: AsRef<str>,
{
    let dequeue = Arc::new(Dequeue::new(dir, node_size, version)?);

    let pusher = DequeuePusher::new(dequeue.clone());
    let popper = DequeuePopper::new(dequeue);

    Ok((pusher, popper))
}

#[cfg(test)]
mod test {

    use std::{
        thread::{sleep, spawn},
        time::Duration,
    };

    use matches::assert_matches;

    use crate::entry::Version;

    use super::{dequeue, Dequeue, Pop};

    #[test]
    fn test_dequeue() {
        let dir = tempfile::tempdir().unwrap();
        let dequeue = Dequeue::new(dir.path().to_str().unwrap(), 1024, Version::V1).unwrap();

        let mut buf = [0u8; 1024];
        dequeue.push(b"hello kitties").unwrap();
        assert_matches!(dequeue.pop(&mut buf), Ok(Pop::WaitForFlush));
        dequeue.flush().unwrap();
        assert_matches!(dequeue.pop(&mut buf), Ok(Pop::Popped));

        assert_eq!(&buf[..13], b"hello kitties");
    }

    #[test]
    fn test_dequeue_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let dequeue = Dequeue::new(dir.path().to_str().unwrap(), 1024, Version::V1).unwrap();

        dequeue.push(b"hello kitties").unwrap();
        dequeue.push(b"hello kitties").unwrap();
        dequeue.flush().unwrap();

        let mut buf = [0u8; 1024];
        dequeue.pop(&mut buf).unwrap();
        dequeue.pop(&mut buf).unwrap();

        assert_eq!(&buf[..13], b"hello kitties");
    }

    #[test]
    fn test_dequeue_multiple_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let dequeue = Dequeue::new(dir.path().to_str().unwrap(), 1024, Version::V1).unwrap();

        for i in 0..100 {
            dequeue
                .push(format!("hello kitties {i}").as_bytes())
                .unwrap();
        }
        dequeue.flush().unwrap();

        // Because we read from the node in mem we also need to know to skip the backing buffer as well...
        for i in 0..100 {
            let expected = format!("hello kitties {i}");
            let mut buf = [0u8; 1024];
            dequeue.pop(&mut buf).unwrap();

            assert_eq!(&buf[..expected.len()], expected.as_bytes());
        }
    }

    #[test]
    fn test_dequeue_as_spsc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_owned();
        let (tx, rx) = dequeue(path, 1024, Version::V1).unwrap();

        spawn(move || {
            for i in 0..100 {
                tx.push(format!("hello kitties {i}").as_bytes()).unwrap();
            }
            tx.flush().unwrap();
        });

        for i in 0..100 {
            let expected = format!("hello kitties {i}");
            let mut buf = [0u8; 1024];
            while let Ok(Pop::WaitForFlush) = rx.pop(&mut buf) {
                sleep(Duration::from_millis(1));
            }

            assert_eq!(&buf[..expected.len()], expected.as_bytes());
        }
    }

    #[test]
    fn test_dequeue_empty() {
        let dir = tempfile::tempdir().unwrap();
        let dequeue = Dequeue::new(dir.path().to_str().unwrap(), 1024, Version::V1).unwrap();

        let mut buf = [0u8; 1024];
        assert_matches!(dequeue.pop(&mut buf), Ok(Pop::WaitForFlush));
    }
}
