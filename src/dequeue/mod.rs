use std::{
    fs::{create_dir_all, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::Path,
    ptr::NonNull,
    sync::{
        atomic::{AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
};

use necronomicon::{Decode, DecodeOwned, Owned, Shared};

use crate::{
    buffer::InMemBuffer,
    entry::{Metadata, Readable, Version},
    Error,
};

use self::node::DequeueNode;

mod node;

pub(crate) struct File {
    file: std::fs::File,
    path: String,
    index: u64,
}

pub struct BackingDequeueNodeGenerator {
    read_index: AtomicU64,
    write_index: AtomicU64,
    dir: String,
}

impl BackingDequeueNodeGenerator {
    pub fn new(dir: impl AsRef<str>) -> Result<Self, Error> {
        let (first, last) = {
            let dir = Path::new(dir.as_ref());
            if !dir.exists() {
                create_dir_all(dir)?;
            }

            let mut read_dir = dir.read_dir()?;

            let first = if let Some(entry) = read_dir.nth(0) {
                entry?
                    .file_name()
                    .to_str()
                    .expect("valid file name string")
                    .split_once('.')
                    .expect("remove .bin")
                    .0
                    .parse::<u64>()
                    .expect("valid file name")
            } else {
                0
            };

            let last = if let Some(entry) = read_dir.last() {
                entry?
                    .file_name()
                    .to_str()
                    .expect("valid file name string")
                    .split_once('.')
                    .expect("remove .bin")
                    .0
                    .parse::<u64>()
                    .expect("valid file name")
            } else {
                0
            };

            (first, last)
        };

        Ok(Self {
            read_index: AtomicU64::new(first),
            write_index: AtomicU64::new(last),
            dir: dir.as_ref().to_string(),
        })
    }

    pub(crate) fn next_read(&self) -> Result<File, Error> {
        let index = self.read_index.load(Ordering::Acquire);
        let paths = format!("{}/{}.bin", self.dir, index);
        let path = Path::new(&paths);
        if path.exists() {
            let file = OpenOptions::new().read(true).open(path)?;

            self.read_index.store(index + 1, Ordering::Release);

            Ok(File {
                file,
                path: paths,
                index,
            })
        } else {
            Err(Error::FileDoesNotExist(paths))
        }
    }

    pub fn dir(&self) -> &str {
        &self.dir
    }

    pub fn read_idx(&self) -> u64 {
        self.read_index.load(Ordering::Acquire)
    }

    pub fn write_idx(&self) -> u64 {
        self.write_index.load(Ordering::Acquire)
    }

    pub(crate) fn next_write(&self) -> Result<File, Error> {
        let index = self.write_index.fetch_add(1, Ordering::AcqRel);
        let path = format!("{}/{}.bin", self.dir, index);
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path.clone())?;

        Ok(File { file, path, index })
    }

    pub fn init_write(&self, buffer: &mut InMemBuffer) -> Result<(), Error> {
        let index = self.write_index.load(Ordering::Acquire);
        let path = format!("{}/{}.bin", self.dir, index);
        let path = Path::new(&path);
        if path.exists() {
            let file = OpenOptions::new().read(true).open(path)?;
            file.read_at(buffer.as_mut(), 0)?;
        }

        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Pop<S>
where
    S: Shared,
{
    Popped(Readable<S>),
    WaitForFlush,
}

#[derive(Debug)]
pub struct Push {
    pub file: u64,
    pub offset: u64,
    pub len: u64,
    pub crc: u32,
}

struct Inner {
    read: AtomicPtr<DequeueNode<InMemBuffer>>,
    write: AtomicPtr<DequeueNode<InMemBuffer>>,
    backing_generator: BackingDequeueNodeGenerator,
    node_size: u64,
    max_disk_usage: u64,
    disk_usage: AtomicU64,
    version: Version,
}

impl Drop for Inner {
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
impl Inner {
    pub fn new(
        dir: impl AsRef<str>,
        node_size: u64,
        max_disk_usage: u64,
        version: Version,
    ) -> Result<Self, Error> {
        let backing_generator = BackingDequeueNodeGenerator::new(dir)?;

        // Need to read first and last file to get the metadata and correct ptrs.
        let mut buffer = InMemBuffer::new(node_size);
        backing_generator.init_write(&mut buffer)?;

        let file_path = format!(
            "{}/{}.bin",
            backing_generator.dir(),
            backing_generator.read_idx()
        );

        let node = DequeueNode::new(file_path.into(), buffer, version)?;
        let last_write_idx = node.write_ptr();
        let node = Box::into_raw(Box::new(node));

        let last_file = backing_generator.write_idx();
        let current_disk_usage = if last_file == 0 {
            0
        } else {
            (last_file * node_size) - (node_size - last_write_idx)
        };

        if current_disk_usage > max_disk_usage {
            return Err(Error::InvalidDequeueCapacity {
                capacity: max_disk_usage,
                current_capacity: current_disk_usage,
            });
        }

        Ok(Self {
            read: AtomicPtr::new(std::ptr::null_mut()),
            write: AtomicPtr::new(node),
            backing_generator,
            node_size,
            max_disk_usage,
            disk_usage: AtomicU64::new(current_disk_usage),
            version,
        })
    }

    pub fn pop<O>(&self, buf: &mut O) -> Result<Pop<O::Shared>, Error>
    where
        O: Owned,
    {
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
            Ok(data) => Ok(Pop::Popped(data)),
            // We caught up to the write ptr in node.
            Err(Error::NodeFull) => {
                let next_ptr = match self.next_read_ptr() {
                    Ok(ptr) => ptr,
                    Err(Error::FileDoesNotExist(_)) => return Ok(Pop::WaitForFlush),
                    Err(e) => return Err(e),
                };

                let data = {
                    let read = NonNull::new(next_ptr).expect("valid ptr");
                    let read = unsafe { read.as_ref() };
                    read.pop(buf)?
                };

                // drop old node.
                read.delete()?;
                unsafe { drop(Box::from_raw(read_ptr)) };
                // put in new node.
                self.read.store(next_ptr, Ordering::Release);
                // reclaim disk space.
                self.disk_usage.fetch_sub(self.node_size, Ordering::Release);

                Ok(Pop::Popped(data))
            }
            Err(e) => Err(e),
        }
    }

    pub fn push(&self, buf: &[u8]) -> Result<Push, Error> {
        // Check if we have room left on disk.
        let current_disk_usage = self.disk_usage.load(Ordering::Acquire);
        if current_disk_usage > self.max_disk_usage {
            return Err(Error::DequeueFull);
        }

        // Should never be null!
        let write_ptr = self.write.load(Ordering::Acquire);
        let write = NonNull::new(write_ptr)
            .expect("write ptr should never be null, it is initialized in new");

        let write = unsafe { write.as_ref() };
        match write.push(buf) {
            Ok(node::Push { offset, len, crc }) => Ok(Push {
                file: self.backing_generator.write_idx(),
                offset,
                len,
                crc,
            }),
            Err(Error::NodeFull) => {
                let File {
                    file: mut next,
                    path,
                    index,
                } = self.backing_generator.next_write()?;
                next.write_all((*write).as_ref())?;
                next.flush()?;

                let node =
                    DequeueNode::new(path.into(), InMemBuffer::new(self.node_size), self.version)?;

                let node::Push { offset, len, crc } = node.push(buf)?;

                let node = Box::into_raw(Box::new(node));

                self.write.store(node, Ordering::Release);

                // claim disk space.
                self.disk_usage.fetch_add(self.node_size, Ordering::Release);

                // allow for `write_ptr` to be dropped if not also pointed to by read half.
                if write_ptr != self.read.load(Ordering::Acquire) {
                    unsafe { drop(Box::from_raw(write_ptr)) };
                }

                Ok(Push {
                    file: index,
                    offset,
                    len,
                    crc,
                })
            }
            Err(e) => Err(e),
        }
    }

    pub fn flush(&self) -> Result<(), Error> {
        let write_ptr = self.write.load(Ordering::Acquire);
        let write = NonNull::new(write_ptr)
            .expect("write ptr should never be null, it is initialized in new");

        let write = unsafe { write.as_ref() };

        let File {
            file: mut next,
            path,
            index: _,
        } = self.backing_generator.next_write()?;
        next.write_all((*write).as_ref())?;
        next.sync_data()?;

        let node = DequeueNode::new(path.into(), InMemBuffer::new(self.node_size), self.version)?;
        let node = Box::into_raw(Box::new(node));
        // reclaim memory.
        drop(unsafe { Box::from_raw(write_ptr) });

        self.write.store(node, Ordering::Release);

        Ok(())
    }

    pub(crate) fn get<O>(
        &self,
        file: u64,
        offset: u64,
        buf: &mut O,
        version: Version,
    ) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        let file = OpenOptions::new()
            .read(true)
            .open(format!("{}/{}.bin", self.backing_generator.dir, file))?;
        let mut buf_reader = BufReader::new(file);

        let meta = Metadata::decode(&mut buf_reader)?;
        meta.verify()?;

        buf_reader.seek(SeekFrom::Start(
            offset + Metadata::struct_size(version) as u64,
        ))?;

        let data = Readable::decode_owned(&mut buf_reader, buf)?;

        Ok(data)
    }

    fn next_read_ptr(&self) -> Result<*mut DequeueNode<InMemBuffer>, Error> {
        // We have to try to read from disk.
        let File {
            file: mut next,
            path,
            index: _,
        } = self.backing_generator.next_read()?;
        let mut buffer = InMemBuffer::new(self.node_size);

        _ = next.read(buffer.as_mut())?;
        let node = DequeueNode::new(path.into(), buffer, self.version)?;

        Ok(Box::into_raw(Box::new(node)))
    }
}

// Good idea: treat the file not found err for read as a special case for yielding for writes.
// Also add a flush method to be called at end of writes.
pub struct Dequeue(Arc<Inner>);

impl Clone for Dequeue {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Dequeue {
    pub fn new(
        dir: impl AsRef<str>,
        node_size: u64,
        max_disk_usage: u64,
        version: Version,
    ) -> Result<Self, Error> {
        Ok(Self(Arc::new(Inner::new(
            dir,
            node_size,
            max_disk_usage,
            version,
        )?)))
    }

    pub fn push(&self, buf: &[u8]) -> Result<Push, Error> {
        self.0.push(buf)
    }

    pub fn flush(&self) -> Result<(), Error> {
        self.0.flush()
    }

    pub fn pop<O>(&self, buf: &mut O) -> Result<Pop<O::Shared>, Error>
    where
        O: Owned,
    {
        self.0.pop(buf)
    }

    pub(crate) fn get<O>(
        &self,
        file: u64,
        offset: u64,
        buf: &mut O,
    ) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.0.get(file, offset, buf, self.0.version)
    }
}

pub struct Pusher(Dequeue);

impl Pusher {
    pub fn new(dequeue: Dequeue) -> Self {
        Self(dequeue)
    }

    pub fn push(&self, buf: &[u8]) -> Result<Push, Error> {
        self.0.push(buf)
    }

    pub fn flush(&self) -> Result<(), Error> {
        self.0.flush()
    }
}

pub struct Popper(Dequeue);

impl Popper {
    pub fn new(dequeue: Dequeue) -> Self {
        Self(dequeue)
    }

    pub fn pop<O>(&self, buf: &mut O) -> Result<Pop<O::Shared>, Error>
    where
        O: Owned,
    {
        self.0.pop(buf)
    }
}

pub fn dequeue(
    dir: impl AsRef<str>,
    node_size: u64,
    max_disk_usage: u64,
    version: Version,
) -> Result<(Pusher, Popper), Error> {
    let dequeue = Dequeue::new(dir, node_size, max_disk_usage, version)?;

    let pusher = Pusher::new(dequeue.clone());
    let popper = Popper::new(dequeue);

    Ok((pusher, popper))
}

#[cfg(test)]
mod test {

    use std::{
        thread::{sleep, spawn},
        time::Duration,
    };

    use matches::assert_matches;

    use necronomicon::{Pool, PoolImpl, Shared};

    use crate::entry::Version;

    use super::{dequeue, Dequeue, Pop};

    #[test]
    fn test_dequeue() {
        let dir = tempfile::tempdir().unwrap();
        let dequeue =
            Dequeue::new(dir.path().to_str().unwrap(), 1024, 1024 * 1024, Version::V1).unwrap();

        let pool = PoolImpl::new(1024, 1024);
        let mut buf = pool.acquire("pop");
        dequeue.push(b"hello kitties").unwrap();
        assert_matches!(dequeue.pop(&mut buf), Ok(Pop::WaitForFlush));
        dequeue.flush().unwrap();
        let res = dequeue.pop(&mut buf);
        assert_matches!(res, Ok(Pop::Popped(_)));

        let Pop::Popped(data) = res.unwrap() else {
            panic!("expected Pop::Popped");
        };
        assert_eq!(&data.into_inner().data().as_slice(), b"hello kitties");
    }

    #[test]
    fn test_dequeue_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let dequeue =
            Dequeue::new(dir.path().to_str().unwrap(), 1024, 1024 * 1024, Version::V1).unwrap();

        dequeue.push(b"hello kitties").unwrap();
        dequeue.push(b"hello kitties").unwrap();
        dequeue.flush().unwrap();

        let pool = PoolImpl::new(1024, 1024);
        let mut buf = pool.acquire("pop");
        let pop = dequeue.pop(&mut buf).unwrap();
        let Pop::Popped(data) = pop else {
            panic!("expected Pop::Popped");
        };
        assert_eq!(&data.into_inner().data().as_slice(), b"hello kitties");

        let mut buf = pool.acquire("pop");
        let pop = dequeue.pop(&mut buf).unwrap();
        let Pop::Popped(data) = pop else {
            panic!("expected Pop::Popped");
        };
        assert_eq!(&data.into_inner().data().as_slice(), b"hello kitties");
    }

    #[test]
    fn test_dequeue_multiple_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let dequeue =
            Dequeue::new(dir.path().to_str().unwrap(), 1024, 1024 * 1024, Version::V1).unwrap();

        for i in 0..100 {
            dequeue
                .push(&format!("hello kitties {i}").as_bytes())
                .unwrap();
        }
        dequeue.flush().unwrap();

        let pool = PoolImpl::new(1024, 1024);
        // Because we read from the node in mem we also need to know to skip the backing buffer as well...
        for i in 0..100 {
            let expected = format!("hello kitties {i}");
            let mut buf = pool.acquire("pop");
            let pop = dequeue.pop(&mut buf).unwrap();

            let Pop::Popped(data) = pop else {
                panic!("expected Pop::Popped");
            };
            assert_eq!(data.into_inner().data().as_slice(), expected.as_bytes());
        }
    }

    #[test]
    fn test_dequeue_as_spsc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_owned();
        let (tx, rx) = dequeue(path, 1024, 1024 * 1024, Version::V1).unwrap();

        spawn(move || {
            for i in 0..100 {
                _ = tx.push(&format!("hello kitties {i}").as_bytes());
            }
            tx.flush().unwrap();
        });

        let pool = PoolImpl::new(1024, 1024);
        for i in 0..100 {
            let expected = format!("hello kitties {i}");
            let mut buf = pool.acquire("pop");
            loop {
                if let Ok(Pop::Popped(data)) = rx.pop(&mut buf) {
                    assert_eq!(data.into_inner().data().as_slice(), expected.as_bytes());
                    break;
                }
                sleep(Duration::from_millis(1));
            }
        }
    }

    #[test]
    fn test_dequeue_empty() {
        let dir = tempfile::tempdir().unwrap();
        let dequeue =
            Dequeue::new(dir.path().to_str().unwrap(), 1024, 1024 * 1024, Version::V1).unwrap();

        let pool = PoolImpl::new(1024, 1024);
        let mut buf = pool.acquire("pop");
        assert_matches!(dequeue.pop(&mut buf), Ok(Pop::WaitForFlush));
    }
}
