use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::{create_dir_all, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::Path,
    rc::Rc,
};

use necronomicon::{BufferOwner, Decode, DecodeOwned, Owned, Pool, Shared};

use crate::{
    deque::node::DequeNode,
    entry::{Metadata, Readable, Version},
};

mod error;
pub use error::Error;

mod file;

mod location;
pub(crate) use location::Location;

use self::file::{Entry, File, Pop, Push};

mod node;

pub struct Deque {
    deque: VecDeque<(DequeNode, Rc<RefCell<Option<File>>>)>,
    /// The location of the node we are writing to.
    location: Location,
    dir: String,
    node_size: u64,
    max_disk_usage: u64,
    disk_usage: u64,
    version: Version,
}

// should read from files on init, write to files from push, and pop should go through list nodes.
// we should also have max in-mem limit and max disk limit. if we hit the in-mem limit, we should
// write to disk. if we hit the disk limit, we should error.
// pop needs to read from disk and fall back to read from next node if node is empty.
// this is because we could be writing in mem and not have written to disk yet.
impl Deque {
    pub fn new(
        dir: String,
        node_size: u64,
        max_disk_usage: u64,
        version: Version,
    ) -> Result<Self, Error> {
        let path = Path::new(&dir);
        if !path.exists() {
            create_dir_all(path)?;
        }

        let read_dir = path.read_dir()?;

        let mut deque = VecDeque::new();
        let mut location = Location::new(dir.clone(), 0);
        for file in read_dir {
            let file = file?;
            let file_path = file.path();
            let index = file_path
                .file_name()
                .expect("valid name")
                .to_str()
                .expect("valid str")
                .split(".")
                .next()
                .expect("valid index")
                .parse::<u64>()
                .expect("valid index");
            location = Location::new(dir.clone(), index);
            let node = DequeNode::new(&location, node_size, version);
            deque.push_back((node, Rc::new(RefCell::new(None))));
        }

        let current_disk_usage = node_size * deque.len() as u64;

        if current_disk_usage > max_disk_usage {
            return Err(Error::InvalidDequeCapacity {
                capacity: max_disk_usage,
                current_capacity: current_disk_usage,
            });
        }

        Ok(Self {
            deque,
            node_size,
            max_disk_usage,
            disk_usage: current_disk_usage,
            version,
            location,
            dir,
        })
    }

    pub fn peek<P, O>(
        &self,
        pool: P,
        owner: O,
    ) -> impl Iterator<Item = Result<file::Entry<<P::Buffer as Owned>::Shared>, Error>>
    where
        P: Pool,
        O: BufferOwner,
    {
        self.deque.iter().flat_map(|(node, open)| {
            if let Some(open) = open {
                open.peek(pool, owner)
            } else {
                node.open().map(|open| open.peek(pool, owner))
            }
        })
    }

    pub fn pop<O>(&mut self, buf: &mut O) -> Result<Pop<O::Shared>, Error>
    where
        O: Owned,
    {
        loop {
            if self.deque.is_empty() {
                return Ok(Pop::WaitForFlush);
            }

            // What can happen now is that we keep the file open for read and write (flush)
            // but file buffer will allocate memory, so we need to make sure we don't OOM.
            // so think on this!
            let (node, open_rc) = self.deque.pop_front().expect("valid node");
            let open = open_rc.clone();
            let mut open = open.borrow_mut();
            if open.is_none() {
                open.replace(node.open()?);
            }
            let open = open.as_mut().expect("valid file");
            match open.pop(buf)? {
                Pop::Entry(entry) => {
                    // reclaim disk space.
                    self.disk_usage -= entry.size as u64;
                    self.deque.push_front((node, open_rc));
                    return Ok(Pop::Entry(entry));
                }
                Pop::WaitForFlush => {}
            }
        }
    }

    pub fn push(&mut self, buf: &[u8]) -> Result<Push<'_>, Error> {
        // Check if we have room left on disk.
        let current_disk_usage = self.disk_usage;
        if current_disk_usage > self.max_disk_usage {
            return Err(Error::DequeFull {
                max: self.max_disk_usage,
                current: current_disk_usage,
            });
        }

        if self.deque.is_empty() {
            let node = DequeNode::new(&self.location, self.node_size, self.version);
            self.deque.push_back((node, Rc::default()));
        }

        let (node, open_rc) = self.deque.pop_back().expect("valid node");
        let open = open_rc.clone();
        let mut open = open.borrow_mut();
        if open.is_none() {
            open.replace(node.open()?);
        }
        let open = open.as_mut().expect("valid file");
        let push = open.push(buf)?;

        match push {
            Push::Entry {
                file,
                offset,
                len,
                crc,
                flush,
            } => {
                self.disk_usage += len;
                self.deque.push_back((node, open_rc));
                Ok(Push::Entry {
                    file,
                    offset,
                    len,
                    crc,
                    flush,
                })
            }
            Push::Full => {
                self.location.move_forward();
                let node = DequeNode::new(&self.location, self.node_size, self.version);
                let open = node.open()?;
                self.deque
                    .push_back((node, Rc::new(RefCell::new(Some(open)))));
                let push = {
                    let push = open.push(buf)?;
                    if let Push::Entry { len, .. } = push {
                        self.disk_usage += len;
                    }
                    push
                };
                Ok(push)
            }
        }
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
            .open(format!("{}/{}.bin", self.dir, file))?;
        let mut buf_reader = BufReader::new(file);
        println!("content: {:?}", buf_reader.fill_buf());

        let meta = Metadata::decode(&mut buf_reader)?;
        meta.verify()?;

        buf_reader.seek(SeekFrom::Start(
            offset + Metadata::struct_size(version) as u64,
        ))?;

        let data = Readable::decode_owned(&mut buf_reader, buf)?;

        Ok(data)
    }
}

#[cfg(test)]
mod test {

    use matches::assert_matches;

    use necronomicon::{Pool, PoolImpl, Shared};

    use crate::entry::Version;

    use super::{Deque, Pop};

    #[test]
    fn test_deque() {
        let dir = tempfile::tempdir().unwrap();
        let mut deque = Deque::new(
            dir.path().to_str().unwrap().to_owned(),
            1024,
            1024 * 1024,
            Version::V1,
        )
        .unwrap();

        let pool = PoolImpl::new(1024, 1024);
        let mut buf = pool.acquire("pop");
        let push = deque.push(b"hello kitties").unwrap();
        assert_matches!(deque.pop(&mut buf), Ok(Pop::WaitForFlush));
        push.flush().unwrap();
        let res = deque.pop(&mut buf);
        assert_matches!(res, Ok(Pop::Popped(_)));

        let Pop::Popped(data) = res.unwrap() else {
            panic!("expected Pop::Popped");
        };
        assert_eq!(&data.into_inner().data().as_slice(), b"hello kitties");
    }

    #[test]
    fn test_deque_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let mut deque = Deque::new(
            dir.path().to_str().unwrap().to_owned(),
            1024,
            1024 * 1024,
            Version::V1,
        )
        .unwrap();

        deque.push(b"hello kitties").unwrap();
        let push = deque.push(b"hello kitties").unwrap();
        push.flush().unwrap();

        let pool = PoolImpl::new(1024, 1024);
        let mut buf = pool.acquire("pop");
        let pop = deque.pop(&mut buf).unwrap();
        let Pop::Popped(data) = pop else {
            panic!("expected Pop::Popped");
        };
        assert_eq!(&data.into_inner().data().as_slice(), b"hello kitties");

        let mut buf = pool.acquire("pop");
        let pop = deque.pop(&mut buf).unwrap();
        let Pop::Popped(data) = pop else {
            panic!("expected Pop::Popped");
        };
        assert_eq!(&data.into_inner().data().as_slice(), b"hello kitties");
    }

    #[test]
    fn test_deque_multiple_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let mut deque = Deque::new(
            dir.path().to_str().unwrap().to_owned(),
            1024,
            8192,
            Version::V1,
        )
        .unwrap();

        for i in 0..100 {
            deque
                .push(&format!("hello kitties {i}").as_bytes())
                .unwrap()
                .flush()
                .unwrap();
        }

        let pool = PoolImpl::new(1024, 1024);
        // Because we read from the node in mem we also need to know to skip the backing buffer as well...
        for i in 0..100 {
            let expected = format!("hello kitties {i}");
            let mut buf = pool.acquire("pop");
            let pop = deque.pop(&mut buf).unwrap();

            let Pop::Popped(data) = pop else {
                panic!("expected Pop::Popped");
            };
            assert_eq!(data.into_inner().data().as_slice(), expected.as_bytes());
        }
    }

    #[test]
    fn test_deque_empty() {
        let dir = tempfile::tempdir().unwrap();
        let deque = Deque::new(
            dir.path().to_str().unwrap().to_owned(),
            1024,
            1024 * 1024,
            Version::V1,
        )
        .unwrap();

        let pool = PoolImpl::new(1024, 1024);
        let mut buf = pool.acquire("pop");
        assert_matches!(deque.pop(&mut buf), Ok(Pop::WaitForFlush));
    }
}
