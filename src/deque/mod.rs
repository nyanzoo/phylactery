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
    deque: VecDeque<(DequeNode, Option<File>)>,
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
            deque.push_back((node, None));
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

    pub fn peek(&self) -> impl Iterator<Item = &(DequeNode, Option<File>)> {
        self.deque.iter()
    }

    pub fn back_mut(&mut self) -> Result<Option<(&mut DequeNode, &mut File)>, Error> {
        if self.deque.is_empty() {
            return Ok(None);
        }
        if let Some((node, file)) = self.deque.back_mut() {
            if file.is_none() {
                file.replace(node.open()?);
            }
            Ok(Some((node, file.as_mut().expect("file"))))
        } else {
            Ok(None)
        }
    }

    pub fn front_mut(&mut self) -> Result<(&mut DequeNode, &mut File), Error> {
        if self.deque.is_empty() {
            self.deque.push_back((
                DequeNode::new(&self.location, self.node_size, self.version),
                None,
            ));
        }
        let (node, file) = self.deque.front_mut().expect("empty");
        if file.is_none() {
            file.replace(node.open()?);
        }
        Ok((node, file.as_mut().expect("file")))
    }

    pub fn pop_front(&mut self) -> Result<(), Error> {
        if self.deque.is_empty() {
            return Ok(());
        }
        let (node, _) = self.deque.pop_front().expect("valid node");
        node.delete()?;
        Ok(())
    }

    pub fn push_back(&mut self) {
        let node = DequeNode::new(&self.location, self.node_size, self.version);
        self.deque.push_back((node, None));
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

    use crate::{
        deque::file::{Entry, Push},
        entry::Version,
    };

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

        {
            let mut flushes = vec![];
            deque.push_back();
            let (_, open) = deque.back_mut().expect("valid node").expect("empty");
            for _ in 0..10 {
                let Push::Entry { flush, .. } = open.push(b"hello kitties").unwrap() else {
                    panic!("expected Push::Entry");
                };
                flushes.push(flush);
            }

            for mut flush in flushes.drain(..) {
                flush.flush().unwrap();
            }
        }

        for _ in 0..10 {
            let (_, open) = deque.back_mut().expect("valid node").expect("empty");
            let pop = open.pop(&mut buf).expect("valid pop");
            let Pop::Entry(Entry { data, .. }) = pop else {
                panic!("expected Pop::Popped");
            };

            assert_eq!(&data.into_inner().data().as_slice(), b"hello kitties");
        }
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
        let node = DequeNode::new(&deque.location, deque.node_size, deque.version);
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
