use std::{
    collections::{BTreeMap, VecDeque},
    fs::{create_dir_all, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom},
    ops::Range,
    path::Path,
};

use necronomicon::{Decode, DecodeOwned, Owned};

use crate::{
    deque::node::DequeNode,
    entry::{Metadata, Readable, Version},
};

mod error;
pub use error::Error;

mod file;

mod location;
pub(crate) use location::Location;

use self::file::{File, Remaining};
pub(crate) use self::file::{Pop, Push};

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

    pub fn push(&mut self, value: &[u8]) -> Result<Push, Error> {
        if self.disk_usage >= self.max_disk_usage {
            return Err(Error::DequeFull {
                current: self.disk_usage,
                max: self.max_disk_usage,
            });
        }

        let push = if let Some((_, file)) = self.back_mut()? {
            file.push(value)?
        } else {
            self.push_back();
            let (_, file) = self.back_mut()?.expect("just pushed");
            file.push(value)?
        };

        match push {
            Push::Entry { len, .. } => {
                self.disk_usage += len;
                Ok(push)
            }
            Push::Full => {
                self.push_back();
                let (_, file) = self.back_mut()?.expect("just pushed");
                let push = file.push(value)?;
                if let Push::Entry { len, .. } = push {
                    self.disk_usage += len;
                }

                Ok(push)
            }
        }
    }

    pub fn pop<O>(&mut self, buf: &mut O) -> Result<Option<Readable<O::Shared>>, Error>
    where
        O: Owned,
    {
        if self.deque.is_empty() {
            return Ok(None);
        }

        if let Some((_, file)) = self.front_mut()? {
            let pop = file.pop(buf)?;
            match pop {
                Pop::Entry(entry) => {
                    self.disk_usage -= entry.size as u64;
                    Ok(Some(entry.data))
                }
                Pop::WaitForFlush => {
                    self.pop_front()?;
                    if let Some((_, file)) = self.front_mut()? {
                        match file.pop(buf)? {
                            Pop::Entry(entry) => {
                                self.disk_usage -= entry.size as u64;
                                Ok(Some(entry.data))
                            }
                            Pop::WaitForFlush => Ok(None),
                        }
                    } else {
                        Ok(None)
                    }
                }
            }
        } else {
            Ok(None)
        }
    }

    fn back_mut(&mut self) -> Result<Option<(&mut DequeNode, &mut File)>, Error> {
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

    fn front_mut(&mut self) -> Result<Option<(&mut DequeNode, &mut File)>, Error> {
        if self.deque.is_empty() {
            return Ok(None);
        }
        let (node, file) = self.deque.front_mut().expect("empty");
        if file.is_none() {
            file.replace(node.open()?);
        }
        Ok(Some((node, file.as_mut().expect("file"))))
    }

    fn pop_front(&mut self) -> Result<(), Error> {
        if self.deque.is_empty() {
            return Ok(());
        }
        let (node, _) = self.deque.pop_front().expect("valid node");
        node.delete()?;
        Ok(())
    }

    fn push_back(&mut self) {
        let node = DequeNode::new(&self.location, self.node_size, self.version);
        self.deque.push_back((node, None));
        self.location.move_forward();
    }

    pub(crate) fn compact(
        &mut self,
        ranges_to_delete: BTreeMap<Location, Vec<Range<usize>>>,
    ) -> Result<(), Error> {
        let mut remove_nodes = Vec::new();

        for (location, ranges_to_delete) in ranges_to_delete.into_iter() {
            let (idx, (node, file)) = self
                .deque
                .iter()
                .enumerate()
                .find(|(_, (node, _))| *node.location() == location)
                .expect("file exists");

            let file = if let Some(file) = file {
                file
            } else {
                &mut node.open()?
            };
            let Remaining { space_to_write, .. } = file.remaining();
            let size = node.buffer_size() - space_to_write;

            let first = ranges_to_delete.first().expect("at least one range");
            let last = ranges_to_delete.last().expect("at least one range");
            let mut running_range: Range<usize> = first.clone();

            if last.end as u64 == size {
                for range in ranges_to_delete.iter().skip(1) {
                    if range.start == running_range.end {
                        running_range.end = range.end;
                    } else {
                        break;
                    }
                }

                if running_range.end as u64 == size && running_range.start == 0 {
                    node.delete()?;
                    self.disk_usage -= size as u64;
                    remove_nodes.push(idx);
                }
            }

            node.compact(&ranges_to_delete)?;
        }

        for idx in remove_nodes.into_iter().rev() {
            self.deque.remove(idx);
        }

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
        let path = format!("{}/{:08x}.bin", self.dir, file);
        let file = OpenOptions::new().read(true).open(path)?;
        let mut buf_reader = BufReader::new(file);

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

    use crate::{deque::file::Push, entry::Version};

    use super::Deque;

    #[test]
    fn deque() {
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
            for _ in 0..10 {
                let Push::Entry { flush, .. } = deque.push(b"hello kitties").expect("valid node")
                else {
                    panic!("expected Push::Entry");
                };
                flushes.push(flush);
            }

            for flush in flushes.drain(..) {
                flush.flush().unwrap();
            }
        }

        for _ in 0..10 {
            let pop = deque.pop(&mut buf).expect("valid node").expect("empty");
            assert_eq!(&pop.into_inner().data().as_slice(), b"hello kitties");
        }
    }

    #[test]
    fn deque_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let mut deque = Deque::new(
            dir.path().to_str().unwrap().to_owned(),
            1024,
            1024 * 1024,
            Version::V1,
        )
        .unwrap();
        let _push = deque.push(b"hello kitties").unwrap();
        let push = deque.push(b"hello kitties").unwrap();
        match push {
            Push::Entry { flush, .. } => flush.flush().unwrap(),
            Push::Full => panic!("expected Push::Entry"),
        }

        let pool = PoolImpl::new(1024, 1024);
        let mut buf = pool.acquire("pop");
        let pop = deque.pop(&mut buf).unwrap().unwrap();
        assert_eq!(&pop.into_inner().data().as_slice(), b"hello kitties");

        let mut buf = pool.acquire("pop");
        let pop = deque.pop(&mut buf).unwrap().unwrap();
        assert_eq!(&pop.into_inner().data().as_slice(), b"hello kitties");
    }

    #[test]
    fn deque_multiple_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let mut deque = Deque::new(
            dir.path().to_str().unwrap().to_owned(),
            1024,
            8192 * 1024,
            Version::V1,
        )
        .unwrap();

        let now = std::time::Instant::now();
        let mut flushes = vec![];
        for i in 0..100_000 {
            let push = deque
                .push(&format!("hello kitties {i}").as_bytes())
                .unwrap();

            match push {
                Push::Entry { flush, .. } => {
                    flushes.push(flush);
                }
                Push::Full => break,
            }
        }

        for flush in flushes.drain(..) {
            flush.flush().unwrap();
        }

        let pool = PoolImpl::new(1024, 1024);
        // Because we read from the node in mem we also need to know to skip the backing buffer as well...
        for i in 0..100_000 {
            let expected = format!("hello kitties {i}");
            let mut buf = pool.acquire("pop");
            let pop = deque.pop(&mut buf).unwrap().unwrap();
            assert_eq!(
                String::from_utf8_lossy(pop.into_inner().data().as_slice()),
                expected
            );
        }
        let elapsed = now.elapsed();
        println!("100,000 nodes in {:?}", elapsed);
    }

    #[test]
    fn deque_empty() {
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
        assert_matches!(deque.pop(&mut buf), Ok(None));
    }
}
