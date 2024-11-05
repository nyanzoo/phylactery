use std::ops::Range;

use crate::{
    buffer::FileBuffer,
    entry::{last_metadata, Version},
};

use super::{
    file::{File, Flush, Remaining},
    Error, Location,
};

#[derive(Clone, Debug)]
pub struct DequeNode {
    location: Location,
    buffer_size: u64,
    version: Version,
}

// SPSC queue
impl DequeNode {
    pub fn new(location: &Location, buffer_size: u64, version: Version) -> Self {
        Self {
            location: location.clone(),
            buffer_size,
            version,
        }
    }

    pub(crate) fn compact(&self, ranges_to_delete: &[Range<usize>]) -> Result<Flush, Error> {
        if ranges_to_delete.is_empty() {
            return Ok(Flush::NoOp);
        }

        let mut file = self.open()?;
        file.compact(ranges_to_delete)
    }

    pub(crate) fn open(&self) -> Result<File, Error> {
        let Self {
            location,
            buffer_size,
            version,
        } = self;

        let buffer = FileBuffer::new(*buffer_size, location.path())?;
        let meta = last_metadata(&buffer, *version)?;

        let read = meta.map(|meta| meta.read_ptr()).unwrap_or_default() as usize;
        let write = meta.map(|meta| meta.write_ptr()).unwrap_or_default() as usize;

        Ok(File {
            buffer,
            read,
            write,
            location: self.location.clone(),
            version: self.version,
        })
    }

    pub(crate) fn read(&self) -> Result<File, Error> {
        let Self {
            location,
            buffer_size,
            version,
        } = self;

        let buffer = FileBuffer::read(*buffer_size, location.path())?;
        let meta = last_metadata(&buffer, *version)?;

        let read = meta.map(|meta| meta.read_ptr()).unwrap_or_default() as usize;
        let write = meta.map(|meta| meta.write_ptr()).unwrap_or_default() as usize;

        Ok(File {
            buffer,
            read,
            write,
            location: self.location.clone(),
            version: self.version,
        })
    }

    pub(crate) fn delete(&self) -> Result<(), Error> {
        let path = self.location.path();
        std::fs::remove_file(path)?;
        Ok(())
    }

    pub(crate) fn location(&self) -> &Location {
        &self.location
    }

    pub(crate) fn buffer_size(&self) -> u64 {
        self.buffer_size
    }

    pub(crate) fn remaining(&self) -> Result<Remaining, Error> {
        let file = self.open()?;
        Ok(file.remaining())
    }
}

#[cfg(test)]
mod test {

    use matches::assert_matches;
    use necronomicon::{Pool, PoolImpl, Shared};

    use crate::{
        buffer::Buffer as _,
        deque::file::{Entry, File, Pop, Push},
        entry::Version,
    };

    use super::{DequeNode, Error, Location};

    const TEST_DIR: &str = "test";
    const TEST_FILE: u64 = 0;

    #[test]
    fn test_new_fresh() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir = dir.path().join(TEST_DIR);
        std::fs::create_dir_all(dir.clone()).unwrap();
        let location = Location::new(dir, TEST_FILE);
        let node = DequeNode::new(&location, 128, Version::V1);
        let File {
            buffer,
            read,
            write,
            location: other_location,
            version,
        } = node.open().unwrap();
        assert_eq!(other_location, location);
        assert_eq!(buffer.capacity(), 128);
        assert_eq!(read, 0);
        assert_eq!(write, 0);
        assert_eq!(version, Version::V1);
    }

    #[test]
    fn test_new_with_data() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir = dir.path().join(TEST_DIR);
        std::fs::create_dir_all(dir.clone()).unwrap();
        let location = Location::new(dir, TEST_FILE);
        let node = DequeNode::new(&location, 128, Version::V1);

        let mut open = node.open().unwrap();
        let push = open.push(b"hello world").unwrap();
        match push {
            Push::Entry { flush, .. } => flush.flush().expect("flush"),
            Push::Full => panic!("full"),
        }

        let other = DequeNode::new(&location, 128, Version::V1);
        let File {
            buffer,
            read,
            write,
            location: other_location,
            version,
        } = other.read().unwrap();

        assert_eq!(other_location, location);
        assert_eq!(buffer.capacity(), 128);
        assert_eq!(read, 0);
        assert_eq!(write, 53);
        assert_eq!(version, Version::V1);
    }

    #[test]
    fn push_pop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir = dir.path().join(TEST_DIR);
        std::fs::create_dir_all(dir.clone()).unwrap();
        let location = Location::new(dir, TEST_FILE);
        let node = DequeNode::new(&location, 128, Version::V1);

        let mut open = node.open().unwrap();
        let push = open.push(b"hello world").unwrap();
        match push {
            Push::Entry { flush, .. } => flush.flush().expect("flush"),
            Push::Full => panic!("full"),
        }

        let pool = PoolImpl::new(1024, 1024);
        let mut owned = pool.acquire("pop", "pop");
        let Pop::Entry(Entry { data, size }) = open.pop(&mut owned).expect("pop") else {
            panic!("pop");
        };
        assert_eq!(size, 53);

        assert_eq!(data.into_inner().data().as_slice(), b"hello world");
    }

    #[test]
    fn push_node_full() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir = dir.path().join(TEST_DIR);
        std::fs::create_dir_all(dir.clone()).unwrap();
        let location = Location::new(dir, TEST_FILE);
        let node = DequeNode::new(&location, 64, Version::V1);

        let mut open = node.open().unwrap();
        open.push(b"hello world").unwrap();
        assert_matches!(open.push(b"hello world"), Ok(Push::Full));
    }

    #[test]
    fn push_entry_too_large() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir = dir.path().join(TEST_DIR);
        std::fs::create_dir_all(dir.clone()).unwrap();
        let location = Location::new(dir, TEST_FILE);
        let node = DequeNode::new(&location, 128, Version::V1);

        assert_matches!(
            node.open().unwrap().push(&[0u8; 129]),
            Err(Error::EntryLargerThanNode(171, 128))
        );
    }

    #[test]
    fn test_pop_empty_data() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir = dir.path().join(TEST_DIR);
        std::fs::create_dir_all(dir.clone()).unwrap();
        let location = Location::new(dir, TEST_FILE);
        let node = DequeNode::new(&location, 128, Version::V1);

        let pool = PoolImpl::new(1024, 1024);

        let mut owned = pool.acquire("pop", "pop");

        assert_matches!(node.open().unwrap().pop(&mut owned), Ok(Pop::WaitForFlush));
    }

    #[ignore = "This is just for getting size of DequeNode"]
    #[test]
    fn remove_me() {
        let size = std::mem::size_of::<DequeNode>();
        println!("Size of DequeNode: {}", size);
    }
}
