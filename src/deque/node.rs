use crate::{
    buffer::FileBuffer,
    entry::{last_metadata, Version},
};

use super::{file::File, Error, Location};

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

    pub(crate) fn delete(self) -> Result<(), Error> {
        let path = self.location.path();
        std::fs::remove_file(path)?;
        Ok(())
    }

    pub(crate) fn location(&self) -> &Location {
        &self.location
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
        let node = DequeNode::new(&test_location(), 128, Version::V1);
        let File {
            buffer,
            read,
            write,
            location,
            version,
        } = node.open().expect("open");
        assert_eq!(location, test_location());
        assert_eq!(buffer.capacity(), 128);
        assert_eq!(read, 0);
        assert_eq!(write, 0);
        assert_eq!(version, Version::V1);
    }

    #[test]
    fn test_new_with_data() {
        let node = DequeNode::new(&test_location(), 128, Version::V1);

        let mut open = node.open().expect("open");
        open.push(b"hello world").unwrap();

        let other = DequeNode::new(&test_location(), 128, Version::V1);
        let File {
            buffer,
            read,
            write,
            location,
            version,
        } = other.open().expect("open");

        assert_eq!(location, test_location());
        assert_eq!(buffer.capacity(), 128);
        assert_eq!(read, 0);
        assert_eq!(write, 61);
        assert_eq!(version, Version::V1);
    }

    #[test]
    fn test_push_pop() {
        let node = DequeNode::new(&test_location(), 128, Version::V1);

        let mut open = node.open().expect("open");
        open.push(b"hello world").unwrap();

        let pool = PoolImpl::new(1024, 1024);
        let mut owned = pool.acquire("pop");
        let Pop::Entry(Entry { data, size }) = open.pop(&mut owned).expect("pop") else {
            panic!("pop");
        };
        assert_eq!(size, 32);

        assert_eq!(data.into_inner().data().as_slice(), b"hello world");
    }

    #[test]
    fn test_push_node_full() {
        let node = DequeNode::new(&test_location(), 64, Version::V1);

        let mut open = node.open().expect("open");
        open.push(b"hello world").unwrap();
        assert_matches!(open.push(b"hello world"), Ok(Push::Full));
    }

    #[test]
    fn test_push_entry_too_large() {
        let node = DequeNode::new(&test_location(), 128, Version::V1);

        assert_matches!(
            node.open().expect("open").push(&[0u8; 129]),
            Err(Error::EntryLargerThanNode(179, 128))
        );
    }

    #[test]
    fn test_pop_empty_data() {
        let node = DequeNode::new(&test_location(), 128, Version::V1);

        let pool = PoolImpl::new(1024, 1024);

        let mut owned = pool.acquire("pop");

        assert_matches!(
            node.open().expect("open").pop(&mut owned),
            Err(Error::EmptyData)
        );
    }

    #[test]
    fn remove_me() {
        let size = std::mem::size_of::<DequeNode>();
        println!("Size of DequeNode: {}", size);
    }

    fn test_location() -> Location {
        Location::new(TEST_DIR.to_owned(), TEST_FILE)
    }
}
