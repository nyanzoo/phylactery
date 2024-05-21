use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use necronomicon::Owned;

use crate::{
    buffer::Buffer,
    entry::{last_metadata, Metadata, Readable, Version, Writable},
    Error,
};

pub struct DequeueNode<B>
where
    B: Buffer,
{
    buffer: B,
    write: AtomicU64,
    read: AtomicU64,
    entry: AtomicU64,
    has_data: AtomicBool,
    version: Version,
}

impl<B> AsRef<[u8]> for DequeueNode<B>
where
    B: Buffer,
{
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl<B> AsMut<[u8]> for DequeueNode<B>
where
    B: Buffer,
{
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut()
    }
}

#[derive(Debug)]
pub struct Push {
    pub offset: u64,
    pub len: u64,
    pub crc: u32,
}

// SPSC queue
impl<B> DequeueNode<B>
where
    B: Buffer,
{
    pub fn new(buffer: B, version: Version) -> Result<Self, Error> {
        let mut read = 0;
        let mut write = 0;
        let mut entry = 0;
        let mut has_data = false;

        if let Some(metadata) = last_metadata(&buffer, version)? {
            read = metadata.read_ptr();
            write = metadata.write_ptr();
            entry = metadata.entry();
            has_data = true;
        }

        Ok(Self {
            buffer,
            write: AtomicU64::new(write),
            read: AtomicU64::new(read),
            entry: AtomicU64::new(entry),
            has_data: AtomicBool::new(has_data),
            version,
        })
    }

    /// # Description
    /// Writes an entry to the queue.
    ///
    /// # Example
    /// ```rust,ignore
    /// queue.pop(buf)?;
    /// assert_eq!(buf, b"hello world");
    /// ```
    ///
    /// # Arguments
    /// - `buf`: A slice of bytes that stores results from pop.
    ///
    /// # Errors
    /// See [`Error`] for more details.
    pub fn pop<O>(&self, buf: &mut O) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        let mut read_ptr = self.read.load(Ordering::Acquire);
        let write_ptr = self.write.load(Ordering::Acquire);
        let has_data = self.has_data.load(Ordering::Acquire);

        if read_ptr == write_ptr {
            if has_data {
                return Err(Error::NodeFull);
            } else {
                return Err(Error::EmptyData);
            }
        }

        let start = read_ptr as usize;
        let len = Metadata::struct_size(self.version) as usize;
        let metadata: Metadata = self.buffer.decode_at(start, len)?;
        metadata.verify()?;

        let start = start + len;
        let len = metadata.data_size() as usize;
        let data: Readable<O::Shared> = self.buffer.decode_at_owned(start, len, buf)?;
        data.verify()?;

        read_ptr = (start + len) as u64;
        self.read.store(read_ptr, Ordering::Release);

        Ok(data)
    }

    /// # Description
    /// Writes an entry to the queue.
    ///
    /// # Example
    /// ```rust,ignore
    /// queue.push(binary_data(b"hello world"))?;
    /// ```
    ///
    /// # Arguments
    /// - `buf`: A slice of bytes that is the actual data being written to the buffer.
    ///
    /// # Errors
    /// See [`Error`] for more details.
    pub fn push(&self, buf: &[u8]) -> Result<Push, Error> {
        if buf.is_empty() {
            return Err(Error::EmptyData);
        }

        let mut write_ptr = self.write.load(Ordering::Acquire);
        let orig_write_ptr = write_ptr;
        let read_ptr = self.read.load(Ordering::Acquire);
        let entry = self.entry.load(Ordering::Acquire) + 1;

        let data_size = buf.len() as u32;
        let data = Writable::new(self.version, buf);
        let entry_size = Metadata::struct_size(self.version) as u64
            + Metadata::calculate_data_size(self.version, data_size) as u64;
        let metadata = Metadata::new(
            self.version,
            entry,
            read_ptr,
            write_ptr + entry_size,
            data_size,
        );

        // If the entry is too big, we can't write.
        if entry_size > self.buffer.capacity() {
            return Err(Error::EntryLargerThanNode(
                entry_size,
                self.buffer.capacity(),
            ));
        }

        if write_ptr + entry_size > self.buffer.capacity() {
            return Err(Error::NodeFull);
        }

        // We need the original ptr for returning where the data is stored.
        let offset = write_ptr;

        // write the metadata.
        self.buffer.encode_at(
            write_ptr as usize,
            Metadata::struct_size(self.version) as usize,
            &metadata,
        )?;

        write_ptr += Metadata::struct_size(self.version) as u64;

        // write the data.
        self.buffer
            .encode_at(write_ptr as usize, metadata.data_size() as usize, &data)?;

        write_ptr += Metadata::calculate_data_size(self.version, data_size) as u64;

        // update the write pointer.
        let len = write_ptr - orig_write_ptr;
        self.buffer.flush_range(offset as usize, len as usize)?;
        self.write.store(write_ptr, Ordering::Release);
        self.entry.store(entry, Ordering::Release);
        self.has_data.store(true, Ordering::Release);

        Ok(Push {
            offset,
            len,
            crc: data.crc(),
        })
    }

    /// # Description
    /// Returns the current Write pointer.
    ///
    /// # Example
    /// ```rust,ignore
    /// let write_ptr = node.write_ptr();
    /// ```
    ///
    /// # Returns
    /// The current write pointer.
    pub fn write_ptr(&self) -> u64 {
        self.write.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod test {
    use matches::assert_matches;
    use necronomicon::{Pool, PoolImpl, Shared};

    use crate::{buffer::InMemBuffer, entry::Version};

    use super::{super::Error, DequeueNode};

    #[test]
    fn test_new_fresh() {
        let buffer = InMemBuffer::new(128);
        let node = DequeueNode::new(buffer, Version::V1).unwrap();

        assert_eq!(node.read.load(std::sync::atomic::Ordering::Acquire), 0);
        assert_eq!(node.write.load(std::sync::atomic::Ordering::Acquire), 0);
        assert_eq!(node.entry.load(std::sync::atomic::Ordering::Acquire), 0);
        assert!(!node.has_data.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn test_new_with_data() {
        let buffer = InMemBuffer::new(128);
        let node = DequeueNode::new(buffer, Version::V1).unwrap();

        node.push(b"hello world").unwrap();

        let buffer = node.buffer;
        let node = DequeueNode::new(buffer, Version::V1).unwrap();

        assert_eq!(node.read.load(std::sync::atomic::Ordering::Acquire), 0);
        assert_eq!(node.write.load(std::sync::atomic::Ordering::Acquire), 61);
        assert_eq!(node.entry.load(std::sync::atomic::Ordering::Acquire), 1);
        assert!(node.has_data.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn test_push_pop() {
        let buffer = InMemBuffer::new(128);
        let node = DequeueNode::new(buffer, Version::V1).unwrap();

        node.push(b"hello world").unwrap();

        let pool = PoolImpl::new(1024, 1024);
        let mut owned = pool.acquire("pop");
        let data = node.pop(&mut owned).unwrap();

        assert_eq!(data.into_inner().data().as_slice(), b"hello world");
    }

    #[test]
    fn test_push_node_full() {
        let buffer = InMemBuffer::new(64);
        let node = DequeueNode::new(buffer, Version::V1).unwrap();

        node.push(b"hello world").unwrap();
        assert_matches!(node.push(b"hello world"), Err(Error::NodeFull));
    }

    #[test]
    fn test_push_entry_too_large() {
        let buffer = InMemBuffer::new(128);
        let node = DequeueNode::new(buffer, Version::V1).unwrap();

        assert_matches!(
            node.push(&[0u8; 129]),
            Err(Error::EntryLargerThanNode(179, 128))
        );
    }

    #[test]
    fn test_pop_empty_data() {
        let buffer = InMemBuffer::new(128);
        let node = DequeueNode::new(buffer, Version::V1).unwrap();

        let pool = PoolImpl::new(1024, 1024);

        let mut owned = pool.acquire("pop");

        assert_matches!(node.pop(&mut owned), Err(Error::EmptyData));
    }
}
