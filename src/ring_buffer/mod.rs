use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use log::{error, trace};

use necronomicon::Owned;

use crate::{
    buffer::Buffer,
    entry::{last_metadata, Metadata, Readable, Version, Writable},
    Error,
};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq, PartialOrd, Ord))]
pub enum Remaining {
    Left(u64),
    Right(u64),
    Both { left: u64, right: u64 },
    None,
}

pub struct RingBuffer<B>(Arc<Inner<B>>)
where
    B: Buffer;

unsafe impl<B> Send for RingBuffer<B> where B: Buffer {}

impl<B> Clone for RingBuffer<B>
where
    B: Buffer,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<B> RingBuffer<B>
where
    B: Buffer,
{
    pub fn new(buffer: B, version: Version) -> Result<Self, Error> {
        Ok(Self(Arc::new(Inner::new(buffer, version)?)))
    }

    pub fn push(&self, buf: &[u8]) -> Result<u64, Error> {
        self.0.push(buf)
    }

    pub fn pop<O>(&self, buf: &mut O) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.0.pop(buf)
    }

    pub fn peek<O>(&self, buf: &mut O) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.0.peek(buf)
    }

    #[cfg(test)]
    fn inner(&self) -> &Inner<B> {
        &self.0
    }
}

/// A thread-safe ring buffer.
/// It is SPSC, so only one thread can write to it, and only one thread can read from it.
/// Therefore don't need to worry about contention.
struct Inner<B>
where
    B: Buffer,
{
    // for checking if data is available.
    has_data: AtomicBool,
    // ptrs for tracking where sections are in the buffer.
    // only read & write are tracked, they are the first 8 bytes in buffer.
    // peek is ephemeral and is not stored in the buffer.
    read_ptr: AtomicU64,
    write_ptr: AtomicU64,
    // TODO(nyanzebra): impl peek!!
    #[allow(dead_code)]
    peek_ptr: AtomicU64,

    // current entry
    entry: AtomicU64,

    // version of the ring buffer.
    version: Version,

    // The buffer itself.
    buffer: B,
}

impl<B> Inner<B>
where
    B: Buffer,
{
    // Create a new ring buffer.
    pub fn new(buffer: B, version: Version) -> Result<Self, Error> {
        let mut read_ptr = 0;
        let mut write_ptr = 0;
        let mut entry = 0;
        let mut has_data = false;

        if let Some(metadata) = last_metadata(&buffer, version)? {
            read_ptr = metadata.read_ptr();
            write_ptr = metadata.write_ptr();
            entry = metadata.entry();
            has_data = true;
        }

        Ok(Self {
            has_data: AtomicBool::new(has_data),
            read_ptr: AtomicU64::new(read_ptr),
            write_ptr: AtomicU64::new(write_ptr),
            peek_ptr: AtomicU64::new(0),
            entry: AtomicU64::new(entry),
            version,
            buffer,
        })
    }

    /// # Description
    /// Writes an entry to the ring buffer.
    ///
    /// # Example
    /// ```rust,ignore
    /// ring_buffer.push(b"hello world")?;
    /// ```
    ///
    /// # Arguments
    /// - `buf`: A slice of bytes that is the actual data being written to the buffer.
    ///
    /// # Errors
    /// See [`Error`] for more details.
    pub fn push(&self, buf: &[u8]) -> Result<u64, Error> {
        if buf.is_empty() {
            return Err(Error::EmptyData);
        }

        let read_ptr = self.read_ptr.load(Ordering::SeqCst);
        let mut write_ptr = self.write_ptr.load(Ordering::SeqCst);
        let entry = self.entry.load(Ordering::SeqCst) + 1;
        let has_data = self.has_data.load(Ordering::SeqCst);

        trace!(
            "push original read_ptr: {}, write_ptr: {}",
            read_ptr,
            write_ptr
        );

        let len = buf.len() as u32;
        let entry_size = Metadata::struct_size(self.version) as u64
            + Metadata::calculate_data_size(self.version, len) as u64;

        // if there is data and write_ptr == read_ptr, then the buffer is full.
        if has_data && write_ptr == read_ptr {
            return Err(Error::EntryTooBig {
                entry_size,
                remaining: self.remaining(has_data, write_ptr, read_ptr),
            });
        }

        let data = Writable::new(self.version, buf);
        let metadata = Metadata::new(
            self.version,
            entry,
            read_ptr,
            (write_ptr + entry_size) % self.buffer.capacity(),
            len,
        );

        // If the entry is too big, we can't write.
        if entry_size > self.buffer.capacity() {
            return Err(Error::EntryTooBig {
                entry_size,
                remaining: self.remaining(has_data, write_ptr, read_ptr),
            });
        }

        // check if write ptr would pass read ptr.
        if write_ptr < read_ptr && write_ptr + entry_size > read_ptr {
            return Err(Error::EntryTooBig {
                entry_size,
                remaining: self.remaining(has_data, write_ptr, read_ptr),
            });
        }

        // check if entry will fit at right side, otherwise see if fits in left.
        // if it fits move write ptr to left.
        if write_ptr + entry_size > self.buffer.capacity() {
            if entry_size > read_ptr {
                return Err(Error::EntryTooBig {
                    entry_size,
                    remaining: self.remaining(has_data, write_ptr, read_ptr),
                });
            }

            write_ptr = 0;
        }

        // This is the final write pointer location for the data we are writing into buffer.
        let location = write_ptr;

        // write metadata
        self.buffer.encode_at(
            write_ptr as usize,
            Metadata::struct_size(self.version) as usize,
            &metadata,
        )?;

        write_ptr += Metadata::struct_size(self.version) as u64;

        // write data
        self.buffer
            .encode_at(write_ptr as usize, metadata.data_size() as usize, &data)?;

        write_ptr += metadata.data_size() as u64;
        write_ptr %= self.buffer.capacity();

        // TODO: see if it is better to do flush ranges instead.
        self.buffer.flush()?;

        self.write_ptr.store(write_ptr, Ordering::SeqCst);
        self.entry.store(entry, Ordering::SeqCst);
        self.has_data.store(true, Ordering::SeqCst);

        Ok(location)
    }

    // TODO(nyanzebra): This could ignore version and try all of them.
    /// # Description
    /// Removes an entry from the ring buffer.
    /// Note that it is possible to get duplicates if the ring buffer is reconstructed from
    /// a persisted [`Buffer`].
    ///
    /// # Example
    /// ```rust, ignore
    /// let mut data = [0u8; 1024];
    /// let data_size = ring_buffer.pop(&mut data)?;
    /// ```
    ///
    /// # Parameters
    /// - `buf`: The data buffer to write the entry into.
    ///
    /// # Returns
    /// Result with size of data read as Ok variant, or an Error.
    ///
    /// # Errors
    /// See [`Error`] for more details.
    pub fn pop<O>(&self, buf: &mut O) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.peek_or_pop(buf, false)
    }

    // TODO(nyanzebra): This could ignore version and try all of them.
    /// # Description
    /// Peeks an entry from the ring buffer.
    /// Note that it is possible to get duplicates if the ring buffer is reconstructed from
    /// a persisted [`Buffer`].
    ///
    /// # Example
    /// ```rust, ignore
    /// let mut data = [0u8; 1024];
    /// let data_size = ring_buffer.peek(&mut data)?;
    /// ```
    ///
    /// # Parameters
    /// - `buf`: The data buffer to write the entry into.
    ///
    /// # Returns
    /// Result with size of data read as Ok variant, or an Error.
    ///
    /// # Errors
    /// See [`Error`] for more details.
    pub fn peek<O>(&self, buf: &mut O) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.peek_or_pop(buf, true)
    }

    fn peek_or_pop<O>(&self, buf: &mut O, peek: bool) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        let mut read_ptr = self.read_ptr.load(Ordering::SeqCst);
        let write_ptr = self.write_ptr.load(Ordering::SeqCst);
        let has_data = self.has_data.load(Ordering::SeqCst);
        let metadata_struct_size = Metadata::struct_size(self.version);
        trace!("original read_ptr: {}, write_ptr: {}", read_ptr, write_ptr);

        // If the buffer is empty, we can't read.
        if !has_data {
            return Err(Error::BufferEmpty);
        }

        // handle wrap around case
        if read_ptr + metadata_struct_size as u64 > self.buffer.capacity() {
            read_ptr = 0;
        }

        trace!(
            "maybe wrap read_ptr: {}, write_ptr: {}",
            read_ptr,
            write_ptr
        );
        let (metadata, mut read_ptr) =
            self.read_metadata(metadata_struct_size as u64, read_ptr, write_ptr)?;

        // If the data buffer is too small to hold the data, we can't read.
        if buf.unfilled_capacity() < metadata.real_data_size() as usize {
            return Err(Error::BufferTooSmall(
                buf.unfilled_capacity() as u32,
                metadata.real_data_size(),
            ));
        }

        read_ptr += Metadata::struct_size(self.version) as u64;

        let data: Readable<O::Shared> =
            self.buffer
                .decode_at_owned(read_ptr as usize, metadata.data_size() as usize, buf)?;
        data.verify()?;
        read_ptr += metadata.data_size() as u64;

        read_ptr %= self.buffer.capacity();
        if !peek {
            self.read_ptr.store(read_ptr, Ordering::SeqCst);
            self.has_data.store(read_ptr != write_ptr, Ordering::SeqCst);
        }
        Ok(data)
    }

    fn remaining(&self, has_data: bool, write_ptr: u64, read_ptr: u64) -> Remaining {
        let capacity = self.buffer.capacity();
        if has_data {
            if write_ptr > read_ptr {
                if read_ptr == 0 {
                    Remaining::Right(capacity - write_ptr)
                } else {
                    Remaining::Both {
                        left: read_ptr,
                        right: capacity - write_ptr,
                    }
                }
            } else if read_ptr == write_ptr {
                Remaining::None
            } else if write_ptr == 0 {
                Remaining::Left(read_ptr)
            } else {
                Remaining::Left(read_ptr - write_ptr)
            }
        } else {
            Remaining::Left(capacity)
        }
    }

    fn read_metadata(
        &self,
        metadata_struct_size: u64,
        read_ptr: u64,
        write_ptr: u64,
    ) -> Result<(Metadata, u64), Error> {
        let capacity = self.buffer.capacity();

        let metadata = self
            .buffer
            .decode_at::<Metadata>(read_ptr as usize, metadata_struct_size as usize);
        match metadata {
            Ok(metadata) => {
                match metadata.verify() {
                    Ok(_) => Ok((metadata, read_ptr)),
                    Err(err) => {
                        trace!("failed to decode metadata of size {metadata_struct_size} with buf cap {}, read_ptr {}, write_ptr {} due to {err}", capacity, read_ptr, write_ptr);
                        let metadata = self
                            .buffer
                            .decode_at::<Metadata>(0, metadata_struct_size as usize).map_err(|err| {
                                error!("failed to decode metadata of size {metadata_struct_size} with buf cap {}, read_ptr {}, write_ptr {} due to {err}", capacity, read_ptr, write_ptr);
                                err
                            })?;
                        // If the metadata CRC does not match, we can't read.
                        metadata.verify().map_err(|err| {
                            error!("failed to verify metadata of size {metadata_struct_size} with buf cap {}, read_ptr {}, write_ptr {} due to {err}", capacity, read_ptr, write_ptr);
                            err
                        })?;
                        Ok((metadata, 0))
                    }
                }
            }
            Err(err) => {
                trace!("failed to decode metadata of size {metadata_struct_size} with buf cap {}, read_ptr {}, write_ptr {} due to {err}", capacity, read_ptr, write_ptr);
                let metadata = self
                    .buffer
                    .decode_at::<Metadata>(0, metadata_struct_size as usize).map_err(|err| {
                        error!("failed to decode metadata of size {metadata_struct_size} with buf cap {}, read_ptr {}, write_ptr {} due to {err}", capacity, read_ptr, write_ptr);
                        err
                    })?;
                // If the metadata CRC does not match, we can't read.
                metadata.verify().map_err(|err| {
                    error!("failed to verify metadata of size {metadata_struct_size} with buf cap {}, read_ptr {}, write_ptr {} due to {err}", capacity, read_ptr, write_ptr);
                    err
                })?;
                Ok((metadata, 0))
            }
        }
    }
}

pub struct Iter<'a, B> {
    buffer: &'a B,
    start: u64,
    end: u64,
    has_data: bool,
    version: Version,
}

impl<'a, B> Iterator for Iter<'a, B>
where
    B: Buffer,
{
    type Item = u64;

    // Very similar to `pop` except we don't update the read pointer and crash if cannot proceed.
    fn next(&mut self) -> Option<Self::Item> {
        // If the buffer is empty, we can't read.
        if !self.has_data {
            return None;
        }

        let next = self.start;

        // wrap around means we have read everything
        if self.start + Metadata::struct_size(self.version) as u64 > self.buffer.capacity() {
            self.start = 0;
        }

        let metadata = self
            .buffer
            .decode_at::<Metadata>(
                self.start as usize,
                Metadata::struct_size(self.version) as usize,
            )
            .expect("failed to decode metadata");

        // If the metadata CRC does not match, we can't read.
        metadata.verify().expect("corrupted metadata");

        self.start += Metadata::struct_size(self.version) as u64;

        // handle wrap around case
        self.start += metadata.data_size() as u64;
        self.start %= self.buffer.capacity();
        self.has_data = self.start != self.end;

        Some(next)
    }
}

pub struct Pusher<B>(RingBuffer<B>)
where
    B: Buffer;

impl<B> Pusher<B>
where
    B: Buffer,
{
    pub fn new(buffer: RingBuffer<B>) -> Self {
        Self(buffer)
    }

    pub fn push<S>(&self, buf: BinaryData<S>) -> Result<u64, Error>
    where
        S: Shared,
    {
        self.0.push(buf)
    }
}

pub struct Popper<B>(RingBuffer<B>)
where
    B: Buffer;

impl<B> Popper<B>
where
    B: Buffer,
{
    pub fn new(dequeue: RingBuffer<B>) -> Self {
        Self(dequeue)
    }

    pub fn pop<O>(&self, buf: &mut O) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.0.pop(buf)
    }

    pub fn peek<O>(&self, buf: &mut O) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.0.peek(buf)
    }
}

pub fn ring_buffer<B>(buffer: B, version: Version) -> Result<(Pusher<B>, Popper<B>), Error>
where
    B: Buffer,
{
    let buffer = RingBuffer::new(buffer, version)?;

    let pusher = Pusher::new(buffer.clone());
    let popper = Popper::new(buffer);

    Ok((pusher, popper))
}

#[cfg(test)]
mod tests {

    use std::{
        fs::OpenOptions,
        io::{Cursor, Read, Write},
        sync::atomic::Ordering,
        thread::{sleep, spawn},
        time::Duration,
    };

    use coverage_helper::test;
    use matches::assert_matches;
    use necronomicon::{Decode, DecodeOwned, Encode, Pool, PoolImpl, Shared, SharedImpl};

    use crate::{
        buffer::{InMemBuffer, MmapBuffer},
        entry::{Metadata, Readable, Version, Writable},
        Error,
    };

    use super::{Remaining, RingBuffer};

    #[test]
    fn test_init() {
        let dir = tempfile::tempdir().expect("tempfile");

        let buffer =
            MmapBuffer::new(dir.path().to_path_buf().join("test_buffer"), 1024).expect("buffer");

        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        for _ in 0..5 {
            ring_buffer.push(b"kittens").expect("push");
        }

        let pool = PoolImpl::new(1024, 1024);

        for _ in 0..3 {
            let mut buf = pool.acquire("pop");
            let data = ring_buffer.pop(&mut buf).expect("pop");
            assert_eq!(&data.into_inner().data().as_slice()[..7], b"kittens");
        }

        // We don't write the read ptr on reads, so to test we do another write.
        ring_buffer.push(b"kittens").expect("push");

        let expected_read_ptr = ring_buffer.inner().read_ptr.load(Ordering::SeqCst);
        let expected_write_ptr = ring_buffer.inner().write_ptr.load(Ordering::SeqCst);
        let expected_has_data = ring_buffer.inner().has_data.load(Ordering::SeqCst);
        let expected_entry = ring_buffer.inner().entry.load(Ordering::SeqCst);

        let buffer1 =
            MmapBuffer::new(dir.path().to_path_buf().join("test_buffer"), 1024).expect("buffer");

        let ring_buffer2 = RingBuffer::new(buffer1, Version::V1).expect("new buffer");
        assert_eq!(
            ring_buffer2.inner().read_ptr.load(Ordering::SeqCst),
            expected_read_ptr
        );
        assert_eq!(
            ring_buffer2.inner().write_ptr.load(Ordering::SeqCst),
            expected_write_ptr
        );
        assert_eq!(
            ring_buffer2.inner().has_data.load(Ordering::SeqCst),
            expected_has_data
        );
        assert_eq!(
            ring_buffer2.inner().entry.load(Ordering::SeqCst),
            expected_entry
        );
    }

    #[test]
    fn test_peek_buffer() {
        let buffer = InMemBuffer::new(1024);
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        ring_buffer.push(b"kittens").unwrap();

        let pool = PoolImpl::new(1024, 1024);

        let mut buf = pool.acquire("pop");
        let data = ring_buffer.peek(&mut buf).unwrap();
        let data = data.into_inner();
        assert_eq!(data.len(), 7);
        assert_eq!(data.data().as_slice(), b"kittens");

        let mut buf = pool.acquire("pop");
        let data = ring_buffer.peek(&mut buf).unwrap();
        let data = data.into_inner();
        assert_eq!(data.len(), 7);
        assert_eq!(data.data().as_slice(), b"kittens");

        let mut buf = pool.acquire("pop");
        let data = ring_buffer.pop(&mut buf).unwrap();
        let data = data.into_inner();
        assert_eq!(data.len(), 7);
        assert_eq!(data.data().as_slice(), b"kittens");

        let mut buf = pool.acquire("pop");
        assert!(ring_buffer.peek(&mut buf).is_err());
    }

    #[test]
    fn test_pop_buffer_empty() {
        let buffer = InMemBuffer::new(1024);
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let pool = PoolImpl::new(1024, 1024);
        let mut buf = pool.acquire("pop");

        let res = ring_buffer.pop(&mut buf).unwrap_err();
        assert_matches!(res, Error::BufferEmpty);
    }

    #[test]
    fn test_pop_metadata_crc_mismatch() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(20, Ordering::SeqCst);
        ring_buffer.inner().has_data.store(true, Ordering::SeqCst);

        let mut meta = Metadata::new(Version::V1, 1, 4, 10, 10);
        match &mut meta {
            Metadata::Version1(meta) => {
                meta.set_crc(1234567);
            }
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        meta.encode(&mut file).unwrap();

        let pool = PoolImpl::new(1024, 1024);

        let mut buf = pool.acquire("pop");

        let result = ring_buffer.pop(&mut buf);
        assert_matches!(
            result.unwrap_err(),
            Error::MetadataCrcMismatch {
                expected: 1234567,
                actual: 1970696030,
            }
        );
    }

    #[test]
    fn test_pop_data_crc_mismatch() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(512, Ordering::SeqCst);
        ring_buffer.inner().has_data.store(true, Ordering::SeqCst);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = b"hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let mut data = Writable::new(Version::V1, data);
        match &mut data {
            Writable::Version1(data) => data.crc = 1234567,
        }

        let mut buf = vec![0u8; 1024];
        meta.encode(&mut Cursor::new(
            &mut buf[..Metadata::struct_size(Version::V1) as usize],
        ))
        .unwrap();
        data.encode(&mut Cursor::new(
            &mut buf[Metadata::struct_size(Version::V1) as usize..],
        ))
        .unwrap();
        file.write_all(&buf).expect("write");

        let pool = PoolImpl::new(1024, 1024);

        let mut buf = pool.acquire("pop");
        let result = ring_buffer.pop(&mut buf);
        assert_matches!(
            result.unwrap_err(),
            Error::DataCrcMismatch {
                expected: 1234567,
                actual: 222957957
            }
        );
    }

    #[test]
    fn test_pop_buffer_too_small() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(50, Ordering::SeqCst);
        ring_buffer.inner().has_data.store(true, Ordering::SeqCst);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = b"hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let data = Writable::new(Version::V1, data);

        meta.encode(&mut file).unwrap();
        data.encode(&mut file).unwrap();

        let pool = PoolImpl::new(1, 1024);

        let mut buf = pool.acquire("pop");
        let result = ring_buffer.pop(&mut buf);
        assert_matches!(result.unwrap_err(), Error::BufferTooSmall(1, 11));
    }

    #[test]
    fn test_pop_wrap_around_metadata() {
        const METADATA_SPOT: u32 = 1024 - (Metadata::struct_size(Version::V1) / 2);

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer.inner().write_ptr.store(0, Ordering::SeqCst);
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::SeqCst);
        ring_buffer
            .inner()
            .read_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::SeqCst);
        ring_buffer.inner().has_data.store(true, Ordering::SeqCst);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = b"hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let data = Writable::new(Version::V1, data);

        let mut buf = vec![0u8; 1024];
        meta.encode(&mut Cursor::new(
            &mut buf[..Metadata::struct_size(Version::V1) as usize],
        ))
        .unwrap();
        data.encode(&mut Cursor::new(
            &mut buf[Metadata::struct_size(Version::V1) as usize..],
        ))
        .unwrap();
        file.write_all(&buf).expect("write");

        let pool = PoolImpl::new(1024, 1024);

        let mut buf = pool.acquire("pop");
        let data = ring_buffer.pop(&mut buf).unwrap();
        assert_matches!(data.into_inner().data().as_slice(), b"hello world");
    }

    #[test]
    fn test_pop_no_wrap_around() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(50, Ordering::SeqCst);
        ring_buffer.inner().has_data.store(true, Ordering::SeqCst);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = b"hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let data = Writable::new(Version::V1, data);

        let mut buf = vec![0u8; 1024];
        meta.encode(&mut Cursor::new(
            &mut buf[..Metadata::struct_size(Version::V1) as usize],
        ))
        .unwrap();
        data.encode(&mut Cursor::new(
            &mut buf[Metadata::struct_size(Version::V1) as usize..],
        ))
        .unwrap();
        file.write_all(&buf).expect("write");

        let pool = PoolImpl::new(1024, 1024);

        let mut buf = pool.acquire("pop");
        let data = ring_buffer.pop(&mut buf).unwrap();
        assert_matches!(data.into_inner().data().as_slice(), b"hello world");
    }

    #[test]
    fn test_push_entry_larger_than_buffer() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file, 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let data = &[0u8; 1024_usize + 1];
        assert_matches!(
            ring_buffer.push(data),
            Err(Error::EntryTooBig { entry_size, remaining }) if entry_size == 1075 && remaining == Remaining::Left(1024)
        );
    }

    #[test]
    fn test_push_entry_too_big_due_to_metadata_wrap() {
        const METADATA_SPOT: u32 = 1024 - (Metadata::struct_size(Version::V1) / 2);

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file, 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::SeqCst);

        let data = &[0u8; 40];
        assert_matches!(ring_buffer.push(data), Err(Error::EntryTooBig { .. }));
    }

    #[test]
    fn test_push_entry_too_big_due_for_remaining_space() {
        const METADATA_SPOT: u32 = 1024 - (Metadata::struct_size(Version::V1) / 2);
        const READ_WRAP: u32 = 10;

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file, 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::SeqCst);
        ring_buffer
            .inner()
            .read_ptr
            .fetch_add(READ_WRAP as u64, Ordering::SeqCst);

        let data = &[0u8; 40];
        assert_matches!(ring_buffer.push(data), Err(Error::EntryTooBig { .. }));
    }

    #[test]
    fn test_push_entry_too_big_due_for_remaining_space_with_metadata_wrap() {
        const METADATA_SPOT: u32 = 1024 - 6;
        const READ_WRAP: u32 = 49;

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file, 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .store(METADATA_SPOT as u64, Ordering::SeqCst);
        ring_buffer
            .inner()
            .read_ptr
            .store(READ_WRAP as u64, Ordering::SeqCst);
        ring_buffer.inner().has_data.store(true, Ordering::SeqCst);

        let data = b"hello world 19";
        assert_matches!(ring_buffer.push(data), Err(Error::EntryTooBig { .. }));
    }

    #[test]
    fn test_push_no_wrap_around() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let data = b"abcdefghijklmnopqrstuvwxyz";
        ring_buffer.push(data).unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let mut data = vec![0u8; 256]; // Large enough buf to be sure we got everything
        let _ = file.read(&mut data).unwrap();

        let Metadata::Version1(meta) = Metadata::decode(&mut Cursor::new(
            &data[..Metadata::struct_size(Version::V1) as usize],
        ))
        .unwrap();
        meta.verify().unwrap();
        assert_eq!(meta.read_ptr(), 0);
        assert_eq!(meta.write_ptr(), 76);
        assert_eq!(meta.entry(), 1);
        assert_eq!(meta.size(), 26);

        let start = Metadata::struct_size(Version::V1) as usize;
        let end = Metadata::Version1(meta).data_size() as usize + start;
        let pool = PoolImpl::new(1024, 1024);

        let mut buf = pool.acquire("pop");
        let data =
            Readable::decode_owned(&mut Cursor::new(&mut data[start..end]), &mut buf).unwrap();
        data.verify().unwrap();
        assert_eq!(
            data.into_inner().data().as_slice(),
            b"abcdefghijklmnopqrstuvwxyz"
        );
    }

    #[test]
    fn test_fill() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        // fill buffer
        let mut i = 0;
        while let Ok(_) = ring_buffer.push(format!("hello {i}").as_bytes()) {
            i += 1;
        }

        let mut reads = vec![];

        let pool = PoolImpl::new(1024, 1024);

        // read half
        loop {
            let mut buf = pool.acquire("pop");
            if let Ok(data) = ring_buffer.pop(&mut buf) {
                reads
                    .push(String::from_utf8(data.into_inner().data().as_slice().to_vec()).unwrap());
            } else {
                break;
            }
        }

        // fill buffer again
        while let Ok(_) = ring_buffer.push(format!("hello {i}").as_bytes()) {
            i += 1;
        }

        // read all
        loop {
            let mut buf = pool.acquire("pop");
            match ring_buffer.pop(&mut buf) {
                Ok(data) => {
                    reads.push(
                        String::from_utf8(data.into_inner().data().as_slice().to_vec()).unwrap(),
                    );
                }
                Err(Error::BufferEmpty) => break,
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }

        assert_eq!(reads.len(), i);
        for i in 0..reads.len() {
            assert_eq!(reads[i].trim_matches(char::from(0)), format!("hello {i}"));
        }
    }

    #[test]
    fn test_1_million_push_pop() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024 * 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let data = [7; 131];
        let pool = PoolImpl::new(1024, 1024);

        // fill buffer
        let mut i = 0;
        loop {
            while let Err(err) = ring_buffer.push(&data) {
                if let Error::EntryTooBig { .. } = err {
                    let mut buf = pool.acquire("pop");
                    if let Ok(readable) = ring_buffer.pop(&mut buf) {
                        assert_eq!(readable.into_inner().data().as_slice(), data);
                    } else {
                        break;
                    }
                }
            }

            i += 1;
            if i == 1_000_000 {
                break;
            }
        }
    }

    #[test]
    fn thread_safety_test() {
        const ENTRIES: usize = 100_000;
        let buffer = InMemBuffer::new(1024);
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let reader = {
            let ring_buffer = ring_buffer.clone();
            spawn(move || {
                let pool = PoolImpl::new(1024, 1024);
                let mut retries: u8 = 10;
                let mut i = 0;
                loop {
                    let mut buf = pool.acquire("pop");
                    match ring_buffer.pop(&mut buf) {
                        Ok(data) => {
                            retries = 10;
                            verify_data(data, i);
                            i += 1;
                            if i == ENTRIES {
                                break;
                            }
                        }
                        Err(Error::BufferEmpty) => {
                            if retries == 0 {
                                panic!("retries exhausted");
                            }

                            sleep(Duration::from_millis(1));
                            retries -= 1;
                            continue;
                        }
                        Err(err) => {
                            panic!("unexpected error: {:?}", err);
                        }
                    }
                }
            })
        };

        let writer = spawn(move || {
            for i in 0..ENTRIES {
                let data = test_data(i);

                loop {
                    match ring_buffer.push(&data) {
                        Ok(_) => {
                            break;
                        }
                        Err(Error::EntryTooBig { .. }) => {
                            sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(err) => {
                            panic!("unexpected error: {:?}", err);
                        }
                    }
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    fn test_data(i: usize) -> Vec<u8> {
        format!("hello world {}", i).into_bytes()
    }

    fn verify_data(data: Readable<SharedImpl>, i: usize) {
        let s =
            unsafe { String::from_utf8_unchecked(data.into_inner().data().as_slice().to_vec()) };

        let split: Vec<_> = s.split(' ').collect();
        assert_eq!(split[0], "hello");
        assert_eq!(split[1], "world");
        assert_eq!(split[2].parse::<usize>().unwrap(), i);
    }
}
