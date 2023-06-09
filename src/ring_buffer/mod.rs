use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use crate::{
    buffer::Buffer,
    codec::Encode,
    entry::{crc_check, last_metadata, Data, Metadata, Version},
};

use self::error::Error;

pub mod error;

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

    pub fn push(&self, buf: &[u8]) -> Result<(), Error> {
        self.0.push(buf)
    }

    pub fn pop(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.0.pop(buf)
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
    pub fn push(&self, buf: &[u8]) -> Result<(), Error> {
        if buf.is_empty() {
            return Err(Error::EmptyData);
        }

        let read_ptr = self.read_ptr.load(Ordering::Acquire);
        let mut write_ptr = self.write_ptr.load(Ordering::Acquire);
        let entry = self.entry.load(Ordering::Acquire) + 1;
        let has_data = self.has_data.load(Ordering::Acquire);

        let data = Data::new(self.version, buf);
        let entry_size = Metadata::size(self.version) as u64
            + Metadata::calculate_data_size(self.version, buf.len() as u32) as u64;
        let metadata = Metadata::new(
            self.version,
            entry,
            read_ptr,
            write_ptr + entry_size,
            buf.len() as u32,
        );

        // If the entry is too big, we can't write.
        if entry_size > self.buffer.capacity() {
            return Err(Error::EntryLargerThanBuffer(
                entry_size,
                self.buffer.capacity(),
            ));
        }

        // check if metadata can fit without wrapping
        let mut has_wrapped = false;
        if write_ptr + Metadata::size(self.version) as u64 > self.buffer.capacity() {
            write_ptr = 0;
            has_wrapped = true;
        }

        // If the entry is too big to fit in the remaining buffer, we can't write.
        if has_wrapped
            && (write_ptr + metadata.data_size() as u64) % self.buffer.capacity() > read_ptr
        {
            let remaining = if write_ptr > read_ptr {
                self.buffer.capacity() - (write_ptr - read_ptr)
            } else {
                read_ptr - write_ptr
            };
            return Err(Error::EntryTooBig(entry_size, remaining));
        }

        // Also need to check if data would wrap and would be too big
        let next_write_ptr = write_ptr + entry_size;

        let pass = write_ptr < read_ptr
            && next_write_ptr < self.buffer.capacity()
            && next_write_ptr > read_ptr;

        let pass_around = write_ptr > read_ptr
            && next_write_ptr > self.buffer.capacity()
            && next_write_ptr % self.buffer.capacity() > read_ptr;

        if has_data && (pass || pass_around) {
            let remaining = if write_ptr > read_ptr {
                self.buffer.capacity() - (write_ptr - read_ptr)
            } else {
                read_ptr - write_ptr
            };
            return Err(Error::EntryTooBig(entry_size, remaining));
        }

        // write metadata
        self.buffer.encode_at(
            write_ptr as usize,
            Metadata::size(self.version) as usize,
            &metadata,
        )?;

        write_ptr += Metadata::size(self.version) as u64;

        // write data
        if write_ptr + metadata.data_size() as u64 > self.buffer.capacity() {
            let remaining = self.buffer.capacity() - write_ptr;
            let mut temp = vec![0; metadata.data_size() as usize];
            data.encode(&mut temp)?;
            let (left, right) = temp.split_at(remaining as usize);

            self.buffer.write_at(left, write_ptr as usize)?;
            self.buffer.write_at(right, 0)?;
        } else {
            self.buffer
                .encode_at(write_ptr as usize, metadata.data_size() as usize, &data)?;
        }

        write_ptr += metadata.data_size() as u64;
        write_ptr %= self.buffer.capacity();

        self.write_ptr.store(write_ptr, Ordering::Release);
        self.entry.store(entry, Ordering::Release);
        self.has_data.store(true, Ordering::Release);

        Ok(())
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
    pub fn pop(&self, buf: &mut [u8]) -> Result<usize, Error> {
        let mut read_ptr = self.read_ptr.load(Ordering::Acquire);
        let write_ptr = self.write_ptr.load(Ordering::Acquire);
        let has_data = self.has_data.load(Ordering::Acquire);

        // If the buffer is empty, we can't read.
        if !has_data {
            return Err(Error::BufferEmpty);
        }

        // handle wrap around case
        if read_ptr + Metadata::size(self.version) as u64 > self.buffer.capacity() {
            read_ptr = 0;
        }

        let metadata = self
            .buffer
            .decode_at::<Metadata>(read_ptr as usize, Metadata::size(self.version) as usize)?;

        // If the metadata CRC does not match, we can't read.
        metadata.verify()?;

        // If the data buffer is too small to hold the data, we can't read.
        if buf.len() < metadata.real_data_size() as usize {
            return Err(Error::BufferTooSmall(
                buf.len() as u32,
                metadata.real_data_size(),
            ));
        }

        read_ptr += Metadata::size(self.version) as u64;

        // handle wrap around case
        if read_ptr + metadata.data_size() as u64 > self.buffer.capacity() {
            // Read data in buf anyway, we will manually verify to avoid another copy.
            let len = metadata.real_data_size() as usize;
            let skip = metadata.data_encoding_metadata_size();
            let buf = &mut buf[..len];
            read_ptr += skip as u64;
            // it is possible that the data encoding metadata from encoding pushes the start
            // of real data to the beginning of the buffer allowing for a contiguous read.
            if read_ptr > self.buffer.capacity() {
                read_ptr %= self.buffer.capacity();
                self.buffer.read_at(buf, read_ptr as usize)?;
            } else {
                // need to split into left and right
                let split = if (read_ptr + len as u64) > self.buffer.capacity() {
                    let right = (read_ptr + len as u64) % self.buffer.capacity();
                    let left = len as u64 - right;
                    left as usize
                } else {
                    len
                };
                self.buffer.read_at(&mut buf[..split], read_ptr as usize)?;
                self.buffer.read_at(&mut buf[split..], 0)?;
            }
            read_ptr += len as u64;

            // it is also possible that crc is wrapped around
            let mut crc = [0u8; 4];
            if read_ptr > self.buffer.capacity() {
                read_ptr %= self.buffer.capacity();
                self.buffer.read_at(&mut crc, read_ptr as usize)?;
            } else {
                // need to split into left and right
                let split = if (read_ptr + 4) > self.buffer.capacity() {
                    let right = (read_ptr + 4) % self.buffer.capacity();
                    let left = 4 - right;
                    left as usize
                } else {
                    4
                };
                self.buffer.read_at(&mut crc[..split], read_ptr as usize)?;
                self.buffer.read_at(&mut crc[split..], 0)?;
            }

            // TODO(rojang): it seems bincode uses little endian for u32, but we should
            // make sure this is the case always?
            let crc = u32::from_le_bytes(crc);
            crc_check(crc, &buf[..metadata.real_data_size() as usize])?;

            read_ptr += 4;
        } else {
            let data: Data<'_> = self
                .buffer
                .decode_at(read_ptr as usize, metadata.data_size() as usize)?;
            data.verify()?;
            data.copy_into(buf);
            read_ptr += metadata.data_size() as u64;
        }

        read_ptr %= self.buffer.capacity();
        self.read_ptr.store(read_ptr, Ordering::Release);
        self.has_data
            .store(read_ptr != write_ptr, Ordering::Release);

        Ok(metadata.real_data_size() as usize)
    }
}

#[cfg(test)]
mod tests {

    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
        os::unix::prelude::FileExt,
        sync::atomic::Ordering,
        thread::{sleep, spawn},
        time::Duration,
    };

    use matches::assert_matches;

    use crate::{
        buffer::{InMemBuffer, MmapBuffer},
        codec::Encode,
        entry::{self, Data, Metadata, Version},
    };

    use super::{error::Error, RingBuffer};

    #[test]
    fn test_init() {
        let dir = tempfile::tempdir().expect("tempfile");

        let buffer =
            MmapBuffer::new(dir.path().to_path_buf().join("test_buffer"), 1024).expect("buffer");

        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        for _ in 0..5 {
            ring_buffer.push(b"kittens").expect("push");
        }

        let mut buf = [0u8; 1024];

        for _ in 0..3 {
            ring_buffer.pop(&mut buf).expect("pop");
            assert_eq!(&buf[..7], b"kittens");
        }

        // We don't write the read ptr on reads, so to test we do another write.
        ring_buffer.push(b"kittens").expect("push");

        let expected_read_ptr = ring_buffer.inner().read_ptr.load(Ordering::Acquire);
        let expected_write_ptr = ring_buffer.inner().write_ptr.load(Ordering::Acquire);
        let expected_has_data = ring_buffer.inner().has_data.load(Ordering::Acquire);
        let expected_entry = ring_buffer.inner().entry.load(Ordering::Acquire);

        let buffer1 =
            MmapBuffer::new(dir.path().to_path_buf().join("test_buffer"), 1024).expect("buffer");

        let ring_buffer2 = RingBuffer::new(buffer1, Version::V1).expect("new buffer");
        assert_eq!(
            ring_buffer2.inner().read_ptr.load(Ordering::Acquire),
            expected_read_ptr
        );
        assert_eq!(
            ring_buffer2.inner().write_ptr.load(Ordering::Acquire),
            expected_write_ptr
        );
        assert_eq!(
            ring_buffer2.inner().has_data.load(Ordering::Acquire),
            expected_has_data
        );
        assert_eq!(
            ring_buffer2.inner().entry.load(Ordering::Acquire),
            expected_entry
        );
    }

    #[test]
    fn test_pop_buffer_empty() {
        let buffer = InMemBuffer::new(1024);
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let mut data = vec![0u8; 10];
        let res = ring_buffer.pop(&mut data).unwrap_err();
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
            .fetch_add(20, Ordering::Release);
        ring_buffer.inner().has_data.store(true, Ordering::Release);

        let mut meta = Metadata::new(Version::V1, 1, 4, 10, 10);
        match &mut meta {
            Metadata::Version1(meta) => {
                meta.crc = 1234567;
            }
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        file.write_at(bincode::serialize(&meta).unwrap().as_slice(), 0)
            .expect("write metadata");

        let mut data = vec![0u8; 20];
        let result = ring_buffer.pop(&mut data);
        assert_matches!(
            result.unwrap_err(),
            Error::Entry(entry::Error::MetadataCrcMismatch {
                expected: 1234567,
                actual: 1970696030,
            })
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
            .fetch_add(512, Ordering::Release);
        ring_buffer.inner().has_data.store(true, Ordering::Release);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = "hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let mut data = Data::new(Version::V1, data.as_bytes());
        match &mut data {
            Data::Version1(data) => {
                data.crc = 1234567;
            }
        }

        let mut buf = vec![0u8; 1024];
        meta.encode(&mut buf[..Metadata::size(Version::V1) as usize])
            .unwrap();
        data.encode(&mut buf[Metadata::size(Version::V1) as usize..])
            .unwrap();
        file.write_all(&buf).expect("write");

        let mut data = vec![0u8; 40];
        let result = ring_buffer.pop(&mut data);
        assert_matches!(
            result.unwrap_err(),
            Error::Entry(entry::Error::DataCrcMismatch {
                expected: 1234567,
                actual: 222957957
            })
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
            .fetch_add(50, Ordering::Release);
        ring_buffer.inner().has_data.store(true, Ordering::Release);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = "hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let data = Data::new(Version::V1, data.as_bytes());

        file.write_all(bincode::serialize(&meta).unwrap().as_slice())
            .expect("write metadata");
        file.write_all(bincode::serialize(&data).unwrap().as_slice())
            .expect("write data");

        let mut data = vec![0u8; 1];
        let result = ring_buffer.pop(&mut data);
        assert_matches!(result.unwrap_err(), Error::BufferTooSmall(1, 11));
    }

    #[test]
    fn test_pop_wrap_around_data() {
        const METADATA_SPOT: u32 = 1024 - Metadata::size(Version::V1);
        const DATA_SPOT: u32 = METADATA_SPOT + Metadata::size(Version::V1) - 1024;

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer.inner().write_ptr.store(0, Ordering::Release);
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::Acquire);
        ring_buffer
            .inner()
            .read_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::Acquire);
        ring_buffer.inner().has_data.store(true, Ordering::Release);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = "hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let data = Data::new(Version::V1, data.as_bytes());

        file.seek(SeekFrom::Start(METADATA_SPOT as u64))
            .expect("seek to start");
        file.write_all(bincode::serialize(&meta).unwrap().as_slice())
            .expect("write metadata");

        file.seek(SeekFrom::Start(DATA_SPOT as u64))
            .expect("seek to start");
        file.write_all(bincode::serialize(&data).unwrap().as_slice())
            .expect("write data");

        let mut data = vec![0u8; 11];
        let _ = ring_buffer.pop(&mut data).unwrap();
        assert_matches!(data.as_slice(), b"hello world");
    }

    #[test]
    fn test_pop_wrap_around_data_partial() {
        const METADATA_SPOT: u32 = 1024 - Metadata::size(Version::V1) - 5;
        const DATA_SPOT1: u32 = METADATA_SPOT + Metadata::size(Version::V1);
        const DATA_SPOT2: u32 = (DATA_SPOT1 + 5) % 1024;

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer.inner().write_ptr.store(0, Ordering::Release);
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::Acquire);
        ring_buffer
            .inner()
            .read_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::Acquire);
        ring_buffer.inner().has_data.store(true, Ordering::Release);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = "hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let data = Data::new(Version::V1, data.as_bytes());

        file.seek(SeekFrom::Start(METADATA_SPOT as u64))
            .expect("seek to start");
        file.write_all(bincode::serialize(&meta).unwrap().as_slice())
            .expect("write metadata");

        file.seek(SeekFrom::Start(DATA_SPOT1 as u64))
            .expect("seek to start");
        let data = bincode::serialize(&data).unwrap();
        let data = data.as_slice();
        file.write_all(&data[..(1024 - DATA_SPOT1) as usize])
            .expect("write data");

        file.seek(SeekFrom::Start(DATA_SPOT2 as u64))
            .expect("seek to start");
        file.write_all(&data[(1024 - DATA_SPOT1) as usize..])
            .expect("write data");

        let mut data = vec![0u8; 11];
        let _ = ring_buffer.pop(&mut data).unwrap();
        assert_matches!(data.as_slice(), b"hello world");
    }

    #[test]
    fn test_pop_wrap_around_metadata() {
        const METADATA_SPOT: u32 = 1024 - (Metadata::size(Version::V1) / 2);

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer.inner().write_ptr.store(0, Ordering::Release);
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::Acquire);
        ring_buffer
            .inner()
            .read_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::Acquire);
        ring_buffer.inner().has_data.store(true, Ordering::Release);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = "hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let data = Data::new(Version::V1, data.as_bytes());

        let mut buf = vec![0u8; 1024];
        meta.encode(&mut buf[..Metadata::size(Version::V1) as usize])
            .unwrap();
        data.encode(&mut buf[Metadata::size(Version::V1) as usize..])
            .unwrap();
        file.write_all(&buf).expect("write");

        let mut data = vec![0u8; 11];
        let _ = ring_buffer.pop(&mut data).unwrap();
        assert_matches!(data.as_slice(), b"hello world");
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
            .fetch_add(50, Ordering::Release);
        ring_buffer.inner().has_data.store(true, Ordering::Release);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let data = "hello world";
        let meta = Metadata::new(Version::V1, 1, 1, 1, data.len() as u32);
        let data = Data::new(Version::V1, data.as_bytes());

        let mut buf = vec![0u8; 1024];
        meta.encode(&mut buf[..Metadata::size(Version::V1) as usize])
            .unwrap();
        data.encode(&mut buf[Metadata::size(Version::V1) as usize..])
            .unwrap();
        file.write_all(&buf).expect("write");

        let mut data = vec![0u8; 11];
        let _ = ring_buffer.pop(&mut data).unwrap();
        assert_matches!(data.as_slice(), b"hello world");
    }

    #[test]
    fn test_push_entry_larger_than_buffer() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file, 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let data = vec![0u8; 1024_usize + 1];
        assert_matches!(
            ring_buffer.push(&data),
            Err(Error::EntryLargerThanBuffer(_, _))
        );
    }

    #[test]
    fn test_push_entry_too_big_due_to_metadata_wrap() {
        const METADATA_SPOT: u32 = 1024 - (Metadata::size(Version::V1) / 2);

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file, 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::Acquire);

        let data = vec![0u8; 40];
        assert_matches!(ring_buffer.push(&data), Err(Error::EntryTooBig(_, _)));
    }

    #[test]
    fn test_push_entry_too_big_due_for_remaining_space() {
        const METADATA_SPOT: u32 = 1024 - (Metadata::size(Version::V1) / 2);
        const READ_WRAP: u32 = 10;

        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file, 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");
        ring_buffer
            .inner()
            .write_ptr
            .fetch_add(METADATA_SPOT as u64, Ordering::Acquire);
        ring_buffer
            .inner()
            .read_ptr
            .fetch_add(READ_WRAP as u64, Ordering::Acquire);

        let data = vec![0u8; 40];
        assert_matches!(ring_buffer.push(&data), Err(Error::EntryTooBig(_, _)));
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
            .store(METADATA_SPOT as u64, Ordering::Release);
        ring_buffer
            .inner()
            .read_ptr
            .store(READ_WRAP as u64, Ordering::Release);
        ring_buffer.inner().has_data.store(true, Ordering::Release);

        let data = "hello world 19".as_bytes();
        assert_matches!(ring_buffer.push(data), Err(Error::EntryTooBig(_, _)));
    }

    #[test]
    fn test_push_no_wrap_around() {
        let file = tempfile::tempdir().unwrap();
        let file = file.path().join("test");
        let buffer = MmapBuffer::new(file.clone(), 1024).expect("buffer");
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let data = "abcdefghijklmnopqrstuvwxyz".as_bytes().to_vec();
        ring_buffer.push(&data).unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file)
            .expect("open file");

        let mut data = vec![0u8; 256]; // Large enough buf to be sure we got everything
        let _ = file.read(&mut data).unwrap();

        let Metadata::Version1(meta) =
            bincode::deserialize::<Metadata>(&data[..Metadata::size(Version::V1) as usize])
                .unwrap();
        meta.verify().unwrap();
        assert_eq!(meta.read_ptr, 0);
        assert_eq!(meta.write_ptr, 86);
        assert_eq!(meta.entry, 1);
        assert_eq!(meta.size, 26);

        let start = Metadata::size(Version::V1) as usize;
        let end = Metadata::Version1(meta).data_size() as usize + start;
        let Data::Version1(data) = bincode::deserialize::<Data>(&data[start..end]).unwrap();
        data.verify().unwrap();
        assert_eq!(data.data, b"abcdefghijklmnopqrstuvwxyz");
    }

    #[test]
    fn thread_safety_test() {
        let buffer = InMemBuffer::new(1024);
        let ring_buffer = RingBuffer::new(buffer, Version::V1).expect("new buffer");

        let reader = {
            let ring_buffer = ring_buffer.clone();
            spawn(move || {
                let mut reads = vec![];
                loop {
                    let mut data = vec![0u8; 20];
                    match ring_buffer.pop(&mut data) {
                        Ok(bytes) => {
                            data.truncate(bytes);
                            let s = unsafe { String::from_utf8_unchecked(data) };
                            let split: Vec<_> = s.split(' ').collect();
                            let num = split[2].parse::<u32>().unwrap();
                            let s = split[0].to_string() + " " + split[1];
                            reads.push((num, s));
                            if reads.len() == 100 {
                                break;
                            }
                        }
                        Err(Error::BufferEmpty) => {
                            sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(err) => {
                            panic!("unexpected error: {:?}", err);
                        }
                    }
                }
                reads.sort();
                for (i, (num, msg)) in reads.into_iter().enumerate() {
                    assert_eq!(msg, "hello world".to_string());
                    assert_eq!(i as u32, num);
                }
            })
        };

        let writer = spawn(move || {
            for i in 0..100 {
                let data = format!("hello world {}", i);

                loop {
                    match ring_buffer.push(data.as_bytes()) {
                        Ok(_) => {
                            break;
                        }
                        Err(Error::EntryTooBig(_, _)) => {
                            sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(err) => {
                            panic!("unexpected error: {:?}", err);
                        }
                    }
                }
            }
        });

        reader.join().unwrap();
        writer.join().unwrap();
    }
}
