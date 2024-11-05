use std::{
    fmt::{self, Debug, Formatter},
    ops::Range,
};

use necronomicon::{BufferOwner, Owned, Pool, Shared};

use crate::{
    buffer::{Buffer, FileBuffer},
    entry::{Metadata, Readable, Version, Writable},
};

use super::{Error, Location};

pub(super) type Flush = crate::buffer::Flush<<FileBuffer as Buffer>::Flushable>;

#[derive(Debug, Eq, PartialEq)]
pub struct Remaining {
    pub space_to_write: u64,
    pub space_to_read: u64,
}

#[derive(Debug)]
pub struct Entry<S>
where
    S: Shared,
{
    pub data: Readable<S>,
    pub size: usize,
}

pub(crate) struct Peek<P, O>
where
    P: Pool,
    O: BufferOwner,
{
    read: usize,
    write: usize,
    buffer: FileBuffer,
    version: Version,
    pool: P,
    owner: O,
}

impl<'a, P, O> Iterator for Peek<P, O>
where
    P: Pool,
    O: BufferOwner,
{
    type Item = Result<Entry<<P::Buffer as Owned>::Shared>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.read >= self.write {
            return None;
        }

        let mut buf = self.pool.acquire("peek itr", self.owner);
        let entry = read_entry(self.buffer.clone(), self.read, self.version, &mut buf);

        if let Ok(Entry { size, .. }) = entry {
            self.read += size;
        }

        Some(entry)
    }
}

#[derive(Debug)]
pub enum Pop<S>
where
    S: Shared,
{
    Entry(Entry<S>),
    WaitForFlush,
}

#[derive(Debug)]
pub enum Push {
    Entry {
        file: u64,
        offset: u64,
        len: u64,
        crc: u32,
        flush: Flush,
    },
    Full,
}

#[derive(Clone)]
pub struct File {
    pub(super) buffer: FileBuffer,
    pub(super) read: usize,
    pub(super) write: usize,
    pub(super) location: Location,
    pub(super) version: Version,
}

impl Debug for File {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Open")
            .field("read", &self.read)
            .field("write", &self.write)
            .field("location", &self.location)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl File {
    pub(crate) fn peek<P, O>(&self, pool: P, owner: O) -> Peek<P, O>
    where
        P: Pool,
        O: BufferOwner,
    {
        Peek {
            read: self.read,
            write: self.write,
            buffer: self.buffer.clone(),
            version: self.version,
            pool,
            owner,
        }
    }

    pub(crate) fn pop<O>(&mut self, buf: &mut O) -> Result<Pop<O::Shared>, Error>
    where
        O: Owned,
    {
        if self.read >= self.write {
            return Ok(Pop::WaitForFlush);
        }

        let entry = read_entry(self.buffer.clone(), self.read, self.version, buf)?;

        self.read += entry.size;

        Ok(Pop::Entry(entry))
    }

    pub(crate) fn push(&mut self, buf: &[u8]) -> Result<Push, Error> {
        let mut write_ptr = self.write as u64;
        let orig_write_ptr = write_ptr;
        let read_ptr = self.read as u64;

        let data_size = buf.len() as u32;
        let data = Writable::new(self.version, buf);
        let entry_size = Metadata::struct_size(self.version) as u64
            + Metadata::calculate_data_size(self.version, data_size) as u64;
        let metadata = Metadata::new(self.version, read_ptr, write_ptr + entry_size, data_size);
        let capacity = self.buffer.capacity();

        // If the entry is too big, we can't write.
        if entry_size > capacity {
            return Err(Error::EntryLargerThanNode(entry_size, capacity));
        }

        if write_ptr + entry_size > capacity {
            return Ok(Push::Full);
        }

        let struct_size = Metadata::struct_size(self.version);
        // We need the original ptr for returning where the data is stored.
        let offset = write_ptr;
        // write the metadata.
        // ignore the flush result, we'll handle it later with the data flush.
        _ = self
            .buffer
            .encode_at(write_ptr as usize, struct_size, &metadata)?;

        write_ptr += struct_size as u64;
        // write the data.
        let flush =
            self.buffer
                .encode_at(write_ptr as usize, metadata.data_size() as usize, &data)?;

        write_ptr += Metadata::calculate_data_size(self.version, data_size) as u64;
        // update the write pointer.
        let len = write_ptr - orig_write_ptr;
        self.write = write_ptr as usize;

        Ok(Push::Entry {
            file: self.location.file,
            offset,
            len,
            crc: data.crc(),
            flush,
        })
    }

    pub(crate) fn compact(&mut self, ranges_to_delete: &[Range<usize>]) -> Result<Flush, Error> {
        self.buffer.compact(ranges_to_delete).map_err(Error::Buffer)
    }

    pub(crate) fn location(&self) -> &Location {
        &self.location
    }

    pub(crate) fn remaining(&self) -> Remaining {
        Remaining {
            space_to_write: self.buffer.capacity() - (self.write - self.read) as u64,
            space_to_read: (self.write - self.read) as u64,
        }
    }
}

fn read_entry<O>(
    buffer: FileBuffer,
    read: usize,
    version: Version,
    buf: &mut O,
) -> Result<Entry<O::Shared>, Error>
where
    O: Owned,
{
    let metadata_len = Metadata::struct_size(version);
    let metadata: Metadata = buffer.decode_at(read, metadata_len)?;
    metadata.verify()?;

    let start = read + metadata_len;
    let data_len = metadata.data_size() as usize;
    let data: Readable<O::Shared> = buffer.decode_at_owned(start, data_len, buf)?;
    data.verify()?;

    Ok(Entry {
        data,
        size: metadata_len + data_len,
    })
}

#[cfg(test)]
mod test {
    use std::rc::Rc;

    use necronomicon::{Pool, PoolImpl};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_peek_read_write() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let file_path = dir_path.join("test");
        let buffer = FileBuffer::new(1024, file_path).unwrap();

        let mut file = File {
            buffer,
            read: 0,
            write: 0,
            location: Location {
                file: 0,
                dir: Rc::new(dir_path),
            },
            version: Version::V1,
        };

        {
            assert_eq!(
                file.remaining(),
                Remaining {
                    space_to_write: 1024,
                    space_to_read: 0
                }
            );
            file.push(b"hello").unwrap();
            assert_eq!(
                file.remaining(),
                Remaining {
                    space_to_write: 977,
                    space_to_read: 47
                }
            );
            file.push(b"world").unwrap();
            assert_eq!(
                file.remaining(),
                Remaining {
                    space_to_write: 930,
                    space_to_read: 94
                }
            );
            file.push(b"kitties").unwrap();
            assert_eq!(
                file.remaining(),
                Remaining {
                    space_to_write: 881,
                    space_to_read: 143
                }
            );
        }

        {
            let pool = PoolImpl::new(1024, 1024);
            let mut peek = file.peek(pool, "test");

            assert_eq!(
                peek.next()
                    .unwrap()
                    .unwrap()
                    .data
                    .into_inner()
                    .data()
                    .as_slice(),
                b"hello"
            );

            assert_eq!(
                peek.next()
                    .unwrap()
                    .unwrap()
                    .data
                    .into_inner()
                    .data()
                    .as_slice(),
                b"world"
            );

            assert_eq!(
                peek.next()
                    .unwrap()
                    .unwrap()
                    .data
                    .into_inner()
                    .data()
                    .as_slice(),
                b"kitties"
            );

            assert!(peek.next().is_none());
            assert_eq!(
                file.remaining(),
                Remaining {
                    space_to_write: 881,
                    space_to_read: 143
                }
            );
        }

        {
            let pool = PoolImpl::new(1024, 1024);
            let mut owned = pool.acquire("cat", "test");
            let Pop::Entry(pop) = file.pop(&mut owned).unwrap() else {
                panic!("expected entry");
            };
            assert_eq!(pop.data.into_inner().data().as_slice(), b"hello");
            assert_eq!(
                file.remaining(),
                Remaining {
                    space_to_write: 928,
                    space_to_read: 96
                }
            );
        }

        {
            let pool = PoolImpl::new(1024, 1024);
            let mut peek = file.peek(pool, "test");

            assert_eq!(
                peek.next()
                    .unwrap()
                    .unwrap()
                    .data
                    .into_inner()
                    .data()
                    .as_slice(),
                b"world"
            );

            assert_eq!(
                peek.next()
                    .unwrap()
                    .unwrap()
                    .data
                    .into_inner()
                    .data()
                    .as_slice(),
                b"kitties"
            );

            assert!(peek.next().is_none());
        }

        {
            let pool = PoolImpl::new(1024, 1024);
            let mut owned = pool.acquire("cat", "test");
            let Pop::Entry(pop) = file.pop(&mut owned).unwrap() else {
                panic!("expected entry");
            };
            assert_eq!(pop.data.into_inner().data().as_slice(), b"world");
            assert_eq!(
                file.remaining(),
                Remaining {
                    space_to_write: 975,
                    space_to_read: 49
                }
            );

            let Pop::Entry(pop) = file.pop(&mut owned).unwrap() else {
                panic!("expected entry");
            };
            assert_eq!(pop.data.into_inner().data().as_slice(), b"kitties");

            let Pop::WaitForFlush = file.pop(&mut owned).unwrap() else {
                panic!("expected wait for flush");
            };
            assert_eq!(
                file.remaining(),
                Remaining {
                    space_to_write: 1024,
                    space_to_read: 0
                }
            );
        }
    }
}
