use std::fmt::{self, Debug, Formatter};

use necronomicon::{BufferOwner, Owned, Pool, Shared};

use crate::{
    buffer::{Buffer as _, FileBuffer, Flush},
    entry::{Metadata, Readable, Version, Writable},
};

use super::{Error, Location};

pub struct Remaining {
    pub space_to_write: u64,
    pub space_to_read: u64,
}

#[derive(Debug)]
pub(crate) struct Entry<S>
where
    S: Shared,
{
    pub data: Readable<S>,
    pub size: usize,
}

pub(crate) struct Peek<'a, P, O>
where
    P: Pool,
    O: BufferOwner,
{
    read: usize,
    write: usize,
    buffer: &'a FileBuffer,
    version: Version,
    pool: P,
    owner: O,
}

impl<'a, P, O> Iterator for Peek<'a, P, O>
where
    P: Pool,
    O: BufferOwner,
{
    type Item = Result<Entry<<P::Buffer as Owned>::Shared>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.read >= self.write {
            return None;
        }

        let mut buf = self.pool.acquire(self.owner);
        let entry = read_entry(self.buffer, self.read, self.version, &mut buf);

        if let Ok(Entry { size, .. }) = entry {
            self.read = size;
        }

        Some(entry)
    }
}

#[derive(Debug)]
pub(crate) enum Pop<S>
where
    S: Shared,
{
    Entry(Entry<S>),
    WaitForFlush,
}

#[derive(Debug)]
pub enum Push<'a> {
    Entry {
        file: u64,
        offset: u64,
        len: u64,
        crc: u32,
        flush: Flush<'a, FileBuffer>,
    },
    Full,
}

pub(crate) struct File {
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
    pub(crate) fn peek<P, O>(&self, pool: P, owner: O) -> Peek<'_, P, O>
    where
        P: Pool,
        O: BufferOwner,
    {
        Peek {
            read: self.read,
            write: self.write,
            buffer: &self.buffer,
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

        let entry = read_entry(&self.buffer, self.read, self.version, buf)?;

        self.read = entry.size;

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

        // We need the original ptr for returning where the data is stored.
        let offset = write_ptr;

        // write the metadata.
        // ignore the flush result, we'll handle it later with the data flush.
        _ = self.buffer.encode_at(
            write_ptr as usize,
            Metadata::struct_size(self.version) as usize,
            &metadata,
        )?;

        write_ptr += Metadata::struct_size(self.version) as u64;

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

    pub(crate) fn location(&self) -> &Location {
        &self.location
    }

    pub(crate) fn remaining(&self) -> Remaining {
        Remaining {
            space_to_write: self.buffer.capacity() as u64 - (self.write - self.read) as u64,
            space_to_read: (self.write - self.read) as u64,
        }
    }
}

fn read_entry<O>(
    buffer: &FileBuffer,
    read: usize,
    version: Version,
    buf: &mut O,
) -> Result<Entry<O::Shared>, Error>
where
    O: Owned,
{
    let metadata_len = Metadata::struct_size(version) as usize;
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
