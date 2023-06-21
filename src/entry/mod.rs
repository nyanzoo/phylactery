use std::mem::size_of;

use crate::{
    buffer::Buffer,
    codec::{self, Decode, Encode},
};

pub mod v1;

pub mod error;
pub use error::Error;

/// The version for encoding and decoding metadata and data.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum Version {
    V1,
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
#[repr(C)]
pub enum Metadata {
    Version1(v1::Metadata),
}

impl Metadata {
    pub fn new(
        version: Version,
        entry: u64,
        read_ptr: u64,
        write_ptr: u64,
        data_size: u32,
    ) -> Self {
        match version {
            Version::V1 => Self::Version1(v1::Metadata::new(entry, read_ptr, write_ptr, data_size)),
        }
    }

    pub const fn size(version: Version) -> u32 {
        match version {
            Version::V1 => size_of::<Version>() as u32 + v1::Metadata::size(),
        }
    }

    pub fn read_ptr(&self) -> u64 {
        match self {
            Self::Version1(metadata) => metadata.read_ptr,
        }
    }

    pub fn write_ptr(&self) -> u64 {
        match self {
            Self::Version1(metadata) => metadata.write_ptr,
        }
    }

    pub fn entry(&self) -> u64 {
        match self {
            Self::Version1(metadata) => metadata.entry,
        }
    }

    pub fn calculate_data_size(version: Version, size: u32) -> u32 {
        let inner = match version {
            Version::V1 => v1::Metadata::calculate_data_size(size),
        };
        inner + size_of::<Version>() as u32
    }

    pub fn data_size(&self) -> u32 {
        match self {
            // 8 for len of data + 4 for crc + size of version + data size
            Self::Version1(metadata) => Self::calculate_data_size(Version::V1, metadata.size),
        }
    }

    pub fn real_data_size(&self) -> u32 {
        match self {
            Self::Version1(metadata) => metadata.size,
        }
    }

    pub fn data_encoding_metadata_size(&self) -> u32 {
        self.data_size() - self.real_data_size() - 4 // 4 for crc
    }

    pub fn verify(&self) -> Result<(), Error> {
        match self {
            Self::Version1(metadata) => metadata.verify(),
        }
    }

    pub fn mask(&self) -> u32 {
        match self {
            Self::Version1(metadata) => metadata.mask(),
        }
    }
}

impl Decode<'_> for Metadata {
    fn decode(buf: &[u8]) -> Result<Self, codec::Error> {
        Ok(bincode::deserialize(buf)?)
    }
}

impl Encode for Metadata {
    fn encode(&self, buf: &mut [u8]) -> Result<(), codec::Error> {
        bincode::serialize_into(buf, self)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[repr(C)]
pub enum Data<'a> {
    #[serde(borrow)]
    Version1(v1::Data<'a>),
}

impl<'a> Data<'a> {
    pub fn new(version: Version, data: &'a [u8]) -> Self {
        match version {
            Version::V1 => Self::Version1(v1::Data::new(data)),
        }
    }

    pub fn copy_into(self, buf: &mut [u8]) {
        match self {
            Self::Version1(data) => data.copy_into(buf),
        }
    }

    pub const fn size(&self) -> u32 {
        match self {
            Self::Version1(data) => size_of::<Version>() as u32 + data.size(),
        }
    }

    pub fn split_at(&self, idx: usize) -> (&'a [u8], &'a [u8]) {
        match self {
            Self::Version1(data) => data.data.split_at(idx),
        }
    }

    pub fn verify(&self) -> Result<(), Error> {
        match self {
            Self::Version1(data) => data.verify(),
        }
    }

    pub fn crc(&self) -> u32 {
        match self {
            Self::Version1(data) => data.crc,
        }
    }

    pub fn as_mut(self) -> DataMut<'a> {
        match self {
            Data::Version1(data) => DataMut::Version1(data.as_mut()),
        }
    }
}

impl<'a> Decode<'a> for Data<'a> {
    fn decode(buf: &'a [u8]) -> Result<Self, codec::Error> {
        Ok(bincode::deserialize(buf)?)
    }
}

impl Encode for Data<'_> {
    fn encode(&self, buf: &mut [u8]) -> Result<(), codec::Error> {
        bincode::serialize_into(buf, self)?;
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DataMut<'a> {
    Version1(v1::DataMut<'a>),
}

impl<'a> DataMut<'a> {
    pub fn copy_into(self, buf: &mut [u8]) {
        match self {
            Self::Version1(data) => data.copy_into(buf),
        }
    }

    pub const fn size(&self) -> u32 {
        match self {
            Self::Version1(data) => size_of::<Version>() as u32 + data.size(),
        }
    }

    pub fn verify(&self) -> Result<(), Error> {
        match self {
            Self::Version1(data) => data.verify(),
        }
    }

    pub fn crc(&self) -> u32 {
        match self {
            Self::Version1(data) => data.crc,
        }
    }

    pub fn update(&mut self, update_fn: impl FnOnce(&mut [u8])) {
        match self {
            Self::Version1(inner) => inner.update(update_fn),
        }
    }

    pub fn as_ref(self) -> Data<'a> {
        match self {
            Self::Version1(data) => Data::Version1(data.as_ref()),
        }
    }
}

pub fn crc_check(expected: u32, data: &[u8]) -> Result<(), Error> {
    let mut actual = crc32fast::Hasher::new();
    actual.update(data);
    let actual = actual.finalize();
    if expected != actual {
        Err(Error::DataCrcMismatch { expected, actual })
    } else {
        Ok(())
    }
}

pub fn last_metadata<B>(buffer: &B, version: Version) -> Result<Option<Metadata>, Error>
where
    B: Buffer,
{
    let mut off = 0;
    let has_data = loop {
        let mut header = [0u8; 4];
        buffer.read_at(&mut header, off)?;
        // TODO: should be little endian?
        // D5EED5BA
        match header {
            // [0x0, 0x0, 0x0, 0x0] => break false,
            [0xd5, 0xee, 0xd5, 0xba] => break true,
            [_, 0xd5, 0xee, 0xd5] => off += 1,
            [_, _, 0xd5, 0xee] => off += 2,
            [_, _, _, 0xd5] => off += 3,
            _ => off += 4,
        }

        if off as u64 >= buffer.capacity() {
            break false;
        }
    };

    if has_data {
        // rollback 4 bytes to get start of entry.
        off -= 4;

        let mut metas = vec![];
        while (off as u64 + Metadata::size(version) as u64) < buffer.capacity() {
            // read the metadata.
            let metadata: Metadata = buffer.decode_at(off, Metadata::size(version) as usize)?;

            metas.push(metadata);

            // increment the offset by the size of the metadata.
            off += Metadata::size(version) as usize;
            // increment the offset by the size of the data.
            off += metadata.data_size() as usize;
        }

        // The last entry is the one with the highest entry value.
        metas.sort();
        let metadata = metas.last().expect("ptrs should not be empty");
        return Ok(Some(*metadata));

        // what happens right now is that if we write to disk, and then read from disk, we cannot know
        // what the last read entry was... is that okay? Let's think about this carefully.
    }

    Ok(None)
}

#[cfg(test)]
mod tests {

    use crate::codec::{Decode, Encode};

    use super::{v1, Data, Metadata, Version};

    #[test]
    fn test_metadata_size() {
        // 8 bytes for the enum variant
        // rest from actual struct
        assert_eq!(Metadata::size(Version::V1), 44);
    }

    #[test]
    fn test_data_write() {
        // create a Data instance to write
        let data = Data::Version1(v1::Data {
            data: "kittens".as_bytes(),
            crc: 1234,
        });

        // create a buffer to write the data to
        let mut buf = [0u8; 1024];

        // write the data to the buffer
        let result = data.encode(&mut buf);

        // ensure that the write operation succeeded
        assert!(result.is_ok());

        // verify that if deserialized, the data is the same
        let result = Data::decode(&buf);
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(data, deserialized);
    }
}
