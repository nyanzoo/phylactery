use std::{
    io::{Read, Write},
    mem::size_of,
};

use necronomicon::{Decode, Encode};

use crate::{buffer::Buffer, Error};

pub mod v1;

const VERSION_SIZE: usize = size_of::<u8>();

/// The version for encoding and decoding metadata and data.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum Version {
    V1,
}

impl From<Version> for u8 {
    fn from(val: Version) -> Self {
        match val {
            Version::V1 => 1,
        }
    }
}

impl TryFrom<u8> for Version {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            _ => Err(Error::InvalidVersion(value)),
        }
    }
}

impl<W> Encode<W> for Version
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            Self::V1 => 1u8.encode(writer),
        }
    }
}

impl<R> Decode<R> for Version
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        match u8::decode(reader)? {
            1 => Ok(Self::V1),
            _ => Err(necronomicon::Error::Decode(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid version",
            ))),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
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

    pub const fn struct_size(version: Version) -> u32 {
        match version {
            Version::V1 => VERSION_SIZE as u32 + v1::Metadata::struct_size(),
        }
    }

    pub fn read_ptr(&self) -> u64 {
        match self {
            Self::Version1(metadata) => metadata.read_ptr(),
        }
    }

    pub fn write_ptr(&self) -> u64 {
        match self {
            Self::Version1(metadata) => metadata.write_ptr(),
        }
    }

    pub fn entry(&self) -> u64 {
        match self {
            Self::Version1(metadata) => metadata.entry(),
        }
    }

    pub fn calculate_data_size(version: Version, size: u32) -> u32 {
        let inner = match version {
            Version::V1 => v1::Metadata::calculate_data_size(size),
        };
        inner + VERSION_SIZE as u32
    }

    pub fn data_size(&self) -> u32 {
        match self {
            // 2 for len of data + 4 for crc + size of version + data size
            Self::Version1(metadata) => Self::calculate_data_size(Version::V1, metadata.size()),
        }
    }

    pub fn real_data_size(&self) -> u32 {
        match self {
            Self::Version1(metadata) => metadata.size(),
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

impl<R> Decode<R> for Metadata
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let version = Version::decode(reader)?;
        match version {
            Version::V1 => Ok(Self::Version1(v1::Metadata::decode(reader)?)),
        }
    }
}

impl<W> Encode<W> for Metadata
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            Self::Version1(metadata) => {
                Version::V1.encode(writer)?;
                metadata.encode(writer)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum Data<'a> {
    Version1(v1::Data<'a>),
}

impl<'a> Data<'a> {
    pub fn new(version: Version, data: &'a [u8]) -> Self {
        match version {
            Version::V1 => Self::Version1(v1::Data::write(data)),
        }
    }

    pub fn copy_into(self, buf: &mut [u8]) {
        match self {
            Self::Version1(data) => data.copy_into(buf),
        }
    }

    pub fn into_inner(self) -> Vec<u8> {
        match self {
            Self::Version1(data) => match data {
                v1::Data::Read(data) => data.data,
                v1::Data::Write(_) => panic!("cannot get inner data from write data"),
            },
        }
    }

    pub fn struct_size(&self) -> u32 {
        match self {
            Self::Version1(data) => VERSION_SIZE as u32 + data.struct_size(),
        }
    }

    pub fn split_at(&self, idx: usize) -> (&[u8], &[u8]) {
        match self {
            Self::Version1(data) => match data {
                v1::Data::Read(data) => data.data.split_at(idx),
                v1::Data::Write(_) => panic!("cannot split write data"),
            },
        }
    }

    pub fn verify(&self) -> Result<(), Error> {
        match self {
            Self::Version1(data) => data.verify(),
        }
    }

    pub fn crc(&self) -> u32 {
        match self {
            Self::Version1(data) => data.crc(),
        }
    }
}

impl<R> Decode<R> for Data<'_>
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error> {
        let version = Version::decode(reader)?;
        match version {
            Version::V1 => Ok(Self::Version1(v1::Data::decode(reader)?)),
        }
    }
}

impl<W> Encode<W> for Data<'_>
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            Self::Version1(data) => {
                Version::V1.encode(writer)?;
                data.encode(writer)?;
            }
        }
        Ok(())
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
        let mut header = [0u8; 5];
        buffer.read_at(&mut header, off)?;
        // TODO: should be little endian?
        // BAD5EED5
        match header {
            [_, 0xba, 0xd5, 0xee, 0xd5] => break true,
            [_, _, 0xba, 0xd5, 0xee] => off += 2,
            [_, _, _, 0xba, 0xd5] => off += 3,
            [_, _, _, _, 0xba] => off += 4,
            _ => off += 5,
        }

        if off as u64 + 5 >= buffer.capacity() {
            break false;
        }
    };

    if has_data {
        // rollback 5 bytes to get start of entry.
        // off -= 5;

        let mut metas = vec![];
        while (off as u64 + Metadata::struct_size(version) as u64) < buffer.capacity() {
            // read the metadata.
            if let Ok(metadata) =
                buffer.decode_at::<Metadata>(off, Metadata::struct_size(version) as usize)
            {
                metas.push(metadata);

                // increment the offset by the size of the metadata.
                off += Metadata::struct_size(version) as usize;
                // increment the offset by the size of the data.
                off += metadata.data_size() as usize;
            } else {
                break;
            }
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

    use std::io::Cursor;

    use coverage_helper::test;
    use necronomicon::{Decode, Encode};

    use super::{v1, Data, Metadata, Version};

    #[test]
    fn test_metadata_size() {
        // 1 bytes for the enum variant
        // rest from actual struct
        assert_eq!(Metadata::struct_size(Version::V1), 37);
    }

    #[test]
    fn test_data_write() {
        // create a Data instance to write
        let data = Data::Version1(v1::Data::Write(v1::DataWrite {
            data: "kittens".as_bytes(),
            crc: 2940700499,
        }));

        // create a buffer to write the data to
        let mut buf = vec![];

        // write the data to the buffer
        let result = data.encode(&mut buf);

        // ensure that the write operation succeeded
        assert!(result.is_ok());

        // verify that if deserialized, the data is the same
        let result = Data::decode(&mut Cursor::new(&mut buf));
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(data.crc(), deserialized.crc());
        assert!(deserialized.verify().is_ok());
    }
}
