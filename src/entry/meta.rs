use std::io::{Read, Write};

use necronomicon::{Decode, Encode};

use super::{v1, version::VERSION_SIZE, Error, Version};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum Metadata {
    Version1(v1::Metadata),
}

impl Metadata {
    pub fn new(version: Version, read_ptr: u64, write_ptr: u64, data_size: u32) -> Self {
        match version {
            Version::V1 => Self::Version1(v1::Metadata::new(read_ptr, write_ptr, data_size)),
        }
    }

    pub const fn struct_size(version: Version) -> usize {
        match version {
            Version::V1 => VERSION_SIZE + v1::Metadata::struct_size(),
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
            Self::Version1(metadata) => metadata.verify().map_err(Error::V1),
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
