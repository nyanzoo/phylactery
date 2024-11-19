use std::io::Read;

use necronomicon::{BinaryData, Decode, DecodeOwned, Owned, Shared};

use super::{v1, version::VERSION_SIZE, Error, Version};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Readable<S>
where
    S: Shared,
{
    Version1(v1::Readable<S>),
}

impl<S> Readable<S>
where
    S: Shared,
{
    pub fn new(version: Version, data: BinaryData<S>) -> Self {
        match version {
            Version::V1 => Self::Version1(v1::Readable::new(data)),
        }
    }

    pub fn into_inner(self) -> BinaryData<S> {
        match self {
            Readable::Version1(data) => data.data,
        }
    }

    pub fn struct_size(&self) -> u32 {
        match self {
            Self::Version1(data) => VERSION_SIZE as u32 + data.struct_size(),
        }
    }

    pub fn verify(&self) -> Result<(), Error> {
        match self {
            Self::Version1(data) => data.verify().map_err(Error::V1),
        }
    }

    pub fn crc(&self) -> u32 {
        match self {
            Self::Version1(data) => data.crc(),
        }
    }
}

impl<R, O> DecodeOwned<R, O> for Readable<O::Shared>
where
    R: Read,
    O: Owned,
{
    fn decode_owned(reader: &mut R, buffer: &mut O) -> Result<Self, necronomicon::Error> {
        let version = Version::decode(reader)?;
        match version {
            Version::V1 => Ok(Self::Version1(v1::Readable::decode_owned(reader, buffer)?)),
        }
    }
}
