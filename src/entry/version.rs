use std::{
    io::{Read, Write},
    mem::size_of,
};

use necronomicon::{Decode, Encode};

use crate::Error;

pub(crate) const VERSION_SIZE: usize = size_of::<u8>();

/// The version for encoding and decoding metadata and data.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
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

impl TryFrom<necronomicon::Version> for Version {
    type Error = Error;

    fn try_from(value: necronomicon::Version) -> Result<Self, Self::Error> {
        Self::try_from(u8::from(value))
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
        let version = u8::decode(reader)?;
        match version {
            1 => Ok(Self::V1),
            _ => Err(necronomicon::Error::Decode(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid version {version}"),
            ))),
        }
    }
}
