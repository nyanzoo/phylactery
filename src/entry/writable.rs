use std::io::Write;

use necronomicon::Encode;

use super::{v1, version::VERSION_SIZE, Version};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Writable<'a> {
    Version1(v1::Writable<'a>),
}

impl<'a> Writable<'a> {
    pub fn new(version: Version, data: &'a [u8]) -> Self {
        match version {
            Version::V1 => Self::Version1(v1::Writable::new(data)),
        }
    }

    pub fn struct_size(&self) -> u32 {
        match self {
            Self::Version1(data) => VERSION_SIZE as u32 + data.struct_size(),
        }
    }

    pub fn crc(&self) -> u32 {
        match self {
            Self::Version1(data) => data.crc(),
        }
    }
}

impl<'a, W> Encode<W> for Writable<'a>
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
