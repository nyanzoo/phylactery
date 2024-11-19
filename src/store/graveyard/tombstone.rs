use std::io::{Read, Write};

use necronomicon::{Decode, Encode};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Tombstone {
    pub crc: u32,
    pub file: u64,
    pub offset: u64,
    pub len: u64,
}

impl<W> Encode<W> for Tombstone
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        let crc: u32 = unsafe { std::ptr::addr_of!(self.crc).read_unaligned() };
        crc.encode(writer)?;

        let file: u64 = unsafe { std::ptr::addr_of!(self.file).read_unaligned() };
        file.encode(writer)?;

        let offset: u64 = unsafe { std::ptr::addr_of!(self.offset).read_unaligned() };
        offset.encode(writer)?;

        let len: u64 = unsafe { std::ptr::addr_of!(self.len).read_unaligned() };
        len.encode(writer)?;

        Ok(())
    }
}

impl<R> Decode<R> for Tombstone
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let crc = u32::decode(reader)?;
        let file = u64::decode(reader)?;
        let offset = u64::decode(reader)?;
        let len = u64::decode(reader)?;
        Ok(Self {
            crc,
            file,
            offset,
            len,
        })
    }
}
