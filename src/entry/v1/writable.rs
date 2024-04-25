use std::{io::Write, mem::size_of};

use necronomicon::Encode;

use super::generate_crc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Writable<'a> {
    // The data of the entry.
    pub(crate) data: &'a [u8],
    // The crc of the data.
    pub(crate) crc: u32,
}

impl<'a, W> Encode<W> for Writable<'a>
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.data.encode(writer)?;
        self.crc.encode(writer)?;
        Ok(())
    }
}

impl<'a> Writable<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let crc = generate_crc(data);
        Self { data, crc }
    }

    pub fn crc(&self) -> u32 {
        self.crc
    }

    pub fn struct_size(&self) -> u32 {
        2 + self.data.len() as u32 + size_of::<u32>() as u32
    }
}
