use std::{
    io::{Read, Write},
    mem::size_of,
};

use necronomicon::{Decode, Encode};

use crate::MASK;

use super::Error;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct Metadata {
    // The mask for the entry. (not used in crc)
    mask: u32,
    // The read ptr when written.
    read_ptr: u64,
    // The write ptr when written.
    write_ptr: u64,
    // The size of the data.
    size: u32,
    // The crc of all the metadata.
    crc: u32,
}

impl<W> Encode<W> for Metadata
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.mask.encode(writer)?;
        self.read_ptr.encode(writer)?;
        self.write_ptr.encode(writer)?;
        self.size.encode(writer)?;
        self.crc.encode(writer)?;
        Ok(())
    }
}

impl<R> Decode<R> for Metadata
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error> {
        let mask = u32::decode(reader)?;
        let read_ptr = u64::decode(reader)?;
        let write_ptr = u64::decode(reader)?;
        let size = u32::decode(reader)?;
        let crc = u32::decode(reader)?;
        Ok(Self {
            mask,
            read_ptr,
            write_ptr,
            size,
            crc,
        })
    }
}

impl Metadata {
    pub fn new(read_ptr: u64, write_ptr: u64, size: u32) -> Self {
        let crc = Self::generate_crc(read_ptr, write_ptr, size);
        Self {
            mask: MASK,
            read_ptr,
            write_ptr,
            size,
            crc,
        }
    }

    pub const fn mask(&self) -> u32 {
        self.mask
    }

    pub fn read_ptr(&self) -> u64 {
        self.read_ptr
    }

    pub fn write_ptr(&self) -> u64 {
        self.write_ptr
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    pub fn crc(&self) -> u32 {
        self.crc
    }

    #[cfg(test)]
    pub fn set_crc(&mut self, crc: u32) {
        self.crc = crc;
    }

    pub const fn calculate_data_size(size: u32) -> u32 {
        // 8 for len of data + 4 for crc + data size
        8 + size + 4
    }

    pub const fn struct_size() -> usize {
        size_of::<u32>() + // mask
            size_of::<u64>() + // read_ptr
            size_of::<u64>() + // write_ptr
            size_of::<u32>() + // size
            size_of::<u32>() // crc
    }

    pub fn verify(&self) -> Result<(), Error> {
        let crc = Self::generate_crc(self.read_ptr, self.write_ptr, self.size);
        if crc != self.crc {
            return Err(Error::MetadataCrcMismatch {
                expected: self.crc,
                actual: crc,
            });
        }
        Ok(())
    }

    fn generate_crc(read_ptr: u64, write_ptr: u64, size: u32) -> u32 {
        let mut crc = crc32fast::Hasher::new();
        crc.update(&read_ptr.to_be_bytes());
        crc.update(&write_ptr.to_be_bytes());
        crc.update(&size.to_be_bytes());
        crc.finalize()
    }
}

#[cfg(test)]
mod tests {

    use super::Metadata;

    #[test]
    fn metadata_size() {
        assert_eq!(Metadata::struct_size(), 28);
    }
}
