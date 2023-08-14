use std::{
    io::{Read, Write},
    mem::size_of,
};

use necronomicon::{Decode, Encode};

use super::Error;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct Metadata {
    // The mask for the entry. (not used in crc)
    mask: u32,
    // This is used for scanning efficiently and finding most recent entry.
    entry: u64,
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
        self.entry.encode(writer)?;
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
        let entry = u64::decode(reader)?;
        let read_ptr = u64::decode(reader)?;
        let write_ptr = u64::decode(reader)?;
        let size = u32::decode(reader)?;
        let crc = u32::decode(reader)?;
        Ok(Self {
            mask,
            entry,
            read_ptr,
            write_ptr,
            size,
            crc,
        })
    }
}

impl PartialOrd for Metadata {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.entry.partial_cmp(&other.entry())
    }
}

impl Ord for Metadata {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let other = other.entry();
        self.entry.cmp(&other)
    }
}

impl Metadata {
    pub fn new(entry: u64, read_ptr: u64, write_ptr: u64, size: u32) -> Self {
        let crc = Self::generate_crc(entry, read_ptr, write_ptr, size);
        Self {
            mask: 0xbad5eed5,
            entry,
            read_ptr,
            write_ptr,
            size,
            crc,
        }
    }

    pub const fn mask(&self) -> u32 {
        self.mask
    }

    pub fn entry(&self) -> u64 {
        self.entry
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

    pub fn calculate_data_size(size: u32) -> u32 {
        // 2 for len of data + 4 for crc + data size
        2 + size + 4
    }

    pub const fn struct_size() -> u32 {
        36
    }

    pub fn verify(&self) -> Result<(), Error> {
        let crc = Self::generate_crc(self.entry, self.read_ptr, self.write_ptr, self.size);
        if crc != self.crc {
            return Err(Error::MetadataCrcMismatch {
                expected: self.crc,
                actual: crc,
            });
        }
        Ok(())
    }

    fn generate_crc(entry: u64, read_ptr: u64, write_ptr: u64, size: u32) -> u32 {
        let mut crc = crc32fast::Hasher::new();
        crc.update(&entry.to_be_bytes());
        crc.update(&read_ptr.to_be_bytes());
        crc.update(&write_ptr.to_be_bytes());
        crc.update(&size.to_be_bytes());
        crc.finalize()
    }
}

#[derive(Clone, Debug, Eq)]
#[repr(C)]
pub struct Data {
    // The data of the entry.
    pub data: Vec<u8>,
    // The crc of the data.
    pub crc: u32,
}

impl<W> Encode<W> for Data
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.data.encode(writer)?;
        self.crc.encode(writer)?;
        Ok(())
    }
}

impl<R> Decode<R> for Data
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let data = Vec::decode(reader)?;
        let crc = u32::decode(reader)?;
        Ok(Self { data, crc })
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        self.crc == other.crc
    }
}

impl Data {
    pub fn new(data: Vec<u8>) -> Self {
        let crc = generate_crc(&data);
        Self { data, crc }
    }

    pub fn copy_into(self, buf: &mut [u8]) {
        let len = std::cmp::min(buf.len(), self.data.len());
        buf[..len].copy_from_slice(&self.data[..len]);
    }

    pub fn struct_size(&self) -> u32 {
        2 + self.data.len() as u32 + size_of::<u32>() as u32
    }

    pub fn verify(&self) -> Result<(), Error> {
        let crc = generate_crc(&self.data);
        if crc != self.crc {
            return Err(Error::DataCrcMismatch {
                expected: self.crc,
                actual: crc,
            });
        }
        Ok(())
    }
}

fn generate_crc(data: &[u8]) -> u32 {
    let mut crc = crc32fast::Hasher::new();
    crc.update(data);
    crc.finalize()
}

#[cfg(test)]
mod tests {

    use super::Metadata;

    #[test]
    fn test_metadata_size() {
        assert_eq!(Metadata::struct_size(), 36);
    }
}
