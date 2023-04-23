use std::mem::size_of;

use crate::codec::{self, Decode, Encode};

use super::error::Error;

/// The version for encoding and decoding metadata and data.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum Version {
    V1,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[repr(C)]
pub enum Metadata {
    Version1(version1::Metadata),
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
            Version::V1 => Self::Version1(version1::Metadata::new(
                entry, read_ptr, write_ptr, data_size,
            )),
        }
    }

    pub const fn size(version: Version) -> u32 {
        match version {
            Version::V1 => size_of::<Version>() as u32 + version1::Metadata::size(),
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
            Version::V1 => version1::Metadata::calculate_data_size(size),
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[repr(C)]
pub enum Data<'a> {
    #[serde(borrow)]
    Version1(version1::Data<'a>),
}

impl<'a> Data<'a> {
    pub fn new(version: Version, data: &'a [u8]) -> Self {
        match version {
            Version::V1 => Self::Version1(version1::Data::new(data)),
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

mod version1 {
    use std::mem::size_of;

    use crate::ring_buffer::Error;

    #[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
    #[repr(C)]
    pub struct Metadata {
        // The mask for the entry. (not used in crc)
        pub mask: u32,
        // This is used for scanning efficiently and finding most recent entry.
        pub entry: u64,
        // The read ptr when written.
        pub read_ptr: u64,
        // The write ptr when written.
        pub write_ptr: u64,
        // The size of the data.
        pub size: u32,
        // The crc of all the metadata.
        pub crc: u32,
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

        pub fn calculate_data_size(size: u32) -> u32 {
            // 8 for len of data + 4 for crc + data size
            8 + size + 4
        }

        pub const fn size() -> u32 {
            size_of::<Self>() as u32
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

        pub const fn mask(&self) -> u32 {
            self.mask
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

    #[derive(Copy, Clone, Debug, Eq, serde::Deserialize, serde::Serialize)]
    #[repr(C)]
    pub struct Data<'a> {
        // The data of the entry.
        pub data: &'a [u8],
        // The crc of the data.
        pub crc: u32,
    }

    impl<'a> PartialEq for Data<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.crc == other.crc
        }
    }

    impl<'a> Data<'a> {
        pub fn new(data: &'a [u8]) -> Self {
            let crc = Self::generate_crc(data);
            Self { data, crc }
        }

        pub fn copy_into(self, buf: &mut [u8]) {
            buf[..self.data.len()].copy_from_slice(self.data);
        }

        pub const fn size(&self) -> u32 {
            8 + self.data.len() as u32 + size_of::<u32>() as u32
        }

        pub fn verify(&self) -> Result<(), Error> {
            let crc = Self::generate_crc(self.data);
            if crc != self.crc {
                return Err(Error::DataCrcMismatch {
                    expected: self.crc,
                    actual: crc,
                });
            }
            Ok(())
        }

        fn generate_crc(data: &[u8]) -> u32 {
            let mut crc = crc32fast::Hasher::new();
            crc.update(data);
            crc.finalize()
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::codec::{Decode, Encode};

    use super::{version1, Data, Metadata, Version};

    #[test]
    fn test_metadata_size() {
        // 8 bytes for the enum variant
        // rest from actual struct
        assert_eq!(Metadata::size(Version::V1), 44);
    }

    #[test]
    fn test_data_write() {
        // create a Data instance to write
        let data = Data::Version1(version1::Data {
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
