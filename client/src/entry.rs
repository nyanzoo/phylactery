use std::mem::size_of;

use super::error::Error;

pub trait Read<'a> {
    fn read(buf: &'a [u8]) -> Result<Self, Error>
    where
        Self: Sized;
}

pub trait Write {
    fn write(&self, buf: &mut [u8]) -> Result<usize, Error>;
}

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
    pub fn new(version: Version, data_size: u32) -> Self {
        match version {
            Version::V1 => Self::Version1(version1::Metadata::new(data_size)),
        }
    }

    pub const fn size(version: Version) -> u32 {
        match version {
            Version::V1 => size_of::<Version>() as u32 + version1::Metadata::size(),
        }
    }

    pub fn verify(&self) -> Result<(), Error> {
        match self {
            Self::Version1(metadata) => metadata.verify(),
        }
    }

    pub fn data_size(&self) -> u32 {
        match self {
            // 8 for len of data + 4 for crc + size of version + data size
            Self::Version1(metadata) => 8 + metadata.size + size_of::<Version>() as u32 + 4,
        }
    }
}

impl Read<'_> for Metadata {
    fn read(buf: &[u8]) -> Result<Self, Error> {
        Ok(bincode::deserialize(buf)?)
    }
}

impl Write for Metadata {
    fn write(&self, buf: &mut [u8]) -> Result<usize, Error> {
        bincode::serialize_into(buf, self)?;
        Ok({
            match self {
                Self::Version1(_) => version1::Metadata::size(),
            }
        } as usize)
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

    pub const fn size(&self) -> u32 {
        match self {
            Self::Version1(data) => size_of::<Version>() as u32 + data.size(),
        }
    }

    pub fn verify(&self) -> Result<(), Error> {
        match self {
            Self::Version1(data) => data.verify(),
        }
    }
}

impl<'a> Read<'a> for Data<'a> {
    fn read(buf: &'a [u8]) -> Result<Self, Error> {
        Ok(bincode::deserialize(buf)?)
    }
}

impl Write for Data<'_> {
    fn write(&self, buf: &mut [u8]) -> Result<usize, Error> {
        bincode::serialize_into(buf, self)?;
        Ok(self.size() as usize)
    }
}

mod version1 {
    use std::mem::size_of;

    #[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
    #[repr(C)]
    pub struct Metadata {
        // The size of the data.
        pub size: u32,
        // The crc of all the metadata.
        pub crc: u32,
    }

    impl Metadata {
        pub fn new(size: u32) -> Self {
            let crc = Self::generate_crc(size);
            Self { size, crc }
        }

        pub const fn size() -> u32 {
            size_of::<Self>() as u32
        }

        pub fn verify(&self) -> Result<(), super::Error> {
            let crc = Self::generate_crc(self.size);
            if crc != self.crc {
                return Err(super::Error::MetadataCrcMismatch(self.crc, crc));
            }
            Ok(())
        }

        fn generate_crc(size: u32) -> u32 {
            let mut crc = crc32fast::Hasher::new();
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

        pub const fn size(&self) -> u32 {
            8 + self.data.len() as u32 + size_of::<u32>() as u32
        }

        pub fn verify(&self) -> Result<(), super::Error> {
            let crc = Self::generate_crc(self.data);
            if crc != self.crc {
                return Err(super::Error::DataCrcMismatch(self.crc, crc));
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

    use super::{version1, Data, Metadata, Version, Write as _};

    #[test]
    fn test_metadata_size() {
        assert_eq!(Metadata::size(Version::V1), 12);
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
        let result = data.write(&mut buf);

        // ensure that the write operation succeeded
        assert!(result.is_ok());
        let bytes_written = result.unwrap();

        // ensure that the correct number of bytes were written
        assert_eq!(bytes_written, data.size() as usize);

        // verify that if deserialized, the data is the same
        let deserialized: Data = bincode::deserialize(&buf).unwrap();
        assert_eq!(data, deserialized);
    }
}
