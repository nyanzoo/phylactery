use std::mem::size_of;

use super::Error;

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

impl PartialOrd for Metadata {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.entry.partial_cmp(&other.entry)
    }
}

impl Ord for Metadata {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.entry.cmp(&other.entry)
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

#[cfg(test)]
mod tests {

    use super::Metadata;

    #[test]
    fn test_metadata_size() {
        // 8 bytes for the enum variant
        // rest from actual struct
        assert_eq!(Metadata::size(), 40);
    }
}
