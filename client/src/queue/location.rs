use std::fmt::{Debug, Formatter, Result};

const MASK: u64 = 0x0000_0000_FFFF_FFFF;

fn furl(file_id: u32, offset: u32) -> u64 {
    let file_id = file_id as u64;
    let file_id = file_id << 32;
    let offset = offset as u64;
    file_id | offset
}

fn unfurl(furl: u64) -> (u32, u32) {
    let file_id = (furl >> 32) as u32;
    let offset = (furl & MASK) as u32;
    (file_id, offset)
}

/// [file][index] is the format
/// where file is 4 bytes and index is 4 bytes
#[derive(Clone, Copy, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[repr(C)]
pub struct Location(u64);

impl Debug for Location {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let (file_id, offset) = unfurl(self.0);
        f.debug_struct("Location")
            .field("file", &file_id)
            .field("index", &offset)
            .finish()
    }
}

impl PartialOrd for Location {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.file() < other.file() {
            Some(std::cmp::Ordering::Less)
        } else if self.file() > other.file() {
            Some(std::cmp::Ordering::Greater)
        } else if self.index() < other.index() {
            Some(std::cmp::Ordering::Less)
        } else if self.index() > other.index() {
            Some(std::cmp::Ordering::Greater)
        } else {
            Some(std::cmp::Ordering::Equal)
        }
    }
}

impl Location {
    pub fn new(file: u32, index: u32) -> Self {
        Self(furl(file, index))
    }

    pub fn file(&self) -> u32 {
        // This is safe because we only ever allow 32 bit file ids
        unfurl(self.0).0
    }

    pub fn index(&self) -> u32 {
        // This is safe because we only ever allow 16 bit file indexes
        unfurl(self.0).1
    }

    pub fn to_next_file(&mut self) {
        let (file_id, _) = unfurl(self.0);
        self.0 = furl(file_id + 1, 0);
    }

    pub fn next(&self, data_size: u32, file_size: u32) -> Self {
        let (file_id, offset) = unfurl(self.0);
        let file_id = file_id + ((offset + data_size) / file_size);
        let offset = (offset + data_size) % file_size;
        Self(furl(file_id, offset))
    }
}

impl From<u64> for Location {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Location> for u64 {
    fn from(value: Location) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {

    use super::Location;

    #[test]
    fn test_fai() {
        let location = Location::new(0, 0);
        assert_eq!(location.file(), 0);
        assert_eq!(location.index(), 0);

        let location = location.next(5, 10);
        assert_eq!(location.file(), 0);
        assert_eq!(location.index(), 5);

        let location = location.next(5, 10);
        assert_eq!(location.file(), 1);
        assert_eq!(location.index(), 0);

        let location = location.next(5, 10);
        assert_eq!(location.file(), 1);
        assert_eq!(location.index(), 5);
    }
}
