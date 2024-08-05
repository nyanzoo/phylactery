#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::hash::{DefaultHasher, Hash, Hasher};

// mod alloc;
pub mod buffer;
pub mod deque;
pub mod entry;

mod error;
pub use error::Error;

pub mod store;

pub const BAD_MASK: u32 = 0xDEAD_BEA7; // deadbeat
pub const MASK: u32 = 0xBAD5_EED5; // badseeds

pub const fn mask_part(mask: u32, part: u8) -> u8 {
    ((mask >> (part * 8)) & 0xFF) as u8
}

pub const MASK_0: u8 = mask_part(MASK, 0);
pub const MASK_1: u8 = mask_part(MASK, 1);
pub const MASK_2: u8 = mask_part(MASK, 2);
pub const MASK_3: u8 = mask_part(MASK, 3);

pub(crate) fn calculate_hash<T>(t: &T) -> u64
where
    T: Hash,
{
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn usize_to_u64(n: usize) -> u64 {
    u64::try_from(n).expect("should be a valid u64")
}

pub(crate) fn u64_to_usize(n: u64) -> usize {
    usize::try_from(n).expect("should be a valid usize")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_parts() {
        assert_eq!(mask_part(MASK, 0), 0xD5);
        assert_eq!(mask_part(MASK, 1), 0xEE);
        assert_eq!(mask_part(MASK, 2), 0xD5);
        assert_eq!(mask_part(MASK, 3), 0xBA);
    }
}
