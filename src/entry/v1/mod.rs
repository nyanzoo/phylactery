mod error;
pub(crate) use error::Error;

mod meta;
pub use meta::Metadata;

mod readable;
pub use readable::Readable;

mod writable;
pub use writable::Writable;

pub(crate) fn generate_crc(data: &[u8]) -> u32 {
    let mut crc = crc32fast::Hasher::new();
    crc.update(data);
    crc.finalize()
}
