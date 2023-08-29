#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer err: {0}")]
    Buffer(#[from] crate::buffer::Error),
    
    #[error("data crc mismatch: {expected} != {actual}")]
    DataCrcMismatch { expected: u32, actual: u32 },

    #[error("invalid version: {0}")]
    InvalidVersion(u8),

    #[error("metadata crc mismatch: {expected} != {actual}")]
    MetadataCrcMismatch { expected: u32, actual: u32 },

}
