#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer error: {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("data crc mismatch: {expected} != {actual}")]
    DataCrcMismatch { expected: u32, actual: u32 },

    #[error("v1 error: {0}")]
    V1(#[from] crate::entry::v1::Error),
}
