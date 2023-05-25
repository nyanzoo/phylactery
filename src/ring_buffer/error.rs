use crate::buffer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bincode err: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("buffer empty")]
    BufferEmpty,

    #[error("buffer error: {0}")]
    BufferError(#[from] buffer::Error),

    #[error("buffer too small: {0} < {1}")]
    BufferTooSmall(u32, u32),

    #[error("codec err: {0}")]
    Codec(#[from] crate::codec::Error),

    #[error("metadata crc mismatch: {expected} != {actual}")]
    MetadataCrcMismatch { expected: u32, actual: u32 },

    #[error("data crc mismatch: {expected} != {actual}")]
    DataCrcMismatch { expected: u32, actual: u32 },

    #[error("empty data")]
    EmptyData,

    #[error("entry too big: {0} > {1}")]
    EntryTooBig(u64, u64),

    #[error("entry larger than total buffer: {0} > {1}")]
    EntryLargerThanBuffer(u64, u64),

    #[error("invalid buffer size: {0} must be >= {1} and a multiple of {1}")]
    InvalidBufferSize(u32, u32),

    #[error("unerlying io err: {0}")]
    IO(#[from] std::io::Error),
}
