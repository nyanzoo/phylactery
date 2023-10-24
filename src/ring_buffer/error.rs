use crate::buffer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer empty")]
    BufferEmpty,

    #[error("buffer error: {0}")]
    BufferError(#[from] buffer::Error),

    #[error("buffer too small: {0} < {1}")]
    BufferTooSmall(u32, u32),

    #[error("empty data")]
    EmptyData,

    #[error("entry error: {0}")]
    Entry(#[from] crate::entry::error::Error),

    #[error("entry too big: {entry_size} > {remaining}")]
    EntryTooBig { entry_size: u64, remaining: u64 },

    #[error("entry larger than total buffer: {entry_size} > {capacity}")]
    EntryLargerThanBuffer { entry_size: u64, capacity: u64 },

    #[error("invalid buffer size: {0} must be >= {1} and a multiple of {1}")]
    InvalidBufferSize(u32, u32),

    #[error("unerlying io err: {0}")]
    IO(#[from] std::io::Error),

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),
}
