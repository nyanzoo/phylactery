#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bincode err: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("buffer empty")]
    BufferEmpty,

    #[error("buffer full")]
    BufferFull,

    #[error("buffer too small: {0} < {1}")]
    BufferTooSmall(u32, u32),

    #[error("metadata crc mismatch: {0} != {1}")]
    MetadataCrcMismatch(u32, u32),

    #[error("checkpoint crc mismatch: {0} != {1}")]
    CheckpointCrcMismatch(u32, u32),

    #[error("data crc mismatch: {0} != {1}")]
    DataCrcMismatch(u32, u32),

    #[error("entry too big: {0} > {1}")]
    EntryTooBig(usize, usize),

    #[error("entry larger than total buffer: {0} > {1}")]
    EntryLargerThanBuffer(usize, usize),

    #[error("invalid buffer size: {0} must be >= {1} and a multiple of {1}")]
    InvalidBufferSize(usize, usize),

    #[error("unerlying io err: {0}")]
    IO(#[from] std::io::Error),

    #[error("split write failed: {0} > {1}")]
    SplitWriteFailed(u32, u32),

    #[error("GRPC err: {0}")]
    GRPC(#[from] tonic::Status),

    #[error("Queue empty")]
    QueueEmpty,
}
