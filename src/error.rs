#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("alignment error: {0} is not aligned to {1}")]
    Alignment(u64, u64),

    #[error("bad node {0}")]
    BadNode(usize),

    #[error("buffer empty")]
    BufferEmpty,

    #[error("buffer too small: {0} < {1}")]
    BufferTooSmall(u32, u32),

    #[error("data crc mismatch: {expected} != {actual}")]
    DataCrcMismatch { expected: u32, actual: u32 },

    #[error("out of memory {used}/{total}")]
    OutOfMemory { total: u64, used: u64 },

    #[error("empty data")]
    EmptyData,

    #[error("entry larger than total buffer: {entry_size} > {capacity}")]
    EntryLargerThanBuffer { entry_size: u64, capacity: u64 },

    #[error("entry larger than node {0} > {1}")]
    EntryLargerThanNode(u64, u64),

    #[error("entry too big {entry_size} > {remaining}")]
    EntryTooBig { entry_size: u64, remaining: u64 },

    #[error("file does not exist: {0}")]
    FileDoesNotExist(String),

    #[error("invalid buffer size: {0} must be >= {1} and a multiple of {1}")]
    InvalidBufferSize(u32, u32),

    #[error("invalid version: {0}")]
    InvalidVersion(u8),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("metadata crc mismatch: {expected} != {actual}")]
    MetadataCrcMismatch { expected: u32, actual: u32 },

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),

    #[error("node full")]
    NodeFull,

    #[error("not read data")]
    NotReadData,

    #[error("read beyond capacity: {0} + {1} > {2}")]
    ReadBeyondCapacity(u64, u64, u64),

    #[error("write beyond capacity: {0} + {1} > {2}")]
    WriteBeyondCapacity(u64, u64, u64),
}
