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

    #[error("dequeue full")]
    DequeueFull,

    #[error("out of memory {used}/{total}")]
    OutOfMemory { total: u64, used: u64 },

    #[error("empty data")]
    EmptyData,

    #[error("entry larger than node {0} > {1}")]
    EntryLargerThanNode(u64, u64),

    #[error("file does not exist: {0}")]
    FileDoesNotExist(String),

    #[error("invalid buffer size: {0} must be >= {1} and a multiple of {1}")]
    InvalidBufferSize(u32, u32),

    #[error("trying to initialize dequeue with smaller capacity ({capacity}) than the current one ({current_capacity})")]
    InvalidDequeueCapacity {
        capacity: u64,
        current_capacity: u64,
    },

    #[error("invalid version: {0}")]
    InvalidVersion(u8),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("key too long: {key:?} > {max_key_length}")]
    KeyTooLong { key: Vec<u8>, max_key_length: usize },

    #[error("metadata crc mismatch: {expected} != {actual}")]
    MetadataCrcMismatch { expected: u32, actual: u32 },

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),

    #[error("node full")]
    NodeFull,

    #[error("not read data")]
    NotReadData,

    #[error("out of bounds {offset} + {len} > {capacity}")]
    OutOfBounds {
        offset: usize,
        len: usize,
        capacity: usize,
    },

    #[error("read beyond capacity: {0} + {1} > {2}")]
    ReadBeyondCapacity(u64, u64, u64),

    #[error("write beyond capacity: {0} + {1} > {2}")]
    WriteBeyondCapacity(u64, u64, u64),
}
