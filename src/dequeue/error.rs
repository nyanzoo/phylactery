#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer err: {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("data crc mismatch: {expected} != {actual}")]
    DataCrcMismatch { expected: u32, actual: u32 },

    #[error("empty data")]
    EmptyData,

    #[error("entry error: {0}")]
    Entry(#[from] crate::entry::Error),

    #[error("entry too big {0} > {1}")]
    EntryTooBig(u32, u32),

    #[error("entry larger than node {0} > {1}")]
    EntryLargerThanNode(u64, u64),

    #[error("file does not exist: {0}")]
    FileDoesNotExist(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("metadata crc mismatch: {expected} != {actual}")]
    MetadataCrcMismatch { expected: u32, actual: u32 },

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),

    #[error("node full")]
    NodeFull,
}
