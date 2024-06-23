#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("data crc mismatch: {expected} != {actual}")]
    DataCrcMismatch { expected: u32, actual: u32 },

    #[error("metadata crc mismatch: {expected} != {actual}")]
    MetadataCrcMismatch { expected: u32, actual: u32 },
}
