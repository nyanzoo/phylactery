use crate::codec;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("alignment error: {0} is not aligned to {1}")]
    Alignment(u64, u64),

    #[error("codec error: {0}")]
    Codec(#[from] codec::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
