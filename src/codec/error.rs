#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("failed to decode {0} due to {1}")]
    Decode(&'static str, &'static str),

    #[error("failed to encode {0} due to {1}")]
    Encode(&'static str, &'static str),
}
