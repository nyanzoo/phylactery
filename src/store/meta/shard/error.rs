#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer error: {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("full")]
    Full,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),
}
