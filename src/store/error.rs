#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer error: {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("data error: {0}")]
    Data(#[from] super::data::Error),

    #[error("meta error: {0}")]
    Meta(#[from] super::meta::Error),
}
