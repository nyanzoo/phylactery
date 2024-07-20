#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("data error: {0}")]
    Data(#[from] super::data::Error),

    #[error("meta error: {0}")]
    Meta(#[from] super::meta::Error),
}
