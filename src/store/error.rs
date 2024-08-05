use crate::{buffer, deque};

use super::{data, meta};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer error: {0}")]
    Buffer(#[from] buffer::Error),

    #[error("data error: {0}")]
    Data(#[from] data::Error),

    #[error("deque error: {0}")]
    Deque(#[from] deque::Error),

    #[error("deque exists: {0}")]
    DequeExists(String),

    #[error("deque not found: {0}")]
    DequeNotFound(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("meta error: {0}")]
    Meta(#[from] meta::Error),
}
