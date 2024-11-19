use crate::{buffer, deque};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer error: {0}")]
    Buffer(#[from] buffer::Error),

    #[error("deque error: {0}")]
    Deque(#[from] deque::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),
}
