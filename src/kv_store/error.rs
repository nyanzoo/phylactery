#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("alloc error: {0}")]
    Alloc(#[from] crate::alloc::Error),

    #[error("buffer err: {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("codec error: {0}")]
    Dequeue(#[from] crate::dequeue::Error),

    #[error("empty data")]
    EmptyData,

    #[error("entry error: {0}")]
    Entry(#[from] crate::entry::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),

    #[error("ring buffer error: {0}")]
    RingBuffer(#[from] crate::ring_buffer::Error),
}
