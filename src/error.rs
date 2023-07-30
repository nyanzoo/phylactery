#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("alloc error {0}")]
    Alloc(#[from] crate::alloc::Error),

    #[error("buffer error {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("dequeue error {0}")]
    Dequeue(#[from] crate::dequeue::Error),

    #[error("entry error {0}")]
    Entry(#[from] crate::entry::Error),

    #[error("kv store error {0}")]
    KVStore(#[from] crate::kv_store::Error),

    #[error("ring buffer error {0}")]
    RingBuffer(#[from] crate::ring_buffer::Error),
}
