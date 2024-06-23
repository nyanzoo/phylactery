use super::Location;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer error: {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("delete on open file: {0:?}")]
    DeleteOnOpenFile(Location),

    #[error("deque full; current: {current}, max: {max}")]
    DequeFull { current: u64, max: u64 },

    #[error("empty data")]
    EmptyData,

    #[error("entry error: {0}")]
    Entry(#[from] crate::entry::Error),

    #[error("entry larger than node {0} > {1}")]
    EntryLargerThanNode(u64, u64),

    #[error("trying to initialize dequeue with smaller capacity ({capacity}) than the current one ({current_capacity})")]
    InvalidDequeCapacity {
        capacity: u64,
        current_capacity: u64,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),
}
