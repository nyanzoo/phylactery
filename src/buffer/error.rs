#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),

    #[error("out of bounds {offset} + {len} > {capacity}")]
    OutOfBounds {
        offset: usize,
        len: usize,
        capacity: u64,
    },
}
