use thiserror;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer error {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("necronomicon error {0}")]
    Necronomicon(#[from] necronomicon::Error),

    #[error("out of memory {used}/{total}")]
    OutOfMemory { total: u64, used: usize },

    #[error("bad node {0}")]
    BadNode(usize),
}
