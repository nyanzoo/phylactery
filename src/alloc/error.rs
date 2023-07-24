use thiserror;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer error {0}")]
    Buffer(#[from] crate::buffer::Error),

    #[error("codec error {0}")]
    Codec(#[from] crate::codec::Error),

    #[error("out of memory {used}/{total}")]
    OutOfMemory {
        total: u64,
        used: usize,
    },

    #[error("bad node {0}")]
    BadNode(usize),
}
