use crate::store::meta::shard;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("necronomicon error: {0}")]
    Necronomicon(#[from] necronomicon::Error),

    #[error("shard error: {0}")]
    Shard(#[from] shard::Error),
}
