#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BufferOwner {
    Delete,
    Get,
    Init,
}

impl necronomicon::BufferOwner for BufferOwner {
    fn why(&self) -> &'static str {
        match self {
            Self::Delete => "shard delete",
            Self::Get => "shard get",
            Self::Init => "shard init",
        }
    }
}
