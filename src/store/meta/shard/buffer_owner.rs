#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BufferOwner {
    Graveyard,
    Delete,
    Get,
    Init,
    Put,
}

impl necronomicon::BufferOwner for BufferOwner {
    fn why(&self) -> &'static str {
        match self {
            Self::Graveyard => "store meta graveyard",
            Self::Delete => "shard delete",
            Self::Get => "shard get",
            Self::Init => "shard init",
            Self::Put => "shard put",
        }
    }
}
