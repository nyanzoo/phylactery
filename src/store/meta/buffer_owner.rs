#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum BufferOwner {
    Graveyard,
    Delete,
    Get,
    Init,
    Insert,
    ShardInit,
}

impl necronomicon::BufferOwner for BufferOwner {
    fn why(&self) -> &'static str {
        match self {
            Self::Graveyard => "store meta graveyard",
            Self::Delete => "store meta delete",
            Self::Get => "store meta get",
            Self::Init => "store meta init",
            Self::Insert => "store meta insert",
            Self::ShardInit => "store meta shard init",
        }
    }
}
