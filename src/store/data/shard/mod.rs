use std::fs::create_dir;

use necronomicon::PoolImpl;

use crate::{deque::Deque, entry::Version};

#[derive(Copy, Clone, Debug)]
pub(super) struct Tombstone {
    len: usize,
    offset: usize,
}

pub(super) struct Shard {
    dir: String,
    shard: u64,
    deque: Deque,
}

impl Shard {
    pub(super) fn new(
        dir: String,
        shard: u64,
        len: u64,
        max_disk_usage: u64,
        pool: &PoolImpl,
    ) -> Result<Self, Error> {
        let path = format!("{}/{}", dir, shard);
        // TODO: pass in the correct version
        // we need to implement a peek iterator to scan the deque and handle any tombstones
        let mut deque = Deque::new(path, len, max_disk_usage, Version::V1)?;
    }
}
