use std::{collections::BTreeMap, ops::Range};

use necronomicon::Owned;

use crate::{
    deque::{Location, Push},
    entry::{Readable, Version},
    store::data::Error,
    usize_to_u64,
};

use super::shard::Shard;

pub struct Store {
    /// The path to the data directory.
    dir: String,
    /// A sharded set of files for storing data entries.
    shards: Vec<Shard>,
}

impl Store {
    pub fn new(
        dir: String,
        shards: usize,
        shard_len: u64,
        max_disk_usage: u64,
    ) -> Result<Self, Error> {
        let mut shards_v = vec![];
        for shard in 0..shards {
            let shard = Shard::new(dir.clone(), usize_to_u64(shard), shard_len, max_disk_usage)?;
            shards_v.push(shard);
        }

        Ok(Self {
            dir,
            shards: shards_v,
        })
    }

    pub(super) fn compact(
        &mut self,
        shard: usize,
        ranges_to_delete: BTreeMap<Location, Vec<Range<usize>>>,
    ) -> Result<(), Error> {
        self.shards[shard]
            .compact(ranges_to_delete)
            .map_err(Error::Shard)
    }

    pub(super) fn get<O>(
        &self,
        shard: usize,
        file: u64,
        offset: u64,
        buffer: &mut O,
        version: Version,
    ) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.shards[shard]
            .get(file, offset, buffer, version)
            .map_err(Error::Shard)
    }

    pub(super) fn put(&mut self, shard: usize, value: &[u8]) -> Result<Push, Error> {
        self.shards[shard].put(value).map_err(Error::Shard)
    }
}
