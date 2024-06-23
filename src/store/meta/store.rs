use std::hash::Hash;

use necronomicon::{BinaryData, PoolImpl, Shared, SharedImpl};

use crate::{
    calculate_hash,
    store::meta::{
        error::Error,
        shard::{
            delete::Delete,
            get::Get,
            put::{PreparePut, Put},
            Shard,
        },
    },
    u64_to_usize, usize_to_u64,
};

const SHARD_COUNT: usize = 0x40;

/// The metadata store. This manages all metadata pertaining to actual data entries.
pub struct Store {
    /// The path to the metadata directory.
    dir: String,
    /// The metadata pool for allocating metadata entries in memory.
    pool: PoolImpl,
    /// A sharded set of files for storing metadata entries.
    shards: Vec<Shard>,
}

impl Store {
    /// Create a new metadata store.
    pub fn new(dir: String, pool: PoolImpl, shards: usize, shard_len: u64) -> Result<Self, Error> {
        let mut shards_v = vec![];
        for shard in 0..shards {
            let shard = Shard::new(dir.clone(), usize_to_u64(shard), shard_len, &pool)?;
            shards_v.push(shard);
        }
        Ok(Self {
            dir,
            pool,
            shards: shards_v,
        })
    }

    pub fn delete(&mut self, key: BinaryData<SharedImpl>) -> Result<Option<Delete>, Error> {
        let shard = shard(&key, self.shards.len());
        let shard = &mut self.shards[shard];
        shard.delete(key, &self.pool).map_err(Error::Shard)
    }

    pub fn get(&mut self, key: BinaryData<SharedImpl>) -> Result<Option<Get>, Error> {
        let shard = shard(&key, self.shards.len());
        let shard = &mut self.shards[shard];
        shard.get(key, &self.pool).map_err(Error::Shard)
    }

    pub fn prepare_put(&mut self, key: BinaryData<SharedImpl>) -> Result<PreparePut, Error> {
        let shard = shard(&key, self.shards.len());
        let shard = &mut self.shards[shard];
        shard
            .prepare_put(key.data().as_slice())
            .map_err(Error::Shard)
    }

    pub fn put(
        &mut self,
        key: BinaryData<SharedImpl>,
        prepare: PreparePut,
        file: u64,
        offset: u64,
        len: u64,
    ) -> Result<Put, Error> {
        let shard = shard(&key, self.shards.len());
        let shard = &mut self.shards[shard];
        shard.put(prepare, file, offset, len).map_err(Error::Shard)
    }
}

fn shard<T>(t: &T, shards: usize) -> usize
where
    T: Hash,
{
    u64_to_usize(calculate_hash(t)) % shards
}
