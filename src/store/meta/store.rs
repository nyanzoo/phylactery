use hashring::HashRing;
use necronomicon::{BinaryData, PoolImpl, SharedImpl};

use crate::store::meta::{
    error::Error,
    shard::{
        delete::Delete,
        get::Get,
        put::{PreparePut, Put},
        Shard,
    },
};

/// The metadata store. This manages all metadata pertaining to actual data entries.
pub struct Store {
    /// The path to the metadata directory.
    dir: String,
    /// The metadata pool for allocating metadata entries in memory.
    pool: PoolImpl,
    /// A sharded set of files for storing metadata entries.
    shards: Vec<Shard>,
    /// hasher for calculating shard index
    hasher: HashRing<usize>,
}

impl Store {
    /// Create a new metadata store.
    pub fn new(dir: String, pool: PoolImpl, shards: usize, shard_len: u64) -> Result<Self, Error> {
        let mut shards_v = vec![];
        let mut hasher = HashRing::new();
        for shard in 0..shards {
            hasher.add(shard);
            let shard = Shard::new(dir.clone(), shard, shard_len, &pool)?;
            shards_v.push(shard);
        }
        Ok(Self {
            dir,
            pool,
            shards: shards_v,
            hasher,
        })
    }

    pub fn compact(&mut self, shards: &[usize]) -> Result<(), Error> {
        for shard in shards {
            let shard = &mut self.shards[*shard];
            shard.compact().map_err(Error::Shard)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: BinaryData<SharedImpl>) -> Result<Option<Delete>, Error> {
        let shard = self.hasher.get(&key).copied().expect("no shards");
        let shard = &mut self.shards[shard];
        shard.delete(key, &self.pool).map_err(Error::Shard)
    }

    pub fn get(&mut self, key: BinaryData<SharedImpl>) -> Result<Option<Get>, Error> {
        let shard = self.hasher.get(&key).copied().expect("no shards");
        let shard = &mut self.shards[shard];
        shard.get(key, &self.pool).map_err(Error::Shard)
    }

    pub fn prepare_put(&mut self, key: BinaryData<SharedImpl>) -> Result<PreparePut, Error> {
        let shard = self.hasher.get(&key).copied().expect("no shards");
        let shard = &mut self.shards[shard];
        shard.prepare_put(key).map_err(Error::Shard)
    }

    pub fn put(
        &mut self,
        key: BinaryData<SharedImpl>,
        prepare: PreparePut,
        file: u64,
        offset: u64,
        len: u64,
    ) -> Result<Put, Error> {
        let shard = self.hasher.get(&key).copied().expect("no shards");
        let shard = &mut self.shards[shard];
        shard.put(prepare, file, offset, len).map_err(Error::Shard)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use necronomicon::SharedImpl;
    use tempfile::tempdir;

    const BLOCK_SIZE: usize = 0x1000;
    const POOL_SIZE: usize = 0x1000;
    const SHARDS: usize = 100;
    const SHARD_LEN: u64 = 0x1000;

    #[test]
    fn store_put_get_delete() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf().to_str().unwrap().to_string();
        let mut store = Store::new(
            dir_path,
            PoolImpl::new(BLOCK_SIZE, POOL_SIZE),
            SHARDS,
            SHARD_LEN,
        )
        .unwrap();
        let key = BinaryData::new(SharedImpl::test_new(b"kittens"));

        let prepare = store.prepare_put(key.clone()).unwrap();

        let mut put = store.put(key.clone(), prepare, 0, 0, 10).unwrap();
        let location = put.lookup();
        put.commit().unwrap();

        let Get { lookup, .. } = store.get(key.clone()).unwrap().unwrap();
        assert_eq!(lookup, location);

        let mut delete = store.delete(key.clone()).unwrap().unwrap();
        delete.commit().unwrap();
        assert_eq!(location, delete.lookup());

        assert!(store.get(key).unwrap().is_none());
    }
}
