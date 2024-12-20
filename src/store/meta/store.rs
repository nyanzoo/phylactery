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

use super::Config;

const REPLICA_COUNT: usize = 10;

#[derive(Copy, Clone, Debug, Hash, PartialEq)]
struct VNode {
    shard: usize,
    id: usize,
}

/// The metadata store. This manages all metadata pertaining to actual data entries.
pub struct Store {
    /// The metadata pool for allocating metadata entries in memory.
    pool: PoolImpl,
    /// A sharded set of files for storing metadata entries.
    shards: Vec<Shard>,
    /// hasher for calculating shard index
    hasher: HashRing<VNode>,
}

impl Store {
    /// Create a new metadata store.
    pub fn new(dir: String, pool: PoolImpl, shards: usize, config: Config) -> Result<Self, Error> {
        let mut shards_v = vec![];
        let mut hasher = HashRing::new();
        for shard in 0..shards {
            for id in 0..REPLICA_COUNT {
                hasher.add(VNode { shard, id });
            }
            let shard = Shard::new(dir.clone(), shard, config.size.to_bytes(), &pool)?;
            shards_v.push(shard);
        }
        Ok(Self {
            pool,
            shards: shards_v,
            hasher,
        })
    }

    pub fn compact(&mut self, shards: impl Iterator<Item = usize>) -> Result<(), Error> {
        for shard in shards {
            let shard = &mut self.shards[shard];
            shard.compact().map_err(Error::Shard)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: BinaryData<SharedImpl>) -> Result<Option<Delete>, Error> {
        let shard = self.hasher.get(&key).copied().expect("no shards").shard;
        let shard = &mut self.shards[shard];
        shard.delete(key, &self.pool).map_err(Error::Shard)
    }

    pub fn get(&mut self, key: BinaryData<SharedImpl>) -> Result<Option<Get>, Error> {
        let shard = self.hasher.get(&key).copied().expect("no shards").shard;
        let shard = &mut self.shards[shard];
        shard.get(key, &self.pool).map_err(Error::Shard)
    }

    pub fn prepare_put(&mut self, key: BinaryData<SharedImpl>) -> Result<PreparePut, Error> {
        let shard = self.hasher.get(&key).copied().expect("no shards").shard;
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
        let shard = self.hasher.get(&key).copied().expect("no shards").shard;
        let shard = &mut self.shards[shard];
        shard.put(prepare, file, offset, len).map_err(Error::Shard)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::LazyLock;

    use super::*;

    use necronomicon::SharedImpl;
    use tempfile::tempdir;

    const BLOCK_SIZE: usize = 0x1000;
    const POOL_SIZE: usize = 0x1000;
    const SHARDS: usize = 100;
    const SHARD_LEN: u32 = 0x1000;
    static CONFIG: LazyLock<Config> = LazyLock::new(|| Config::test(SHARD_LEN));

    #[test]
    fn store_put_get_delete() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf().to_str().unwrap().to_string();
        let mut store = Store::new(
            dir_path,
            PoolImpl::new(BLOCK_SIZE, POOL_SIZE),
            SHARDS,
            CONFIG.clone(),
        )
        .unwrap();
        let key = BinaryData::new(SharedImpl::test_new(b"kittens"));

        let prepare = store.prepare_put(key.clone()).unwrap();

        let mut put = store.put(key.clone(), prepare, 0, 0, 10).unwrap();
        let location = put.lookup();
        put.prepare().unwrap();
        put.commit().unwrap();

        let Get { lookup, .. } = store.get(key.clone()).unwrap().unwrap();
        assert_eq!(lookup, location);

        let delete = store.delete(key.clone()).unwrap().unwrap();
        let del_location = delete.lookup();
        delete.commit().unwrap();
        assert_eq!(location, del_location);

        assert!(store.get(key).unwrap().is_none());
    }

    #[test]
    fn store_put() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf().to_str().unwrap().to_string();
        let mut store = Store::new(
            dir_path,
            PoolImpl::new(BLOCK_SIZE, POOL_SIZE),
            SHARDS,
            Config::test(SHARD_LEN.clone() * 0x1000),
        )
        .unwrap();

        let mut commits = vec![];
        let now = std::time::Instant::now();
        for i in 0..100_000 {
            let key = BinaryData::new(SharedImpl::test_new(format!("key-{}", i).as_bytes()));
            let prepare = store.prepare_put(key.clone()).unwrap();
            let put = store.put(key.clone(), prepare, 0, 0, 10).unwrap();
            commits.push(put);
        }
        let elapsed = now.elapsed();
        println!("100,000 put: {:?}", elapsed);

        let now = std::time::Instant::now();
        for mut put in commits {
            put.prepare().unwrap();
            put.commit().unwrap();
        }
        let elapsed = now.elapsed();
        println!("100,000 commit: {:?}", elapsed);
    }
}
