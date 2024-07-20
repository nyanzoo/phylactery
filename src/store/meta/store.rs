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

    pub fn compact(&mut self, shards: &[usize]) -> Result<(), Error> {
        for shard in shards {
            let shard = &mut self.shards[*shard];
            shard.compact().map_err(Error::Shard)?;
        }
        Ok(())
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

#[cfg(test)]
mod tests {

    use crate::store::meta::shard::Lookup;

    use super::*;

    use necronomicon::SharedImpl;
    use tempfile::tempdir;

    const SHARD_COUNT: usize = 100;

    #[test]
    fn test_store() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf().to_str().unwrap().to_string();
        let mut store =
            Store::new(dir_path, PoolImpl::new(0x1000, 0x1000), SHARD_COUNT, 0x1000).unwrap();
        let key = BinaryData::new(SharedImpl::test_new(b"kittens"));

        let prepare = store.prepare_put(key.clone()).unwrap();

        let mut put = store.put(key.clone(), prepare, 0, 0, 10).unwrap();
        let location = put.lookup();
        put.commit().unwrap();

        let get = Lookup::from(store.get(key.clone()).unwrap().unwrap());
        assert_eq!(get, location);

        let mut delete = store.delete(key.clone()).unwrap().unwrap();
        delete.commit().unwrap();
        assert_eq!(location, delete.lookup());

        assert_eq!(store.get(key).unwrap(), None);
    }
}
