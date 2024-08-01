use necronomicon::{BinaryData, OwnedImpl, PoolImpl, Shared, SharedImpl};

use crate::{
    buffer::{Flush, LazyWriteFileFlush},
    deque::Push,
    entry::{
        v1::{self},
        Readable,
    },
    store::MetaState,
    MASK,
};

use super::{
    cache::LRU,
    data::store::Store as DataStore,
    error::Error,
    graveyard::{graveyard::Graveyard, tombstone::Tombstone},
    meta::{
        self,
        shard::{self, delete::Delete},
        store::Store as MetaDataStore,
    },
};

pub struct Put {
    data_flush: Flush<LazyWriteFileFlush>,
    meta: meta::shard::put::Put,
}

impl Put {
    pub fn commit(mut self) -> Result<(), Error> {
        self.meta
            .prepare()
            .map_err(crate::store::meta::Error::Shard)?;
        self.data_flush.flush()?;
        self.meta
            .commit()
            .map_err(crate::store::meta::Error::Shard)?;
        Ok(())
    }
}

pub struct Store {
    meta: MetaDataStore,
    data: DataStore,
    graveyards: Vec<Graveyard>,
    cache: LRU<BinaryData<SharedImpl>, BinaryData<SharedImpl>>,
}

impl Store {
    pub fn new(
        dir: String,
        shards: usize,
        data_shard_len: u64,
        meta_shard_len: u64,
        max_disk_usage: u64,
        meta_pool: PoolImpl,
    ) -> Result<Self, Error> {
        let meta = MetaDataStore::new(dir.clone(), meta_pool, shards, meta_shard_len)?;
        let data = DataStore::new(dir.clone(), shards, data_shard_len, max_disk_usage)?;
        let mut graveyards = Vec::new();
        for _ in 0..shards {
            graveyards.push(Graveyard::new(dir.clone().into(), max_disk_usage));
        }

        Ok(Self {
            meta,
            data,
            graveyards,
            cache: LRU::new(30_000),
        })
    }

    pub fn delete(&mut self, key: BinaryData<SharedImpl>) -> Result<Option<Delete>, Error> {
        if let Some(delete) = self.meta.delete(key)? {
            let lookup = delete.lookup();
            let metadata = delete.metadata();
            let tomb = Tombstone {
                crc: metadata.crc,
                file: metadata.file,
                offset: metadata.offset,
                len: metadata.len,
            };

            let graveyard = &mut self.graveyards[lookup.shard];
            graveyard.bury(tomb);
            if graveyard.should_compact() {
                let compaction = graveyard.compact();
                self.data
                    .compact(lookup.shard, compaction.file_to_tombs())?;
                self.meta.compact(compaction.shards())?;
            }

            Ok(Some(delete))
        } else {
            Ok(None)
        }
    }

    pub fn get(
        &mut self,
        key: BinaryData<SharedImpl>,
        buffer: &mut OwnedImpl,
    ) -> Result<Option<Readable<SharedImpl>>, Error> {
        if let Some(value) = self.cache.get(&key) {
            let data = Readable::new(crate::entry::Version::V1, value.clone());
            return Ok(Some(data));
        }
        if let Some(shard::get::Get { lookup, metadata }) = self.meta.get(key.clone())? {
            let super::meta::Metadata {
                mask,
                file,
                offset,
                state,
                ..
            } = metadata;
            debug_assert_eq!(mask, MASK);

            if state == MetaState::Compacting {
                return Ok(None);
            }

            let data = self.data.get(
                lookup.shard,
                file,
                offset,
                buffer,
                crate::entry::Version::V1,
            )?;

            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    pub fn put(
        &mut self,
        key: BinaryData<SharedImpl>,
        value: BinaryData<SharedImpl>,
    ) -> Result<Option<Put>, Error> {
        let prepare = self.meta.prepare_put(key.clone())?;
        // println!("prepare");
        let res = match self
            .data
            .put(prepare.lookup.shard, value.data().as_slice())?
        {
            Push::Entry {
                file,
                offset,
                len,
                crc,
                flush,
            } => {
                // println!("entry");
                assert_eq!(crc, v1::generate_crc(value.data().as_slice()));
                let meta_put = self.meta.put(key.clone(), prepare, file, offset, len)?;
                // println!("meta_put");
                Ok(Some(Put {
                    data_flush: flush,
                    meta: meta_put,
                }))
            }
            Push::Full => Ok(None),
        };

        if res.is_ok() {
            self.cache.put(key, value);
        }

        res
    }
}

// pub fn store_driver()

#[cfg(test)]
mod test {
    use necronomicon::Pool;
    use tempfile::tempdir;

    use super::*;

    const SHARDS: usize = 100;
    const DATA_SHARD_LEN: u64 = 0x4000;
    const META_SHARD_LEN: u64 = 0x4000 * 0x1000;
    const MAX_DISK_USAGE: u64 = 0x8000 * 0x1000;
    const BLOCK_SIZE: usize = 0x4000;
    const CAPACITY: usize = 0x1000;

    #[test]
    fn store_put_get() {
        let dir = tempdir().unwrap();
        // let dir_path = dir.path().to_path_buf();
        let dir_path_str = dir.path().to_str().unwrap().to_string();
        let mut store = Store::new(
            dir_path_str,
            SHARDS,
            DATA_SHARD_LEN,
            META_SHARD_LEN,
            MAX_DISK_USAGE,
            PoolImpl::new(BLOCK_SIZE, CAPACITY),
        )
        .unwrap();

        let now = std::time::Instant::now();
        let mut flushes = vec![];
        for i in 0..100_000 {
            let key = BinaryData::new(SharedImpl::test_new(format!("key-{}", i).as_bytes()));
            let value = BinaryData::new(SharedImpl::test_new(format!("value-{}", i).as_bytes()));
            let put = store.put(key, value).unwrap().unwrap();
            flushes.push(put);
        }
        let elapsed = now.elapsed();
        println!("100,000 put: {:?}", elapsed);
        // crate::store::tree(&dir_path);

        let now = std::time::Instant::now();
        for Put {
            data_flush,
            mut meta,
        } in flushes
        {
            meta.prepare().unwrap();
            data_flush.flush().unwrap();
            meta.commit().unwrap();
        }
        let elapsed = now.elapsed();
        println!("100,000 flush: {:?}", elapsed);

        let now = std::time::Instant::now();
        let pool = PoolImpl::new(BLOCK_SIZE, CAPACITY);
        let random_range = rand::seq::index::sample(&mut rand::thread_rng(), 100_000, 100_000);
        for i in random_range {
            let key = BinaryData::new(SharedImpl::test_new(format!("key-{}", i).as_bytes()));
            let mut buffer = pool.acquire("test");
            let data = store.get(key, &mut buffer).unwrap().unwrap();
            assert_eq!(
                data.into_inner().data().as_slice(),
                format!("value-{}", i).as_bytes()
            );
        }
        let elapsed = now.elapsed();
        println!("100,000 get: {:?}", elapsed);
    }
}
