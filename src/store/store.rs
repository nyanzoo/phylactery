use std::{collections::BTreeMap, iter::Peekable};

use log::info;
use necronomicon::{BinaryData, ByteStr, OwnedImpl, PoolImpl, Shared, SharedImpl};

use crate::{
    buffer::{Flush, LazyWriteFileFlush},
    deque::{self, Deque, Push},
    entry::{
        v1::{self},
        Readable, Version,
    },
    store::{MetaState, PoolConfig},
    u64_to_usize, MASK,
};

use super::{
    cache::Lru,
    data::store::Store as DataStore,
    error::Error,
    graveyard::{tombstone::Tombstone, Graveyard},
    log::Log,
    meta::{
        self,
        shard::{self, delete::Delete},
        store::Store as MetaDataStore,
    },
    BufferOwner, Config,
};

const LRU_CAPACITY: usize = 10 * 1000;

type DequeIter<'a> = Box<dyn Iterator<Item = Result<Readable<SharedImpl>, Error>> + 'a>;
type DequePeekIter<'a> = Peekable<DequeIter<'a>>;

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

/// TODO: we should limit deque size the same we do stores, that way we can avoid weird counting for TL and
/// for GC.
pub struct Store<'a> {
    dir: String,
    meta: MetaDataStore,
    data: DataStore,
    deques: BTreeMap<String, (Deque, Option<DequePeekIter<'a>>)>,
    graveyards: Vec<Graveyard>,
    cache: Lru<Vec<u8>, Vec<u8>>,
    log: Log,
}

impl<'a> Store<'a> {
    pub fn new(config: &Config) -> Result<Self, Error> {
        let Config {
            dir,
            shards,
            meta_store,
            data_store,
            pool: PoolConfig {
                block_size,
                capacity,
            },
        } = config.clone();

        let meta_pool = PoolImpl::new(u64_to_usize(block_size.to_bytes()), capacity);

        // Each shard has a meta file and a graveyard file. For each shard, we need to
        // calculate the worst case number of data entries that could be in the shard.
        let transaction_log_entries = shards
            + shards
            + (data_store.max_disk_usage.to_bytes() / data_store.node_size.to_bytes()) as usize;

        // Make sure dir exists
        std::fs::create_dir_all(&dir)?;
        let store_dir = format!("{dir}/store");
        std::fs::create_dir_all(&store_dir)?;
        let queue_dir = format!("{dir}/queue");
        std::fs::create_dir_all(&queue_dir)?;
        info!("store creating at '{dir}'");
        let meta = MetaDataStore::new(store_dir.clone(), meta_pool, shards, meta_store)?;
        let data = DataStore::new(store_dir.clone(), shards, data_store)?;
        let mut graveyards = Vec::new();
        for _ in 0..shards {
            graveyards.push(Graveyard::new(
                store_dir.clone().into(),
                data_store.max_disk_usage.to_bytes(),
            ));
        }

        info!("store created at '{dir}'");
        // TODO: read back the deques for recovery
        let mut deques = BTreeMap::new();
        for queue in std::fs::read_dir(queue_dir.clone())
            .map_err(Error::Io)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name())
        {
            let name = queue.to_str().expect("valid queue name").to_owned();
            let deque = deque::recover(format!("{}/{}", queue_dir, name))?;
            assert!(deques.insert(name, (deque, None)).is_none());
        }

        Ok(Self {
            dir: dir.clone(),
            meta,
            data,
            deques,
            graveyards,
            cache: Lru::new(LRU_CAPACITY),
            log: Log::new(dir, transaction_log_entries),
        })
    }

    pub fn create_deque(
        &mut self,
        dir: ByteStr<SharedImpl>,
        node_size: u64,
        max_disk_usage: u64,
    ) -> Result<(), Error> {
        let Ok(dir) = dir.as_str().map(|s| s.to_owned()) else {
            return Err(Error::DequeInvalidKey);
        };
        if self.deques.contains_key(&dir) {
            return Err(Error::DequeExists(dir));
        }
        let deque = Deque::new(
            format!("{}/queue/{}", self.dir, dir),
            node_size,
            max_disk_usage,
            Version::V1,
        )?;
        assert!(self.deques.insert(dir, (deque, None)).is_none());
        Ok(())
    }

    pub fn delete_deque(&mut self, dir: ByteStr<SharedImpl>) -> Result<(), Error> {
        let Ok(dir) = dir.as_str().map(|s| s.to_owned()) else {
            return Err(Error::DequeInvalidKey);
        };
        if self.deques.remove(&dir).is_some() {
            std::fs::remove_dir(&dir)?;
            Ok(())
        } else {
            Err(Error::DequeNotFound(dir))
        }
    }

    pub fn push_back(
        &mut self,
        dir: ByteStr<SharedImpl>,
        value: BinaryData<SharedImpl>,
    ) -> Result<(), Error> {
        let Ok(dir) = dir.as_str().map(|s| s.to_owned()) else {
            return Err(Error::DequeInvalidKey);
        };
        let (deque, _) = self
            .deques
            .get_mut(&dir)
            .ok_or_else(|| Error::DequeNotFound(dir))?;
        deque.push(value.data().as_slice())?;
        Ok(())
    }

    pub fn pop_front(
        &mut self,
        dir: ByteStr<SharedImpl>,
        buffer: &mut OwnedImpl,
    ) -> Result<Option<Readable<SharedImpl>>, Error> {
        let Ok(dir) = dir.as_str().map(|s| s.to_owned()) else {
            return Err(Error::DequeInvalidKey);
        };
        let (deque, _) = self
            .deques
            .get_mut(&dir)
            .ok_or_else(|| Error::DequeNotFound(dir))?;
        deque.pop(buffer).map_err(Error::Deque)
    }

    pub fn peek(
        &'a mut self,
        dir: ByteStr<SharedImpl>,
        pool: &'static PoolImpl,
    ) -> Result<Option<Readable<SharedImpl>>, Box<dyn std::error::Error + 'a>> {
        let Ok(dir) = dir.as_str().map(|s| s.to_owned()) else {
            return Err(Box::new(Error::DequeInvalidKey));
        };
        let (deque, itr) = self
            .deques
            .get_mut(&dir)
            .ok_or_else(|| Error::DequeNotFound(dir))?;

        if deque.is_empty() {
            return Ok(None);
        }

        if itr.is_none() {
            let peekable: DequeIter = Box::new(
                deque
                    .peek()
                    .map(|(node, file)| {
                        if let Some(file) = file.as_ref().cloned() {
                            Ok(file)
                        } else {
                            node.read().map_err(Error::Deque)
                        }
                    })
                    .flat_map(|file| match file {
                        Ok(file) => {
                            let peek = file.peek(pool.clone(), BufferOwner::Peek).map(|entry| {
                                entry.map(|entry| entry.data.clone()).map_err(Error::Deque)
                            });
                            Ok(peek)
                        }
                        Err(e) => Err(e),
                    })
                    .flatten(),
            ) as _;
            let peekable = peekable.peekable();

            _ = itr.insert(peekable);
        }

        if let Some(itr) = itr {
            let peek = itr
                .peek()
                .map(|result| match result {
                    Ok(x) => Ok(x.clone()),
                    Err(e) => Err(e),
                })
                .transpose();

            match peek {
                Ok(value) => Ok(value),
                Err(e) => Err(Box::new(e) as _),
            }
        } else {
            Ok(None)
        }
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
        if let Some(value) = self.cache.get(&key.data().as_slice().into()) {
            let value = BinaryData::from_owned(value, buffer)?;
            let data = Readable::new(Version::V1, value.clone());
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

            let data = self
                .data
                .get(lookup.shard, file, offset, buffer, Version::V1)?;

            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    pub fn put(
        &mut self,
        key: BinaryData<SharedImpl>,
        value: BinaryData<SharedImpl>,
    ) -> Result<Put, Error> {
        let prepare = self.meta.prepare_put(key.clone())?;
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
                assert_eq!(crc, v1::generate_crc(value.data().as_slice()));
                let meta_put = self.meta.put(key.clone(), prepare, file, offset, len)?;

                Ok(Put {
                    data_flush: flush,
                    meta: meta_put,
                })
            }
            Push::Full => Err(Error::StoreFull),
        };

        if res.is_ok() {
            self.cache
                .put(key.data().as_slice().into(), value.data().as_slice().into());
        }

        res
    }
}

// pub fn store_driver()

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use crate::store::{data::Config as DataConfig, meta::Config as MetaConfig};

    use necronomicon::Pool;
    use tempfile::tempdir;

    use super::*;

    const SHARDS: usize = 100;
    const DATA_SHARD_LEN: u32 = 0x8000;
    const META_SHARD_LEN: u32 = 0x8000 * 0x1000;
    const MAX_DISK_USAGE: u32 = 0xC000 * 0x1000;
    const BLOCK_SIZE: u32 = 0x4000;
    const CAPACITY: usize = 0x1000;
    static META_CONFIG: LazyLock<MetaConfig> = LazyLock::new(|| MetaConfig::test(META_SHARD_LEN));
    const DATA_CONFIG: LazyLock<DataConfig> =
        LazyLock::new(|| DataConfig::test(DATA_SHARD_LEN, MAX_DISK_USAGE));
    static POOL_CONFIG: LazyLock<PoolConfig> =
        LazyLock::new(|| PoolConfig::test(BLOCK_SIZE, CAPACITY));

    #[test]
    fn store_put_get_single() {
        let dir = tempdir().unwrap();
        // let dir_path = dir.path().to_path_buf();
        let dir_path_str = dir.path().to_str().unwrap().to_string();
        let mut store = Store::new(&Config {
            dir: dir_path_str,
            shards: SHARDS,
            meta_store: META_CONFIG.clone(),
            data_store: DATA_CONFIG.clone(),
            pool: POOL_CONFIG.clone(),
        })
        .unwrap();

        let now = std::time::Instant::now();
        let mut flushes = vec![];
        for i in 0..100_000 {
            let key = BinaryData::new(SharedImpl::test_new(format!("key-{}", i).as_bytes()));
            let value = BinaryData::new(SharedImpl::test_new(format!("value-{}", i).as_bytes()));
            let put = store.put(key, value).unwrap();
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
        let pool = PoolImpl::new(BLOCK_SIZE as usize, CAPACITY);
        let random_range = rand::seq::index::sample(&mut rand::thread_rng(), 100_000, 100_000);
        for i in random_range {
            let key = BinaryData::new(SharedImpl::test_new(format!("key-{}", i).as_bytes()));
            let mut buffer = pool.acquire("cat", "test");
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
