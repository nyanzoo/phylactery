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
    data::store::Store as DataStore,
    error::Error,
    graveyard::graveyard::Graveyard,
    meta::{self, shard, store::Store as MetaDataStore},
};

pub struct Put {
    data_flush: Flush<LazyWriteFileFlush>,
    meta: meta::shard::put::Put,
}

pub struct Store {
    meta: MetaDataStore,
    data: DataStore,
    graveyards: Vec<Graveyard>,
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
        let data = DataStore::new(dir, shards, data_shard_len, max_disk_usage)?;
        let mut graveyards = Vec::new();
        for _ in 0..shards {
            graveyards.push(Graveyard::new(max_disk_usage));
        }

        Ok(Self {
            meta,
            data,
            graveyards,
        })
    }

    // pub fn delete(&mut self, key: BinaryData<SharedImpl>) -> Result<(), Error> {
    //     if let Some(Delete {
    //         lookup,
    //         metadata,
    //         flush,
    //     }) = self.meta.delete(key)?
    //     {
    //         let tomb = Tombstone {
    //             crc: metadata.crc,
    //             file: metadata.file,
    //             offset: metadata.offset,
    //             len: metadata.len,
    //         };

    //         let shard = usize::try_from(lookup.file).expect("u64 -> usize");
    //         let graveyard = &mut self.graveyards[shard];
    //         graveyard.bury(tomb);
    //         if graveyard.should_compact() {
    //             let compaction = graveyard.compact();
    //             self.data.compact()?;
    //             self.meta.compact(&compaction.shards().collect::<Vec<_>>())?;
    //         }
    //     }

    //     Ok(())
    // }

    pub fn get(
        &mut self,
        key: BinaryData<SharedImpl>,
        buffer: &mut OwnedImpl,
    ) -> Result<Option<Readable<SharedImpl>>, Error> {
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
        match self
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
                let meta_put = self.meta.put(key, prepare, file, offset, len)?;
                // println!("meta_put");
                Ok(Some(Put {
                    data_flush: flush,
                    meta: meta_put,
                }))
            }
            Push::Full => Ok(None),
        }
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
        let dir_path = dir.path().to_path_buf();
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

        crate::store::tree(&dir_path);

        for Put {
            data_flush,
            mut meta,
        } in flushes
        {
            data_flush.flush().unwrap();
            meta.commit().unwrap();
        }

        let pool = PoolImpl::new(BLOCK_SIZE, CAPACITY);
        for i in 0..100_000 {
            let key = BinaryData::new(SharedImpl::test_new(format!("key-{}", i).as_bytes()));
            let mut buffer = pool.acquire("test");
            let data = store.get(key, &mut buffer).unwrap().unwrap();
            assert_eq!(
                data.into_inner().data().as_slice(),
                format!("value-{}", i).as_bytes()
            );
        }
        let elapsed = now.elapsed();
        println!("100,000 put/get: {:?}", elapsed);
    }
}
