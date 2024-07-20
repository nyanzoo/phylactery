use necronomicon::{BinaryData, PoolImpl, SharedImpl};

use super::{
    data::store::Store as DataStore,
    error::Error,
    graveyard::{graveyard::Graveyard, tombstone::Tombstone},
    meta::{shard::delete::Delete, store::Store as MetaDataStore},
};

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
        pool: PoolImpl,
    ) -> Result<Self, Error> {
        let meta = MetaDataStore::new(dir.clone(), pool, shards, meta_shard_len)?;
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
}
