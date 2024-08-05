use necronomicon::{BinaryData, Pool as _, PoolImpl, SharedImpl};

use crate::{
    buffer::{Buffer, Flush, MmapBuffer},
    calculate_hash,
    store::{
        meta::{
            decode_key,
            shard::{BufferOwner, Error, Lookup, Shard, Tombstone},
            Metadata,
        },
        MetaState,
    },
};

pub struct Delete {
    lookup: Lookup,
    metadata: Metadata,
    flush: Flush<<MmapBuffer as Buffer>::Flushable>,
}

impl Delete {
    pub(crate) fn lookup(&self) -> Lookup {
        self.lookup
    }

    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    // TODO: would be better to probably just call flush on the file directly.
    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        self.flush.flush()?;
        Ok(())
    }
}

impl Shard {
    pub(crate) fn delete(
        &mut self,
        key: BinaryData<SharedImpl>,
        pool: &PoolImpl,
    ) -> Result<Option<Delete>, Error> {
        let hash = calculate_hash(&key);
        let lookups = self.entries.get(&hash);
        let res = if let Some(lookups) = lookups {
            let mut res = None;
            let mut remove = None;
            for (i, lookup) in lookups.iter().enumerate() {
                let meta: Metadata = self.buffer.decode_at(lookup.offset, Metadata::size())?;
                // we can skip over all the metadata that are already tombstones.
                if meta.state == MetaState::Compacting {
                    continue;
                }

                let start = lookup.offset + Metadata::size();
                let state_offset = lookup.offset + Metadata::state_offset();
                let mut owned = pool.acquire(BufferOwner::Delete);
                let decoded_key = decode_key(&self.buffer, start, &mut owned)?;

                if decoded_key == key {
                    let len = decoded_key.len() + Metadata::size();

                    let tombstone = Tombstone {
                        len,
                        offset: lookup.offset,
                    };

                    self.tombstones.push(tombstone);

                    let flush = self
                        .buffer
                        .encode_at(state_offset, 1, &MetaState::Compacting)?;

                    res = Some(Delete {
                        lookup: Lookup {
                            shard: self.shard,
                            offset: lookup.offset,
                        },
                        metadata: meta,
                        flush,
                    });
                    remove = Some(i);
                }

                if let Some(remove) = remove {
                    self.entries.get_mut(&hash).unwrap().remove(remove);
                }

                break;
            }
            res
        } else {
            None
        };

        Ok(res)
    }
}
