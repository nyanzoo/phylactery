use necronomicon::{BinaryData, Pool as _, PoolImpl, SharedImpl};

use crate::{
    buffer::{Buffer as _, Flush, MmapBuffer},
    calculate_hash,
    store::{
        meta::{
            decode_key,
            shard::{BufferOwner, Error, Lookup, Shard, Tombstone},
            Metadata,
        },
        MetaState,
    },
    u64_to_usize,
};

pub(crate) struct Delete<'a> {
    lookup: Lookup,
    metadata: Metadata,
    flush: Flush<'a, MmapBuffer>,
}

impl<'a> Delete<'a> {
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
        if let Some(lookups) = lookups {
            for lookup in lookups {
                let meta: Metadata = self
                    .buffer
                    .decode_at(u64_to_usize(lookup.offset), Metadata::size())?;
                // we can skip over all the metadata that are already tombstones.
                if meta.state == MetaState::Compacting {
                    continue;
                }

                let start = lookup.offset + Metadata::size() as u64;
                let state_offset = u64_to_usize(lookup.offset) + Metadata::state_offset();
                let start = u64_to_usize(start);
                let mut owned = pool.acquire(BufferOwner::Delete);
                let decoded_key = decode_key(&self.buffer, start, &mut owned)?;

                if decoded_key == key {
                    let len = usize::try_from(decoded_key.len()).expect("u64 to usize")
                        + Metadata::size();

                    let tombstone = Tombstone {
                        len,
                        offset: u64_to_usize(lookup.offset),
                    };

                    self.tombstones.push(tombstone);

                    let flushable =
                        self.buffer
                            .encode_at(state_offset, 1, &MetaState::Compacting)?;

                    return Ok(Some(Delete {
                        lookup: Lookup {
                            file: self.shard,
                            offset: lookup.offset,
                        },
                        metadata: meta,
                        flush: flushable,
                    }));
                }
            }
        }
        Ok(None)
    }
}
