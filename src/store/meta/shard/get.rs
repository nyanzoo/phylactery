use necronomicon::{BinaryData, Pool, PoolImpl, SharedImpl};

use crate::{
    buffer::Buffer,
    calculate_hash,
    store::meta::{
        decode_key,
        shard::{BufferOwner, Error, Lookup, Metadata, Shard},
    },
};

#[derive(Copy, Clone, Debug)]
pub(crate) struct Get {
    pub lookup: Lookup,
    pub metadata: Metadata,
}

impl Get {
    pub(crate) fn lookup(&self) -> Lookup {
        self.lookup
    }

    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

impl Shard {
    pub(crate) fn get(
        &self,
        key: BinaryData<SharedImpl>,
        pool: &PoolImpl,
    ) -> Result<Option<Get>, Error> {
        let hash = calculate_hash(&key);
        let lookups = self.entries.get(&hash);
        if let Some(lookups) = lookups {
            for lookup in lookups {
                let mut owned = pool.acquire(BufferOwner::Get);
                let meta: Metadata = self.buffer.decode_at(lookup.offset, Metadata::size())?;

                let start = lookup.offset + Metadata::size();
                let start = usize::try_from(start).expect("u64 to usize");
                let decoded_key = decode_key(&self.buffer, start, &mut owned)?;

                if decoded_key == key {
                    return Ok(Some(Get {
                        lookup: *lookup,
                        metadata: meta,
                    }));
                }
            }
        }
        Ok(None)
    }
}
