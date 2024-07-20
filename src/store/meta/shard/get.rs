use necronomicon::{BinaryData, Pool, PoolImpl, SharedImpl};

use crate::{
    calculate_hash,
    store::meta::{
        decode_key,
        shard::{BufferOwner, Error, Lookup, Metadata, Shard},
    },
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Get(Lookup);

impl From<Get> for Lookup {
    fn from(get: Get) -> Self {
        get.0
    }
}

impl Shard {
    pub(crate) fn get(
        &self,
        key: BinaryData<SharedImpl>,
        pool: &PoolImpl,
    ) -> Result<Option<Get>, Error> {
        let hash = calculate_hash(&key);
        println!("entries: {:?}", self.entries);
        println!("hash {hash}");
        let lookups = self.entries.get(&hash);
        if let Some(lookups) = lookups {
            for lookup in lookups {
                let mut owned = pool.acquire(BufferOwner::Get);
                let start = lookup.offset + Metadata::size() as u64;
                let start = usize::try_from(start).expect("u64 to usize");
                let decoded_key = decode_key(&self.buffer, start, &mut owned)?;

                if decoded_key == key {
                    return Ok(Some(Get(lookup.clone())));
                }
            }
        }
        Ok(None)
    }
}
