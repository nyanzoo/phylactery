use necronomicon::{BinaryData, Shared};

use crate::{
    buffer::{Buffer, Flush, MmapBuffer},
    calculate_hash,
    store::meta::{
        shard::{Error, Lookup, Shard},
        Metadata, MetadataWithKey,
    },
    u64_to_usize, usize_to_u64,
};

pub(crate) struct PreparePut {
    lookup: Lookup,
    meta_size: u64,
    hash: u64,
}

pub(crate) struct Put {
    lookup: Lookup,
    metadata: Metadata,
    flush: Flush<<MmapBuffer as Buffer>::Flushable>,
}

impl Put {
    pub(crate) fn lookup(&self) -> Lookup {
        self.lookup
    }

    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        self.flush.flush()?;
        Ok(())
    }
}

impl Shard {
    pub(crate) fn prepare_put<S>(&mut self, key: BinaryData<S>) -> Result<PreparePut, Error>
    where
        S: Shared,
    {
        // We must start with compacting and only change to `Full`
        // if we succeed in writing the data.
        let meta = MetadataWithKey::new(Metadata::tombstone(), key.clone());

        // ignore the flush, we will do that at the end of a store transaction(s).
        let _ = self
            .buffer
            .encode_at(u64_to_usize(self.cursor), meta.size(), &meta)?;

        Ok(PreparePut {
            lookup: Lookup {
                file: self.shard,
                offset: self.cursor,
            },
            meta_size: usize_to_u64(meta.size()),
            hash: calculate_hash(&key),
        })
    }

    pub(crate) fn put(
        &mut self,
        prepare: PreparePut,
        file: u64,
        offset: u64,
        len: u64,
    ) -> Result<Put, Error> {
        let PreparePut {
            lookup,
            meta_size,
            hash,
        } = prepare;
        assert_eq!(lookup.offset, self.cursor);
        assert_eq!(lookup.file, self.shard);

        let metadata = Metadata::new(file, offset, len);

        let flush =
            self.buffer
                .encode_at(u64_to_usize(lookup.offset), Metadata::size(), &metadata)?;

        self.cursor += meta_size;
        self.entries.entry(hash).or_default().push_back(lookup);

        Ok(Put {
            lookup,
            metadata,
            flush,
        })
    }
}
