use crate::{
    buffer::{Buffer as _, Flush, MmapBuffer},
    calculate_hash,
    store::meta::{
        shard::{Error, Lookup, Shard},
        Metadata, MetadataWrite,
    },
    u64_to_usize, usize_to_u64,
};

pub(crate) struct PreparePut {
    lookup: Lookup,
    meta_size: u64,
    hash: u64,
}

pub(crate) struct Put<'a> {
    lookup: Lookup,
    metadata: Metadata,
    flush: Flush<'a, MmapBuffer>,
}

impl<'a> Put<'a> {
    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        self.flush.flush()?;
        Ok(())
    }
}

impl Shard {
    pub(crate) fn prepare_put(&mut self, key: &[u8]) -> Result<PreparePut, Error> {
        // We must start with compacting and only change to `Full`
        // if we succeed in writing the data.
        let meta = MetadataWrite {
            meta: Metadata::tombstone(),
            key,
        };

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

        let offset = u64_to_usize(self.cursor);
        let len = usize::try_from(meta_size).expect("u64 to usize");
        self.cursor += meta_size;
        self.entries.entry(hash).or_default().push_back(lookup);

        Ok(Put {
            lookup,
            metadata,
            flush,
        })
    }
}
