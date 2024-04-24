use std::{collections::BTreeMap, mem::size_of, path::Path};

use crate::{
    buffer::MmapBuffer,
    codec::{self, Decode, Encode},
    dequeue::{Dequeue, Push},
    entry::{Data, Version},
    ring_buffer::{self, RingBuffer},
};

mod graveyard;
pub use graveyard::Graveyard;

mod error;
pub use error::Error;

pub enum Lookup<'a> {
    Absent,
    Found(Data<'a>),
}

// Lookup -> meta file location -> data file location -> data
struct KeyLookup {
    offset: u64,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub enum MetaState {
    // Ready to accept data (never actively set)
    Ready,
    // We need to compact and cannot accept new metadata in this slot
    Compacting,
    // Has live data associated with it
    Full,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Metadata {
    crc: u32,
    file: u64,
    offset: u64,
    key: Vec<u8>,
    // We set the tombstone and do not accept additional data until we have compacted.
    state: MetaState,
}

const METADATA_SIZE: usize = size_of::<Metadata>();

impl Decode<'_> for Metadata {
    fn decode(buf: &[u8]) -> Result<Self, codec::Error>
    where
        Self: Sized,
    {
        bincode::deserialize(buf).map_err(codec::Error::from)
    }
}

impl Encode for Metadata {
    fn encode(&self, buf: &mut [u8]) -> Result<(), codec::Error> {
        bincode::serialize_into(buf, self).map_err(codec::Error::from)
    }
}

// We will need to have 2 layers:
// 1. The key metadata layer that holds [crc, file, offset, key] for each key
//    where the hash of the key is the index into the key metadata layer.
// 2. The data layer is where we store [crc, key length, key, value length, value]
pub struct KVStore<S>
where
    S: AsRef<str>,
{
    // store key metadata
    lookup: BTreeMap<Vec<u8>, KeyLookup>,
    // meta file
    meta: RingBuffer<MmapBuffer>,
    // data files
    dequeue: Dequeue<S>,
    // graveyard pusher
    pusher: ring_buffer::Pusher<MmapBuffer>,
}

// The expectation is that this is single threaded.
impl<S> KVStore<S>
where
    S: AsRef<str> + AsRef<Path>,
{
    pub fn new(
        meta_path: S,
        meta_size: u64,
        data_path: S,
        node_size: u64,
        version: Version,
        pusher: ring_buffer::Pusher<MmapBuffer>,
    ) -> Result<Self, Error> {
        let meta = MmapBuffer::new(meta_path, meta_size)?;
        let meta = RingBuffer::new(meta, version)?;
        let dequeue = Dequeue::new(data_path, node_size, version)?;

        let mut lookup = BTreeMap::new();
        for pos in meta.iter() {
            let mut buf = vec![0; METADATA_SIZE];
            meta.get(pos, &mut buf)?;
            let Metadata { offset, key, .. } = Metadata::decode(&buf)?;

            lookup.insert(key, KeyLookup { offset });
        }

        Ok(Self {
            lookup,
            meta,
            dequeue,
            pusher,
        })
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        if let Some(lookup) = self.lookup.remove(key) {
            let mut buf = vec![0; METADATA_SIZE];
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone,
            // which means the ringbuffer will need to have an update method.
            self.meta.get(lookup.offset, &mut buf)?;
            self.meta.update(lookup.offset, |data: &mut [u8]| {
                let mut metadata = Metadata::decode(data).expect("metadata decode failed");
                metadata.state = MetaState::Compacting;
                metadata.encode(data).expect("metadata encode failed");
            })?;
        }

        Ok(())
    }

    // Need to think through how to handle the possible state of insert to backing queue
    // was successful, but then we crash, and the meta file is not updated.
    // We need to be able to delete that data.
    pub fn get<'a, 'b>(&'a self, key: &[u8], buf: &'b mut [u8]) -> Result<Lookup, Error>
    where
        'b: 'a,
    {
        if let Some(lookup) = self.lookup.get(key) {
            self.meta.get(lookup.offset, buf)?;

            let Metadata {
                crc: _, // TODO: verify crc
                file,
                offset,
                state,
                ..
            } = Metadata::decode(buf)?;

            match state {
                MetaState::Full => {}
                MetaState::Compacting | MetaState::Ready => {
                    return Err(Error::KeyNotFound(format!("{:?}", key)))
                }
            }

            let data = self.dequeue.get(file, offset, buf)?;

            data.verify()?;

            Ok(Lookup::Found(data))
        } else {
            Ok(Lookup::Absent)
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        // We need to tombstone old entry if it exists.
        if let Some(lookup) = self.lookup.get(key) {
            let offset = lookup.offset;
            let mut buf = vec![0; METADATA_SIZE];
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone,
            // which means the ringbuffer will need to have an update method.
            self.meta.get(offset, &mut buf)?;
            self.meta.update(offset, |data: &mut [u8]| {
                let mut metadata = Metadata::decode(data).expect("metadata decode failed");
                metadata.state = MetaState::Compacting;
                metadata.encode(data).expect("metadata encode failed");
            })?;
            self.pusher.push(&buf)?;
        }

        // Dequeue needs to also return the offset and file of the data.
        let Push { file, offset, crc } = self.dequeue.push(value)?;
        self.dequeue.flush()?;

        // Need to store key here too...
        let metadata = Metadata {
            crc,
            file,
            offset,
            state: MetaState::Full,
            key: key.to_vec(),
        };
        let metadata = bincode::serialize(&metadata).expect("failed to serialize metadata");

        // Maybe we should have meta also be a ring buffer?
        let offset = self.meta.push(&metadata)?;

        self.lookup.insert(key.to_vec(), KeyLookup { offset });

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        buffer::MmapBuffer,
        codec::Decode,
        entry::{Data, Version},
        ring_buffer::ring_buffer,
    };

    use super::KVStore;

    #[test]
    fn test_put_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.into_path();

        let mmap_path = path.join("mmap.bin");
        let buffer = MmapBuffer::new(mmap_path, 1024).expect("mmap buffer failed");

        let (pusher, _popper) = ring_buffer(buffer, Version::V1).expect("ring buffer failed");

        let meta_path = path.join("meta.bin");
        let meta_path = meta_path.to_str().unwrap();

        let data_path = path.join("data.bin");
        let data_path = data_path.to_str().unwrap();

        let mut store = KVStore::new(meta_path, 1024, data_path, 1024, Version::V1, pusher)
            .expect("KVStore::new failed");

        store
            .insert(b"pets", "cats".as_bytes())
            .expect("insert failed");
        let mut buf = vec![0; 64];
        store.get(b"pets", &mut buf).expect("key not found");

        let actual = Data::decode(&buf)
            .expect("failed to deserialize")
            .into_inner();
        assert_eq!(actual, b"cats");
    }
}
