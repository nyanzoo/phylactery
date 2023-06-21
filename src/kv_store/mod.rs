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
    // We need to compact and cannot accept new metadata in this slot
    Compacting,
    // Has live data associated with it
    Full,
    // Ready to accept data
    Ready,
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
        bincode::deserialize(buf).map_err(|err| codec::Error::from(err))
    }
}

impl Encode for Metadata {
    fn encode(&self, buf: &mut [u8]) -> Result<(), codec::Error> {
        bincode::serialize_into(buf, self).map_err(|err| codec::Error::from(err))
    }
}

// We will need to have 2 layers:
// 1. The key metadata layer that holds [crc, file, offset, key] for each key
//    where the hash of the key is the index into the key metadata layer.
// 2. The data layer is where we store [crc, key length, key, value length, value]
pub struct KvStore<S>
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

impl<S> KvStore<S>
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

        // TODO: populate in-mem map.

        Ok(KvStore {
            lookup: BTreeMap::new(),
            meta,
            dequeue,
            pusher,
        })
    }

    // Need to think through how to handle the possible state of insert to backing queue
    // was successful, but then we crash, and the meta file is not updated.
    // We need to be able to delete that data.
    pub fn get<'a, 'b>(&'a self, key: &[u8], buf: &'b mut [u8]) -> Result<Lookup, Error>
    where
        'b: 'a,
    {
        if let Some(lookup) = self.lookup.get(key) {
            self.meta.get(lookup.offset as u64, buf)?;

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
            let mut buf = vec![0; METADATA_SIZE];
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone,
            // which means the ringbuffer will need to have an update method.
            self.meta.get(lookup.offset as u64, &mut buf)?;
            self.meta.update(lookup.offset as u64, |data: &mut [u8]| {
                let mut metadata = Metadata::decode(data).expect("metadata decode failed");
                metadata.state = MetaState::Compacting;
                metadata.encode(data).expect("metadata encode failed");
            })?;
            self.pusher.push(&buf)?;
        }

        // Dequeue needs to also return the offset and file of the data.
        let Push { file, offset, crc } = self.dequeue.push(value)?;

        // Need to store key here too...
        let metadata = Metadata {
            crc,
            file,
            offset,
            state: MetaState::Ready,
            key: key.to_vec(),
        };
        let metadata = bincode::serialize(&metadata).expect("failed to serialize metadata");

        // Maybe we should have meta also be a ring buffer?
        let offset = self.meta.push(&metadata)?;

        self.lookup.insert(key.to_vec(), KeyLookup { offset });

        Ok(())
    }
}
