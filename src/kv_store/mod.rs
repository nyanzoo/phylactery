use std::{
    collections::BTreeMap,
    io::{Read, Write},
    mem::size_of,
    path::{self, Path, PathBuf},
    time::Duration,
};

use crate::{
    buffer::{Buffer, InMemBuffer, MmapBuffer},
    codec::{self, Decode, Encode},
    dequeue::Dequeue,
    entry::{Data, Version},
    ring_buffer::{self, RingBuffer},
};

mod error;
pub use error::Error;

#[derive(serde::Deserialize, serde::Serialize)]
struct Tombstone {
    dir: u64,
    file: u64,
    offset: u64,
    len: u64,
}

impl Encode for Tombstone {
    fn encode(&self, buf: &mut [u8]) -> Result<(), codec::Error> {
        Ok(bincode::serialize_into(buf, self)?)
    }
}

impl Decode<'_> for Tombstone {
    fn decode(buf: &[u8]) -> Result<Self, codec::Error> {
        Ok(bincode::deserialize(buf)?)
    }
}

// Only compacts the data files, we just write over the meta file.
// Several things to look out for:
// - crash during compaction of files and moving into new file(s)
// - crash during deletion of old file(s)
// - crash during update of meta file
//
// We will do the following:
// 0. write all tombstones to compact meta file
// 1. copy live data from nodes into new file(s) but not part of dequeue yet! (don't intersperse with incoming data)
// 2. swap the new file(s) into the dequeue (these should be less than the node size)
// 3. delete old file(s) (may not be there if read out before compaction completes)
// 4. update meta file to say old slots are available for new data
// 5. update compact meta file to remove tombstones
//
pub struct Graveyard {
    dir: PathBuf,
    popper: ring_buffer::Popper<MmapBuffer>,
}

impl Graveyard {
    pub fn new(dir: PathBuf, popper: ring_buffer::Popper<MmapBuffer>) -> Self {
        Self { dir, popper }
    }

    pub fn bury(self, interval: u64) -> ! {
        let interval = Duration::from_secs(interval);
        loop {
            // Collect all the files to compact.
            let tombs = self.collect();

            for tomb in tombs {
                let file = tomb[0].file;
                let dir = tomb[0].dir;
                let file = format!("{}/{}.bin", dir, file);
                let out = self.dir.join(format!("{}/{}.new", dir, file));
                let file = self.dir.join(file);
                // If file doesn't exist, then we have already compacted it, or removed it.
                if let Ok(mut file) = std::fs::File::open(file.clone()) {
                    let len = file.metadata().expect("no file metadata").len();
                    let mut in_buf = InMemBuffer::new(len);

                    file.read(in_buf.as_mut()).expect("failed to read file");

                    let out_buf = Self::compact_buf(tomb, in_buf);

                    let mut out = std::fs::File::create(out.clone()).expect("failed to create file");

                    out.write(&out_buf).expect("failed to write file");
                }

                std::fs::remove_file(file.clone()).expect("failed to remove file");
                std::fs::rename(out, file).expect("failed to rename file");
            }

            std::thread::sleep(interval);
        }
    }

    fn collect(&self) -> Vec<Vec<Tombstone>> {
        let len = size_of::<Tombstone>();

        let mut nodes = vec![];
        let mut node = 0;

        let mut buf = vec![0; 32];
        // If we crash and it happens to be that tombstones map to same spot as different data,
        // then we will delete data we should keep.
        while let Ok(bytes) = self.popper.pop(&mut buf) {
            assert!(bytes == len, "invalid tombstone length");
            let tomb = Tombstone::decode(&buf).expect("failed to decode tombstone");

            if nodes.is_empty() {
                nodes.push(vec![]);
            }

            if nodes[node].is_empty() {
                nodes[node].push(tomb);
            } else {
                let last = nodes[node].last().expect("no tombstones in node");
                if tomb.file == last.file && tomb.dir == last.dir {
                    nodes[node].push(tomb);
                } else {
                    node += 1;
                    nodes.push(vec![]);
                    nodes[node].push(tomb);
                }
            }
        }

        nodes
    }

    // We can maybe fix the problem of accidentally deleting data we don't want by
    // comparing crcs of the data and tombstones.
    fn compact_buf(tombs: Vec<Tombstone>, in_buf: InMemBuffer) -> Vec<u8> {
        let mut out_buf = vec![];
        let mut begin = 0;

        for tomb in tombs {
            let end = tomb.offset as usize;
            out_buf.extend_from_slice(&in_buf.as_ref()[begin..end]);
            begin = (tomb.offset + tomb.len) as usize;
        }

        out_buf.extend_from_slice(&in_buf.as_ref()[begin..]);

        out_buf
    }
}

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
struct Metadata<'a> {
    crc: u32,
    file: u64,
    offset: u64,
    key: &'a [u8],
    // We set the tombstone and do not accept additional data until we have compacted.
    state: MetaState,
}

const METADATA_SIZE: usize = size_of::<Metadata>();

impl<'a> Decode<'a> for Metadata<'a> {
    fn decode(buf: &'a [u8]) -> Result<Self, codec::Error>
    where
        Self: Sized,
    {
        bincode::deserialize(buf).map_err(|err| codec::Error::from(err))
    }
}

// We will need to have 2 layers:
// 1. The key metadata layer that holds [crc, file, offset, length] for each key
//    where the hash of the key is the index into the key metadata layer.
// 2. The data layer is where we store [crc, key length, key, value length, value]
pub struct KvStore<'a, S>
where
    S: AsRef<str>,
{
    // store key metadata
    lookup: BTreeMap<&'a [u8], KeyLookup>,
    // meta file
    meta: RingBuffer<MmapBuffer>,
    // data files
    dequeue: Dequeue<S>,
    // graveyard pusher
    pusher: ring_buffer::Pusher<MmapBuffer>,
}

impl<S> KvStore<'_, S>
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
    pub fn get<'a>(&self, key: &[u8], buf: &'a mut [u8]) -> Result<Lookup, Error> {
        if let Some(lookup) = self.lookup.get(key) {
            self.meta.get(lookup.offset as u64, buf)?;

            let Metadata {
                crc,
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

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        // We need to tombstone old entry if it exists.
        if let Some(lookup) = self.lookup.get(key) {
            let mut buf = vec![0; METADATA_SIZE];
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone,
            // which means the ringbuffer will need to have an update method.
            self.meta.get(lookup.offset as u64, &mut buf)?;
            self.meta.update(lookup.offset as u64, |buf: &mut [u8]| {
                let mut metadata = Metadata::decode(buf).expect("metadata decode failed");
                metadata.state = MetaState::Compacting;
                bincode::serialize_into(buf, &metadata).expect("failed to serialize metadata");
            })?;
            self.pusher.push(&buf)?;
        }

        // Dequeue needs to also return the offset and file of the data.
        let data = self.dequeue.push(value)?;

        // Need to store key here too...
        let metadata = Metadata {
            crc: data.crc(),
            file: todo!(),
            offset: todo!(),
            state: MetaState::Ready,
            key,
        };
        let metadata = bincode::serialize(&metadata).expect("failed to serialize metadata");

        // Maybe we should have meta also be a ring buffer?
        let offset = self.meta.push(&metadata)?;

        self.lookup.insert(key, KeyLookup { offset });

        Ok(())
    }
}
