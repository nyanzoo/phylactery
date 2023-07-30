use std::{
    collections::BTreeMap,
    io::{Read, Write},
    mem::size_of,
    path::Path,
};

use necronomicon::{kv_store_codec::Key, Decode, Encode};

use crate::{
    alloc::{Entry, FixedSizeAllocator},
    buffer::MmapBuffer,
    dequeue::{Dequeue, Push},
    entry::{Data, Version},
    ring_buffer::{self},
};

mod graveyard;
pub use graveyard::Graveyard;
use graveyard::{Tombstone, TOMBSTONE_SIZE};

mod error;
pub use error::Error;

#[derive(Debug, Eq, PartialEq)]
pub enum Lookup {
    Absent,
    Found(Data),
}

pub enum MetaState {
    // We need to compact and cannot accept new metadata in this slot
    Compacting,
    // Has live data associated with it
    Full,
}

impl<W> Encode<W> for MetaState
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            Self::Compacting => 0u8.encode(writer),
            Self::Full => 1u8.encode(writer),
        }
    }
}

impl<R> Decode<R> for MetaState
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        match u8::decode(reader)? {
            0 => Ok(Self::Compacting),
            1 => Ok(Self::Full),
            _ => Err(necronomicon::Error::Decode(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid meta state",
            ))),
        }
    }
}

struct Metadata {
    crc: u32,
    file: u64,
    offset: u64,
    len: u64,
    key: Key,
    // We set the tombstone and do not accept additional data until we have compacted.
    state: MetaState,
}

impl Into<Tombstone> for Metadata {
    fn into(self) -> Tombstone {
        Tombstone {
            crc: self.crc,
            file: self.file,
            offset: self.offset,
            len: self.len,
        }
    }
}

const METADATA_SIZE: usize = size_of::<Metadata>();

impl<W> Encode<W> for Metadata
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.crc.encode(writer)?;
        self.file.encode(writer)?;
        self.offset.encode(writer)?;
        self.len.encode(writer)?;
        self.key.encode(writer)?;
        self.state.encode(writer)?;
        Ok(())
    }
}

impl<R> Decode<R> for Metadata
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let crc = u32::decode(reader)?;
        let file = u64::decode(reader)?;
        let offset = u64::decode(reader)?;
        let len = u64::decode(reader)?;
        let key = Key::decode(reader)?;
        let state = MetaState::decode(reader)?;
        Ok(Metadata {
            crc,
            file,
            offset,
            len,
            key,
            state,
        })
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
    lookup: BTreeMap<Key, Entry<MmapBuffer>>,
    // meta file
    meta: FixedSizeAllocator<MmapBuffer, METADATA_SIZE>,
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
        let mut meta = FixedSizeAllocator::new(meta)?;
        let dequeue = Dequeue::new(data_path, node_size, version)?;

        let mut lookup = BTreeMap::new();
        for entry in meta.recovered_entries()? {
            let Metadata { key, .. } = entry.data()?;

            lookup.insert(key, entry);
        }

        Ok(Self {
            lookup,
            meta,
            dequeue,
            pusher,
        })
    }

    pub fn delete(&mut self, key: &Key) -> Result<(), Error> {
        if let Some(mut entry) = self.lookup.remove(key) {
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone.
            let mut meta = entry.data::<Metadata>()?;

            match meta.state {
                MetaState::Full => {}
                MetaState::Compacting => {
                    // Remove bad key!
                    self.lookup.remove(key);
                    return Err(Error::KeyNotFound(format!("{:?}", key)));
                }
            }

            meta.state = MetaState::Compacting;

            entry.update(&meta)?;

            // needs to be tombstone!
            let tombstone: Tombstone = meta.into();
            let mut buf = vec![0; TOMBSTONE_SIZE];
            tombstone.encode(&mut buf)?;
            self.pusher.push(buf)?;
        }

        Ok(())
    }

    /// # Description
    /// Get the value associated with the key.
    ///
    /// ## Details
    /// ### Lookup
    /// We will first lookup the key in our key metadata layer. If the key
    /// is not found, we will return a `Lookup::Absent`. If the key is found, we will
    /// then lookup the metadata in the meta file. If the metadata is not found,
    /// we will error. If the metadata is found, we will then lookup the data
    /// in the data file. If the data is not found, we will error. If the data
    /// is found, we will verify the data and return a `Lookup::Found`.
    ///
    /// ### Tombstone
    /// It is possible that we crashed after we added data to the dequeue, but
    /// before we updated the meta file. In this case, we will have a tombstone
    /// of `MetaState::Compacting` in the meta file (default value) and can
    /// remove the key from the lookup. GC will happen in [`Graveyard`].
    ///
    /// # Example
    /// ```rust, ignore
    /// let mut buf = vec![0; 1024]; // for reading in value as a [`Data`].
    /// store.get(b"key", &mut buf)?;
    /// ```
    ///
    /// # Arguments
    /// - `key` - The key to lookup.
    /// - `buf` - The buffer to read the value, as [`Data`] into.
    ///
    /// # Returns
    /// - `Ok(Lookup::Found)` - The key was found and the value was read into `buf`.
    /// - `Ok(Lookup::Absent)` - The key was not found.
    ///
    /// # Errors
    /// See [`Error`].
    pub fn get(&mut self, key: &Key, buf: &mut Vec<u8>) -> Result<Lookup, Error> {
        if let Some(entry) = self.lookup.get(key) {
            let meta = entry.data::<Metadata>()?;

            let Metadata {
                crc: _, // TODO: verify crc
                file,
                offset,
                state,
                ..
            } = meta;

            match state {
                MetaState::Full => {}
                MetaState::Compacting => {
                    // Remove bad key!
                    self.lookup.remove(key);
                    return Err(Error::KeyNotFound(format!("{:?}", key)));
                }
            }

            let data = self.dequeue.get(file, offset, buf)?;

            data.verify()?;

            Ok(Lookup::Found(data))
        } else {
            Ok(Lookup::Absent)
        }
    }

    /// # Description
    /// Insert a key/value pair into the store. If the key already exists, the
    /// value will be overwritten.
    ///
    /// ## Details
    /// ### Inserting a new key/value pair
    /// When inserting a new key/value pair, we will first write the value
    /// to our backing dequeue and get a file/offset pair. We will then
    /// write the metadata to our meta file, which will include the crc,
    /// file, offset, state (for GC) and key. We will then insert the key into our lookup
    /// table, which will be the hash of the key to the offset in the meta file.
    ///
    /// ### Overwriting an existing key/value pair
    /// Same as inserting a new key/value pair, except we will first tombstone
    /// the old entry in the meta file.
    ///
    /// # Example
    /// ```rust, ignore
    /// store.insert(b"key", b"value");
    /// ```
    ///
    /// # Arguments
    /// - `key` - The key to insert.
    /// - `value` - The value to insert.
    ///
    /// # Errors
    /// See [`Error`] for more details.
    pub fn insert(&mut self, key: Key, value: &[u8]) -> Result<(), Error> {
        // We need to tombstone old entry if it exists.
        if let Some(entry) = self.lookup.get(&key) {
            let mut meta = entry.data::<Metadata>()?;

            meta.state = MetaState::Compacting;
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone.
            let tombstone: Tombstone = meta.into();
            let mut buf = vec![0; TOMBSTONE_SIZE];
            tombstone.encode(&mut buf)?;
            self.pusher.push(buf)?;
        }

        // Dequeue needs to also return the offset and file of the data.
        let Push {
            file,
            offset,
            len,
            crc,
        } = self.dequeue.push(value)?;
        self.dequeue.flush()?;

        // Need to store key here too...
        let metadata = Metadata {
            crc,
            file,
            offset,
            len,
            state: MetaState::Full,
            key,
        };
        // Maybe we should have meta also be a ring buffer?
        let mut entry = self.meta.alloc()?;
        entry.update(&metadata)?;

        self.lookup.insert(key, entry);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use necronomicon::kv_store_codec::Key;

    use crate::{
        buffer::MmapBuffer,
        entry::Version,
        kv_store::{Graveyard, Lookup},
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
            .insert(key(b"pets"), "cats".as_bytes())
            .expect("insert failed");

        let mut buf = vec![0; 64];
        let Lookup::Found(data) = store.get(&key(b"pets"), &mut buf).expect("key not found")
        else {
            panic!("key not found");
        };

        let actual = data.into_inner();
        assert_eq!(actual, b"cats");
    }

    #[test]
    fn test_graveyard() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.into_path();

        let mmap_path = path.join("mmap.bin");
        let buffer = MmapBuffer::new(mmap_path, 1024).expect("mmap buffer failed");

        let (pusher, popper) = ring_buffer(buffer, Version::V1).expect("ring buffer failed");

        let meta_path = path.join("meta.bin");
        let meta_path = meta_path.to_str().unwrap();

        let data_path = path.join("data");
        let data_path = data_path.to_str().unwrap();

        let mut store = KVStore::new(meta_path, 1024, data_path, 1024, Version::V1, pusher)
            .expect("KVStore::new failed");

        let pclone = path.clone();
        _ = std::thread::spawn(move || {
            let graveyard = Graveyard::new(pclone.join("data"), popper);
            graveyard.bury(1);
        });

        store
            .insert(key(b"pets"), "cats".as_bytes())
            .expect("insert failed");

        store
            .insert(key(b"pets"), "dogs".as_bytes())
            .expect("insert failed");

        let mut buf = vec![0; 64];
        let Lookup::Found(data) = store.get(&key(b"pets"), &mut buf).expect("key not found")
        else {
            panic!("key not found");
        };

        let actual = data.into_inner();
        assert_eq!(actual, b"dogs");

        store.delete(&key(b"pets")).expect("delete failed");
        // Wait long enough for graveyard to run
        std::thread::sleep(std::time::Duration::from_secs(5));
        // assert that the data folder is empty
        let mut buf = vec![0; 64];
        let Lookup::Absent = store.get(&key(b"pets"), &mut buf).expect("key not found")
        else {
            panic!("key not found");
        };

        assert!(!std::path::Path::exists(&path.join("data").join("0.bin")));
        assert!(!std::path::Path::exists(&path.join("data").join("1.bin")));
    }

    fn key(key: &[u8]) -> Key {
        let mut buf = [0; 32];
        buf[..key.len()].copy_from_slice(key);
        Key::from(buf)
    }

    #[allow(dead_code)]
    fn tree(path: &std::path::Path) {
        std::io::stdout()
            .write_all(
                &std::process::Command::new("tree")
                    .arg(path)
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap();
    }

    #[allow(dead_code)]
    fn hexyl(path: &std::path::Path) {
        std::io::stdout()
            .write_all(
                &std::process::Command::new("hexyl")
                    .arg(path)
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap();
    }
}
