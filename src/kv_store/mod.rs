use std::{
    collections::BTreeMap,
    io::{Read, Write},
    mem::size_of,
};

use necronomicon::{
    BinaryData, Decode, DecodeOwned, Encode, Owned, Pool, PoolImpl, Shared, SharedImpl,
};

use crate::{
    alloc::{Entry, FixedSizeAllocator},
    buffer::MmapBuffer,
    dequeue::{Dequeue, Push},
    entry::{Data, Version},
    ring_buffer::{self},
    Error,
};

mod graveyard;
pub use graveyard::Graveyard;
use graveyard::Tombstone;

#[derive(Debug, Eq, PartialEq)]
pub enum Lookup<S>
where
    S: Shared,
{
    Absent,
    Found(Data<S>),
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

struct Metadata<S>
where
    S: Shared,
{
    crc: u32,
    file: u64,
    offset: u64,
    len: u64,
    key: BinaryData<S>,
    // We set the tombstone and do not accept additional data until we have compacted.
    state: MetaState,
}

// NOTE: keep in sync with `Metadata` struct
const fn metadata_block_size(max_key_size: usize) -> usize {
    size_of::<u32>() + size_of::<u64>() * 3 + max_key_size + size_of::<MetaState>()
}

impl<S> From<Metadata<S>> for Tombstone
where
    S: Shared,
{
    fn from(val: Metadata<S>) -> Self {
        Self {
            crc: val.crc,
            file: val.file,
            offset: val.offset,
            len: val.len,
        }
    }
}

impl<W, S> Encode<W> for Metadata<S>
where
    W: Write,
    S: Shared,
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

impl<R, O> DecodeOwned<R, O> for Metadata<O::Shared>
where
    R: Read,
    O: Owned,
{
    fn decode_owned(reader: &mut R, buffer: &mut O) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let crc = u32::decode(reader)?;
        let file = u64::decode(reader)?;
        let offset = u64::decode(reader)?;
        let len = u64::decode(reader)?;
        let key = BinaryData::decode_owned(reader, buffer)?;
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

pub struct DeconstructIter(Vec<String>);

impl Iterator for DeconstructIter {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop()
    }
}

// We will need to have 2 layers:
// 1. The key metadata layer that holds [crc, file, offset, key] for each key
//    where the hash of the key is the index into the key metadata layer.
// 2. The data layer is where we store [crc, key length, key, value length, value]
pub struct KVStore<S>
where
    S: Shared,
{
    // store key metadata
    lookup: BTreeMap<BinaryData<S>, Entry<MmapBuffer>>,
    // meta file
    meta: FixedSizeAllocator<MmapBuffer>,
    // data files
    dequeue: Dequeue,
    // graveyard pusher
    graveyard_pusher: ring_buffer::Pusher<MmapBuffer>,

    // directories for data and meta
    data_path: String,
    meta_path: String,

    // max key size allowed
    max_key_size: usize,

    // tombstone pool
    tombstone_pool: PoolImpl,
}

// The expectation is that this is single threaded.
impl<S> KVStore<S>
where
    S: Shared,
{
    pub fn new<O>(
        meta_path: String,
        meta_size: u64,
        max_key_size: usize,
        data_path: String,
        node_size: u64,
        version: Version,
        pusher: ring_buffer::Pusher<MmapBuffer>,
        buffer: &mut O,
    ) -> Result<Self, Error>
    where
        O: Owned<Shared = S>,
    {
        let meta_path_saved = meta_path.clone();
        let data_path_saved = data_path.clone();

        let meta_block_size = metadata_block_size(max_key_size);

        let meta = MmapBuffer::new(meta_path, meta_size)?;
        let mut meta = FixedSizeAllocator::new(meta, meta_block_size)?;
        let dequeue = Dequeue::new(data_path, node_size, version)?;

        let mut lookup = BTreeMap::new();
        for entry in meta.recovered_entries()? {
            let Metadata { key, .. } = entry.data_owned(buffer)?;

            lookup.insert(key, entry);
        }

        Ok(Self {
            lookup,
            meta,
            dequeue,
            graveyard_pusher: pusher,

            data_path: data_path_saved.to_owned(),
            meta_path: meta_path_saved.to_owned(),

            max_key_size,

            tombstone_pool: PoolImpl::new(graveyard::TOMBSTONE_LEN, 1024 * 1024),
        })
    }

    pub fn deconstruct_iter(&self) -> DeconstructIter {
        // Get all the files in the data directory
        let mut files = std::fs::read_dir(&self.data_path)
            .expect("failed to read data directory")
            .map(|res| res.map(|e| format!("{:?}", e.path())))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .expect("failed to read data directory");

        files.push(self.meta_path.clone());

        DeconstructIter(files)
    }

    pub fn reconstruct(&self, file: impl AsRef<str>, contents: &[u8]) {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(file.as_ref())
            .expect("failed to open file");

        file.write_all(contents).expect("failed to write file");
    }

    pub fn delete<O>(&mut self, key: &BinaryData<S>, buffer: &mut O) -> Result<(), Error>
    where
        O: Owned<Shared = S>,
    {
        if let Some(mut entry) = self.lookup.remove(key) {
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone.
            let mut meta: Metadata<S> = entry.data_owned(buffer)?;

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

            let tombstone = self.tombstone_binary_data(meta);
            self.graveyard_pusher.push(tombstone)?;
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
    pub fn get<O>(&mut self, key: &BinaryData<S>, buf: &mut O) -> Result<Lookup<S>, Error>
    where
        O: Owned<Shared = S>,
    {
        if let Some(entry) = self.lookup.get(key) {
            let meta: Metadata<<O as Owned>::Shared> = entry.data_owned(buf)?;

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
    pub fn insert<O>(
        &mut self,
        key: BinaryData<S>,
        value: BinaryData<S>,
        buffer: &mut O,
    ) -> Result<(), Error>
    where
        O: Owned<Shared = S>,
    {
        if key.len() > self.max_key_size {
            return Err(Error::KeyTooLong {
                key: key.data().as_slice().to_vec(),
                max_key_length: self.max_key_size,
            });
        }

        // We need to tombstone old entry if it exists.
        if let Some(entry) = self.lookup.remove(&key) {
            let mut meta: Metadata<S> = entry.data_owned(buffer)?;

            meta.state = MetaState::Compacting;
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone.
            let tombstone = self.tombstone_binary_data(meta);
            self.graveyard_pusher.push(tombstone)?;
        }

        // Maybe we should have meta also be a ring buffer?
        let mut entry = self.meta.alloc()?;

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
            key: key.clone(),
        };

        entry.update(&metadata)?;

        self.lookup.insert(key, entry);

        Ok(())
    }

    fn tombstone_binary_data(&self, metadata: Metadata<S>) -> BinaryData<SharedImpl> {
        let tombstone: Tombstone = metadata.into();
        let mut buffer = self.tombstone_pool.acquire().unwrap();
        let mut buf = buffer.unfilled();
        tombstone.encode(&mut buf).unwrap();
        buffer.fill(graveyard::TOMBSTONE_LEN);
        let buffer = buffer.split_at(graveyard::TOMBSTONE_LEN);
        BinaryData::new(graveyard::TOMBSTONE_LEN, buffer.into_shared())
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use necronomicon::{binary_data, Pool, PoolImpl, SharedImpl};
    use tempfile::TempDir;

    use crate::{
        buffer::MmapBuffer,
        entry::Version,
        kv_store::{Graveyard, Lookup},
        ring_buffer::{ring_buffer, Popper},
    };

    use super::KVStore;

    #[test]
    fn test_put_get() {
        let temp_dir = tempfile::tempdir().unwrap();

        let pool = PoolImpl::new(1024, 1024);

        let (mut store, _) = test_kv_store(&temp_dir, &pool);

        let key = binary_data(b"pets");

        let mut owned = pool.acquire().unwrap();

        store
            .insert(key.clone(), binary_data(b"cats"), &mut owned)
            .expect("insert failed");

        let mut owned = pool.acquire().unwrap();
        let Lookup::Found(data) = store.get(&key, &mut owned).expect("key not found") else {
            panic!("key not found");
        };

        let actual = data.into_inner();
        assert_eq!(actual, binary_data(b"cats"));
    }

    #[test]
    fn test_put_get_delete() {
        let temp_dir = tempfile::tempdir().unwrap();

        let pool = PoolImpl::new(1024, 1024);

        let (mut store, _) = test_kv_store(&temp_dir, &pool);

        let key = binary_data(b"pets");

        let mut owned = pool.acquire().unwrap();

        store
            .insert(key.clone(), binary_data(b"cats"), &mut owned)
            .expect("insert failed");

        let mut owned = pool.acquire().unwrap();
        let Lookup::Found(data) = store.get(&key, &mut owned).expect("key not found") else {
            panic!("key not found");
        };

        let actual = data.into_inner();
        assert_eq!(actual, binary_data(b"cats"));

        let mut owned = pool.acquire().unwrap();
        store.delete(&key, &mut owned).expect("delete failed");

        let mut owned = pool.acquire().unwrap();
        let Lookup::Absent = store.get(&key, &mut owned).expect("key not found") else {
            panic!("key found");
        };
    }

    #[test]
    fn test_graveyard() {
        let temp_dir = tempfile::tempdir().unwrap();
        
        let pool = PoolImpl::new(1024, 1024);
        
        let (mut store, popper) = test_kv_store(&temp_dir, &pool);
        let path = temp_dir.into_path();

        let pclone = path.clone();
        let _ = std::thread::spawn(move || {
            let graveyard = Graveyard::new(pclone.join("data"), popper);
            graveyard.bury(1);
        });

        let key = binary_data(b"pets");

        let mut owned = pool.acquire().unwrap();
        store
            .insert(key.clone(), binary_data(b"cats"), &mut owned)
            .expect("insert failed");

        let mut owned = pool.acquire().unwrap();
        store
            .insert(key.clone(), binary_data(b"dogs"), &mut owned)
            .expect("insert failed");

        let mut owned = pool.acquire().unwrap();
        let Lookup::Found(data) = store.get(&key, &mut owned).expect("key not found") else {
            panic!("key not found");
        };

        let actual = data.into_inner();
        assert_eq!(actual, binary_data(b"dogs"));

        let mut owned = pool.acquire().unwrap();
        store.delete(&key, &mut owned).expect("delete failed");
        // Wait long enough for graveyard to run
        std::thread::sleep(std::time::Duration::from_secs(5));
        // assert that the data folder is empty
        let mut owned = pool.acquire().unwrap();
        let Lookup::Absent = store.get(&key, &mut owned).expect("key not found") else {
            panic!("key not found");
        };

        // For debugging:
        // tree(&path);

        assert!(!std::path::Path::exists(&path.join("data").join("0.bin")));
        assert!(!std::path::Path::exists(&path.join("data").join("1.bin")));
    }

    fn test_kv_store(
        temp_dir: &TempDir,
        pool: &PoolImpl,
    ) -> (KVStore<SharedImpl>, Popper<MmapBuffer>) {
        let path = format!("{}", temp_dir.path().display());

        let mmap_path = path.clone() + "mmap.bin";
        let buffer = MmapBuffer::new(mmap_path, 1024).expect("mmap buffer failed");

        let (pusher, popper) = ring_buffer(buffer, Version::V1).expect("ring buffer failed");

        let meta_path = path.clone() + "meta.bin";

        let data_path = path + "data.bin";

        let mut owned = pool.acquire().unwrap();

        let store = KVStore::new(
            meta_path,
            1024,
            32,
            data_path,
            1024,
            Version::V1,
            pusher,
            &mut owned,
        )
        .expect("KVStore::new failed");

        (store, popper)
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
