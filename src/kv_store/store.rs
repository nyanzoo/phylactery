use std::{collections::BTreeMap, io::Write};

use necronomicon::{BinaryData, Encode, Owned, Shared};

use crate::{
    alloc::{Entry, FixedSizeAllocator},
    buffer::MmapBuffer,
    dequeue::{Dequeue, Push},
    entry::Readable,
    ring_buffer, Error,
};

use super::{
    config::{self, Config},
    graveyard::Tombstone,
    metadata::{metadata_block_size, MetadataRead, MetadataWrite},
    MetaState,
};

#[derive(Debug, Eq, PartialEq)]
pub enum Lookup<S>
where
    S: Shared,
{
    Absent,
    Found(Readable<S>),
}

pub struct DeconstructIter {
    files: Vec<String>,
    curr_file_offset: u64,
    chunk_size: u64,
    file_size: u64,
}

impl Iterator for DeconstructIter {
    type Item = (String, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_file_offset == self.file_size {
            self.files.pop();
        }
        if let Some(file) = self.files.pop() {
            let offset = self.curr_file_offset;
            self.curr_file_offset += self.chunk_size;
            self.curr_file_offset = std::cmp::min(self.curr_file_offset, self.file_size);
            Some((file, offset))
        } else {
            None
        }
    }
}

// We will need to have 2 layers:
// 1. The key metadata layer that holds [crc, file, offset, key] for each key
//    where the hash of the key is the index into the key metadata layer.
// 2. The data layer is where we store [crc, key length, key, value length, value]
pub struct Store<S>
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
    // max file size
    max_file_size: u64,
}

// The expectation is that this is single threaded.
impl<S> Store<S>
where
    S: Shared,
{
    pub fn new<O>(
        config: Config,
        pusher: ring_buffer::Pusher<MmapBuffer>,
        buffer: &mut O,
    ) -> Result<Self, Error>
    where
        O: Owned<Shared = S>,
    {
        let Config {
            path,
            meta:
                config::Metadata {
                    max_disk_usage,
                    max_key_size,
                },
            data: config::Data { node_size },
            version,
        } = config;
        let meta_path = format!("{}/meta.bin", path);
        let data_path = format!("{}/data", path);

        let meta_block_size = metadata_block_size(max_key_size);

        let meta = MmapBuffer::new(meta_path.clone(), max_disk_usage)?;
        let mut meta = FixedSizeAllocator::new(meta, meta_block_size)?;
        let dequeue = Dequeue::new(data_path.clone(), node_size, version)?;

        let mut lookup = BTreeMap::new();
        for entry in meta.recovered_entries()? {
            let MetadataRead { key, .. } = entry.data_owned(buffer)?;

            lookup.insert(key, entry);
        }

        Ok(Self {
            lookup,
            meta,
            dequeue,
            graveyard_pusher: pusher,

            data_path: data_path.to_owned(),
            meta_path: meta_path.to_owned(),

            max_key_size,
            max_file_size: node_size,
        })
    }

    pub fn deconstruct_iter(&self, chunk_size: u64) -> DeconstructIter {
        // Get all the files in the data directory
        let mut files = std::fs::read_dir(&self.data_path)
            .expect("failed to read data directory")
            .map(|res| res.map(|e| format!("{:?}", e.path())))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .expect("failed to read data directory");

        files.push(self.meta_path.clone());

        DeconstructIter{
            files,
            curr_file_offset: 0,
            chunk_size,
            file_size: self.max_file_size,
        }
    }

    pub fn reconstruct(&self, file: impl AsRef<str>, contents: &[u8]) {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
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
            let mut meta: MetadataRead<S> = entry.data_owned(buffer)?;

            match meta.state {
                MetaState::Full => {}
                MetaState::Compacting => {
                    // Remove bad key!
                    self.lookup.remove(key);
                    return Err(Error::KeyNotFound(format!("{:?}", key)));
                }
            }

            meta.state = MetaState::Compacting;

            entry.update(&MetadataWrite::from(&meta))?;
            // needs to be tombstone!

            let mut buf = vec![];
            Tombstone::from(meta).encode(&mut buf)?;
            let _ = self.graveyard_pusher.push(&buf)?;
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
            let meta: MetadataRead<<O as Owned>::Shared> = entry.data_owned(buf)?;

            let MetadataRead {
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
        value: &[u8],
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
            let mut meta: MetadataRead<S> = entry.data_owned(buffer)?;

            meta.state = MetaState::Compacting;
            // Need to use the push-side of the ring buffer for graveyard.
            // We also need to make sure we set the flag for tombstone.
            let mut buf = vec![];
            Tombstone::from(meta).encode(&mut buf)?;
            self.graveyard_pusher.push(&buf)?;
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
        let metadata = MetadataWrite {
            crc,
            file,
            offset,
            len,
            state: MetaState::Full,
            key: key.data().as_slice(),
        };

        entry.update(&metadata)?;

        self.lookup.insert(key, entry);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{io::Write, path::Path};

    use necronomicon::{binary_data, Pool, PoolImpl, SharedImpl};
    use tempfile::TempDir;

    use crate::{
        buffer::MmapBuffer,
        entry::Version,
        kv_store::{
            config::{self, Config},
            store::Lookup,
            Graveyard,
        },
        ring_buffer::{ring_buffer, Popper},
    };

    use super::Store;

    #[test]
    fn test_put_get() {
        let temp_dir = tempfile::tempdir().unwrap();

        let pool = PoolImpl::new(1024, 1024);

        let (mut store, _) = test_kv_store(&temp_dir, &pool);

        let key = binary_data(b"pets");

        let mut owned = pool.acquire().unwrap();

        store
            .insert(key.clone(), b"cats", &mut owned)
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
            .insert(key.clone(), b"cats", &mut owned)
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
        let temp_path = temp_dir.path();
        let pool = PoolImpl::new(1024, 1024);

        let (mut store, popper) = test_kv_store(&temp_dir, &pool);
        let path = format!("{}", temp_path.display());

        let pclone = path.clone();
        let _ = std::thread::spawn(move || {
            let graveyard = Graveyard::new(format!("{}/data", pclone).into(), popper);
            graveyard.bury(1);
        });

        let key = binary_data(b"pets");
        let mut owned = pool.acquire().unwrap();
        store
            .insert(key.clone(), b"cats", &mut owned)
            .expect("insert failed");

        let mut owned = pool.acquire().unwrap();
        store
            .insert(key.clone(), b"dogs", &mut owned)
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
        // tree(&Path::new(&path));
        // hexyl(format!("{}/meta.bin", path).as_ref());

        let path = format!("{}/data", path);
        let path0 = Path::new(&path).join("0.bin");
        let path1 = Path::new(&path).join("1.bin");
        assert!(!std::path::Path::exists(&path0));
        assert!(!std::path::Path::exists(&path1));
    }

    fn test_kv_store(
        temp_dir: &TempDir,
        pool: &PoolImpl,
    ) -> (Store<SharedImpl>, Popper<MmapBuffer>) {
        let path = format!("{}", temp_dir.path().display());

        let mmap_path = format!("{}/graveyard.bin", path);
        let buffer = MmapBuffer::new(mmap_path, 1024).expect("mmap buffer failed");

        let (pusher, popper) = ring_buffer(buffer, Version::V1).expect("ring buffer failed");

        let mut owned = pool.acquire().unwrap();

        let store = Store::new(
            Config {
                path,
                meta: config::Metadata {
                    max_disk_usage: 1024,
                    max_key_size: 32,
                },
                data: config::Data { node_size: 1024 },
                version: Version::V1,
            },
            pusher,
            &mut owned,
        )
        .expect("Store::new failed");

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
