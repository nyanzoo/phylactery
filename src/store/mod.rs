use std::{
    io::{Read, Write},
    sync::mpsc::{Receiver, Sender},
};

use hashring::HashRing;
use necronomicon::{BinaryData, Decode, Encode, Pool as _, PoolImpl, SharedImpl};

use crate::entry::Readable;

mod cache;
pub mod config;

mod graveyard;

mod data;
mod error;
mod meta;
mod metadata;
mod store;

pub enum Request {
    Delete {
        key: BinaryData<SharedImpl>,
    },
    Get {
        key: BinaryData<SharedImpl>,
    },
    Put {
        key: BinaryData<SharedImpl>,
        value: BinaryData<SharedImpl>,
    },
}

pub enum Response {
    Delete {
        result: Result<Option<meta::shard::delete::Delete>, error::Error>,
    },
    Get {
        result: Result<Option<Readable<SharedImpl>>, error::Error>,
    },
    Put {
        result: Result<Option<store::Put>, error::Error>,
    },
}

pub struct Store {
    stores: Vec<store::Store>,
    requests: Receiver<Request>,
    responses: Sender<Response>,
    hasher: HashRing<usize>,
}

impl Store {
    pub fn run(mut self, pool: PoolImpl) -> ! {
        loop {
            match self.requests.recv() {
                Ok(Request::Delete { key }) => {
                    let store = self.hasher.get(&key).copied().expect("no stores");
                    let store = &mut self.stores[store];
                    let result = store.delete(key);
                    self.responses.send(Response::Delete { result }).unwrap();
                }
                Ok(Request::Get { key }) => {
                    let store = self.hasher.get(&key).copied().expect("no stores");
                    let store = &mut self.stores[store];
                    let mut owned = pool.acquire(BufferOwner::Get);
                    let result = store.get(key, &mut owned);
                    self.responses.send(Response::Get { result }).unwrap();
                }
                Ok(Request::Put { key, value }) => {
                    let store = self.hasher.get(&key).copied().expect("no stores");
                    let store = &mut self.stores[store];
                    let result = store.put(key, value);
                    self.responses.send(Response::Put { result }).unwrap();
                }
                Err(_) => break,
            }
        }
        std::process::exit(0);
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) enum MetaState {
    // We need to compact and cannot accept new metadata in this slot
    Compacting,
    // Has live data associated with it
    Full,
}

impl From<MetaState> for u8 {
    fn from(state: MetaState) -> Self {
        match state {
            MetaState::Compacting => 0,
            MetaState::Full => 1,
        }
    }
}

impl From<u8> for MetaState {
    fn from(byte: u8) -> Self {
        match byte {
            0 => MetaState::Compacting,
            1 => MetaState::Full,
            _ => panic!("invalid meta state"),
        }
    }
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

#[allow(dead_code)]
#[cfg(test)]
pub fn tree(path: &std::path::Path) {
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
#[cfg(test)]
pub fn hexyl(path: &std::path::Path) {
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
