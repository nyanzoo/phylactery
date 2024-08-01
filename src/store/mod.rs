use std::{
    collections::VecDeque,
    io::{Read, Write},
    sync::{
        mpsc::{channel, Receiver, Sender, TryRecvError},
        Arc,
    },
    thread::JoinHandle,
};

use hashring::HashRing;
use log::error;
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

const REPLICA_COUNT: usize = 10;

#[derive(Copy, Clone, Debug, Hash, PartialEq)]
struct VNode {
    shard: usize,
    id: usize,
}

pub struct Config {
    pub dir: String,
    pub shards: usize,
    pub data_len: u64,
    pub meta_len: u64,
    pub max_disk_usage: u64,
    pub block_size: usize,
    pub capacity: usize,
}

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

pub enum InnerResponse {
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

pub enum Response {
    Delete {
        result: Result<bool, String>,
    },
    Get {
        result: Result<Option<Readable<SharedImpl>>, String>,
    },
    Put {
        result: Result<bool, String>,
    },
}

pub struct Store {
    stores: Vec<StoreLoop>,
    requests: Receiver<Request>,
    responses: Sender<Response>,
    hasher: HashRing<VNode>,
}

impl Store {
    pub fn new(
        configs: Vec<Config>,
        requests: Receiver<Request>,
        responses: Sender<Response>,
        pool: PoolImpl,
    ) -> Result<Self, error::Error> {
        let mut hasher = HashRing::new();
        let mut stores = vec![];
        for (i, config) in configs.into_iter().enumerate() {
            for id in 0..REPLICA_COUNT {
                hasher.add(VNode { shard: i, id });
            }
            let store = store_loop(config, pool.clone());
            stores.push(store);
        }
        Ok(Self {
            stores,
            requests,
            responses,
            hasher,
        })
    }

    pub fn run(mut self) {
        loop {
            match self.requests.try_recv() {
                Ok(Request::Delete { key }) => {
                    let store = self.hasher.get(&key).copied().expect("no stores").shard;
                    let store = &mut self.stores[store];
                    store
                        .requests
                        .send(Request::Delete { key })
                        .expect("send request");
                }
                Ok(Request::Get { key }) => {
                    let store = self.hasher.get(&key).copied().expect("no stores").shard;
                    let store = &mut self.stores[store];
                    store
                        .requests
                        .send(Request::Get { key })
                        .expect("send request");
                }
                Ok(Request::Put { key, value }) => {
                    let store = self.hasher.get(&key).copied().expect("no stores").shard;
                    let store = &mut self.stores[store];
                    store
                        .requests
                        .send(Request::Put { key, value })
                        .expect("send request");
                }
                Err(TryRecvError::Disconnected) => {
                    error!("store disconnected");
                    return;
                }
                Err(TryRecvError::Empty) => {
                    let mut responses = VecDeque::new();
                    'response: loop {
                        let mut has_some = false;
                        for store in &self.stores {
                            match store.responses.try_recv() {
                                Ok(response) => {
                                    has_some = true;
                                    responses.push_back(response)
                                }
                                Err(TryRecvError::Disconnected) => {
                                    error!("store disconnected");
                                    return;
                                }
                                Err(TryRecvError::Empty) => {}
                            }
                        }

                        if has_some {
                            continue 'response;
                        }

                        for response in responses.drain(..) {
                            self.responses.send(response).expect("send response");
                        }

                        break 'response;
                    }
                }
            }
        }
    }
}

struct StoreLoop {
    requests: Sender<Request>,
    responses: Receiver<Response>,
    handle: JoinHandle<()>,
}

fn store_loop(config: Config, pool: PoolImpl) -> StoreLoop {
    let (requests_tx, requests_rx) = channel();
    let (responses_tx, responses_rx) = channel();
    let handle = std::thread::spawn(move || {
        let Config {
            dir,
            data_len,
            meta_len,
            shards,
            max_disk_usage,
            block_size,
            capacity,
        } = config;
        let mut store = {
            let pool = PoolImpl::new(block_size, capacity);

            self::store::Store::new(
                dir.clone(),
                shards,
                data_len,
                meta_len,
                max_disk_usage,
                pool,
            )
            .expect(&format!("failed to create store at {:?}", dir))
        };

        let mut responses = VecDeque::new();
        loop {
            match requests_rx.try_recv() {
                Ok(Request::Delete { key }) => {
                    let result = store.delete(key);
                    responses.push_back(InnerResponse::Delete { result });
                }
                Ok(Request::Get { key }) => {
                    let mut owned = pool.acquire(BufferOwner::Get);
                    let result = store.get(key, &mut owned);
                    responses.push_back(InnerResponse::Get { result });
                }
                Ok(Request::Put { key, value }) => {
                    let result = store.put(key, value);
                    responses.push_back(InnerResponse::Put { result });
                }
                Err(TryRecvError::Disconnected) => {
                    error!("store at {:?} disconnected", dir);
                    return;
                }
                Err(TryRecvError::Empty) => {
                    for mut response in responses.drain(..) {
                        match &mut response {
                            InnerResponse::Delete { result } => {
                                if let Ok(Some(delete)) = result {
                                    delete.commit().expect("failed to commit");
                                }

                                responses_tx
                                    .send(Response::Delete {
                                        result: result
                                            .as_ref()
                                            .map(|x| x.is_some())
                                            .map_err(|e| e.to_string()),
                                    })
                                    .expect("send response");
                            }
                            InnerResponse::Get { result } => {
                                let result = match result {
                                    Ok(Some(readable)) => Ok(Some(readable.clone())),
                                    Ok(None) => Ok(None),
                                    Err(e) => Err(e.to_string()),
                                };

                                responses_tx
                                    .send(Response::Get { result })
                                    .expect("send response");
                            }
                            InnerResponse::Put { result } => {
                                let result = match result {
                                    Ok(maybe) => {
                                        if let Some(put) = maybe.take() {
                                            put.commit().expect("failed to commit");
                                            Ok(true)
                                        } else {
                                            Ok(false)
                                        }
                                    }
                                    Err(e) => Err(e.to_string()),
                                };

                                responses_tx
                                    .send(Response::Put { result })
                                    .expect("send response");
                            }
                        }
                    }
                }
            }
        }
    });

    StoreLoop {
        requests: requests_tx,
        responses: responses_rx,
        handle,
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

#[cfg(test)]
mod tests {
    use necronomicon::Pool;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn store_put_get() {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let dir3 = tempdir().unwrap();
        let dir4 = tempdir().unwrap();
        let configs = vec![
            Config {
                dir: dir1.path().to_str().unwrap().to_string(),
                shards: 100,
                data_len: 0x4000,
                meta_len: 0x4000 * 0x1000,
                max_disk_usage: 0x8000 * 0x1000,
                block_size: 0x4000,
                capacity: 0x1000,
            },
            Config {
                dir: dir2.path().to_str().unwrap().to_string(),
                shards: 100,
                data_len: 0x4000,
                meta_len: 0x4000 * 0x1000,
                max_disk_usage: 0x8000 * 0x1000,
                block_size: 0x4000,
                capacity: 0x1000,
            },
            Config {
                dir: dir3.path().to_str().unwrap().to_string(),
                shards: 100,
                data_len: 0x4000,
                meta_len: 0x4000 * 0x1000,
                max_disk_usage: 0x8000 * 0x1000,
                block_size: 0x4000,
                capacity: 0x1000,
            },
            Config {
                dir: dir4.path().to_str().unwrap().to_string(),
                shards: 100,
                data_len: 0x4000,
                meta_len: 0x4000 * 0x1000,
                max_disk_usage: 0x8000 * 0x1000,
                block_size: 0x4000,
                capacity: 0x1000,
            },
        ];
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (responses_tx, responses_rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            let store = Store::new(
                configs,
                requests_rx,
                responses_tx,
                PoolImpl::new(0x8000, 0x8000),
            )
            .unwrap();
            store.run();
        });

        let now = std::time::Instant::now();
        for i in 0..100_000 {
            let key = BinaryData::new(SharedImpl::test_new(format!("key-{}", i).as_bytes()));
            let value = BinaryData::new(SharedImpl::test_new(format!("value-{}", i).as_bytes()));
            requests_tx.send(Request::Put { key, value }).unwrap();
        }

        let responses = responses_rx.iter().take(100_000);
        for response in responses {
            match response {
                Response::Put { result } => {
                    assert!(result.is_ok());
                    assert!(result.unwrap());
                }
                _ => panic!("unexpected response"),
            }
        }

        let elapsed = now.elapsed();
        println!("100,000 put: {:?}", elapsed);
        // crate::store::tree(&dir_path);

        let random_range = rand::seq::index::sample(&mut rand::thread_rng(), 100_000, 100_000);
        let now = std::time::Instant::now();
        for i in random_range {
            let key = BinaryData::new(SharedImpl::test_new(format!("key-{}", i).as_bytes()));
            requests_tx.send(Request::Get { key }).unwrap();
        }

        for response in responses_rx.iter().take(100_000) {
            match response {
                Response::Get { result } => {
                    let data = result.unwrap().unwrap();
                    // assert_eq!(
                    //     data.into_inner().data().as_slice(),
                    //     format!("value-{}", i).as_bytes()
                    // );
                }
                _ => panic!("unexpected response"),
            }
        }

        let elapsed = now.elapsed();
        println!("100,000 get: {:?}", elapsed);
        drop(handle);
    }
}
