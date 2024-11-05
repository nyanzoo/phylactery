use std::{
    collections::VecDeque,
    io::{Read, Write},
    thread::JoinHandle,
    time::Duration,
};

use ::log::{error, trace};
use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
pub use data::Config as DataConfig;
use hashring::HashRing;
pub use meta::Config as MetaConfig;
use necronomicon::{
    deque_codec::{
        Create, CreateAck, Delete as DequeDelete, DeleteAck as DequeDeleteAck, Dequeue, DequeueAck,
        Enqueue, EnqueueAck, Peek, PeekAck,
    },
    kv_store_codec::{Delete, DeleteAck, Get, GetAck, Put, PutAck},
    Ack, ByteStr, Decode, Encode, Header, Pool as _, PoolImpl, SharedImpl, KEY_DOES_NOT_EXIST,
    QUEUE_EMPTY,
};

mod cache;

mod graveyard;

mod data;
mod error;
pub use error::Error;
mod log;
mod meta;
mod metadata;
mod store;

// So for store transfer here is what we can do:
// 1. we can make an mmap where we store the transaction #, a byte for lookup of the store,
//    and then a list of bytes for all the files that were modified.
//    we need to make sure we can store metadata files and data files
// 2. these could be fixed sized and then be a ring buffer and then just
//    iterate through where the transaction numbers are different
//
// alternatively
// 1. we have a mmap that maps all files to their latest transaction number
//    because if we send the full file it doesn't really matter we send...
//    the problem is that we might have a lot of files... so we need to be prepared
//    for the max number of files present across all data structures...
// maybe we can store the differences of files and then read the candidate version and compare with tail to know what to send?

const REPLICA_COUNT: usize = 10;

#[derive(Copy, Clone, Debug, Hash, PartialEq)]
struct VNode {
    shard: usize,
    id: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Config {
    /// The directory where the store will be stored.
    pub dir: String,
    /// The number of shards to use for the metadata & data store.
    pub shards: usize,
    /// The configuration for the metadata store.
    pub meta_store: MetaConfig,
    /// The configuration for the data store.
    pub data_store: DataConfig,
    /// The configuration for the pool that manages the data store.
    pub pool: PoolConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PoolConfig {
    /// The size of each block in the pool. This is the granularity of the pool.
    pub block_size: usize,
    /// The number of blocks in the pool.
    pub capacity: usize,
}

#[derive(Clone, Debug)]
pub enum Request {
    // Deque
    Create(Create<SharedImpl>),
    Remove(DequeDelete<SharedImpl>),
    Push(Enqueue<SharedImpl>),
    Pop(Dequeue<SharedImpl>),
    Peek(Peek<SharedImpl>),

    // Store
    Delete(Delete<SharedImpl>),
    Get(Get<SharedImpl>),
    Put(Put<SharedImpl>),
}

#[derive(Clone, Debug)]
pub enum Response {
    // Deque
    Create(CreateAck<SharedImpl>),
    Remove(DequeDeleteAck<SharedImpl>),
    Push(EnqueueAck<SharedImpl>),
    Pop(DequeueAck<SharedImpl>),
    Peek(PeekAck<SharedImpl>),

    // Store
    Delete(DeleteAck<SharedImpl>),
    Get(GetAck<SharedImpl>),
    Put(PutAck<SharedImpl>),
}

impl Ack<SharedImpl> for Response {
    fn header(&self) -> &Header {
        match self {
            // Deque
            Response::Create(response) => response.header(),
            Response::Remove(response) => response.header(),
            Response::Push(response) => response.header(),
            Response::Pop(response) => response.header(),
            Response::Peek(response) => response.header(),

            // Store
            Response::Delete(response) => response.header(),
            Response::Get(response) => response.header(),
            Response::Put(response) => response.header(),
        }
    }

    fn response(&self) -> necronomicon::Response<SharedImpl> {
        match self {
            // Deque
            Response::Create(response) => response.response(),
            Response::Remove(response) => response.response(),
            Response::Push(response) => response.response(),
            Response::Pop(response) => response.response(),
            Response::Peek(response) => response.response(),

            // Store
            Response::Delete(response) => response.response(),
            Response::Get(response) => response.response(),
            Response::Put(response) => response.response(),
        }
    }
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
                Ok(ref request) => {
                    let store = match request {
                        // Deque
                        Request::Create(request) => {
                            let store = self
                                .hasher
                                .get(request.path())
                                .copied()
                                .expect("no stores")
                                .shard;
                            &mut self.stores[store]
                        }
                        Request::Remove(request) => {
                            let store = self
                                .hasher
                                .get(request.path())
                                .copied()
                                .expect("no stores")
                                .shard;
                            &mut self.stores[store]
                        }
                        Request::Push(request) => {
                            let store = self
                                .hasher
                                .get(request.path())
                                .copied()
                                .expect("no stores")
                                .shard;
                            &mut self.stores[store]
                        }
                        Request::Pop(request) => {
                            let store = self
                                .hasher
                                .get(request.path())
                                .copied()
                                .expect("no stores")
                                .shard;
                            &mut self.stores[store]
                        }
                        Request::Peek(request) => {
                            let store = self
                                .hasher
                                .get(request.path())
                                .copied()
                                .expect("no stores")
                                .shard;
                            &mut self.stores[store]
                        }
                        // Store
                        Request::Delete(request) => {
                            let store = self
                                .hasher
                                .get(request.key())
                                .copied()
                                .expect("no stores")
                                .shard;
                            &mut self.stores[store]
                        }
                        Request::Get(request) => {
                            let store = self
                                .hasher
                                .get(request.key())
                                .copied()
                                .expect("no stores")
                                .shard;
                            &mut self.stores[store]
                        }
                        Request::Put(request) => {
                            let store = self
                                .hasher
                                .get(request.key())
                                .copied()
                                .expect("no stores")
                                .shard;
                            &mut self.stores[store]
                        }
                    };

                    store.requests.send(request.clone()).expect("send request");
                }

                // Errors
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

                    // Sleep? or maybe just yield?
                    std::thread::sleep(Duration::from_millis(10));
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
    let (requests_tx, requests_rx) = unbounded();
    let (responses_tx, responses_rx) = unbounded();
    let handle = std::thread::spawn(move || {
        let mut store = {
            self::store::Store::new(&config).unwrap_or_else(|err| {
                panic!("failed to create store at {:?} due to {err}", config.dir)
            })
        };

        // TODO: make constants?
        // We can use a small pool for errors and should be fine?
        let err_pool = PoolImpl::new(128, 1024);

        let mut responses = VecDeque::new();
        let store = &mut store;
        loop {
            // TODO: maybe handle error responses more gracefully?
            //       for now we will just go to a limit and then send them out.
            if responses.len() > 100 {
                for response in responses.drain(..) {
                    responses_tx.send(response).expect("send response");
                }
            }
            match requests_rx.try_recv() {
                // Deque
                Ok(Request::Create(request)) => {
                    let mut owned = err_pool.acquire(BufferOwner::Error);
                    let result = store
                        .create_deque(
                            request.path().clone(),
                            request.node_size(),
                            request.max_disk_usage(),
                        )
                        .map_err(|err| necronomicon::Response {
                            code: necronomicon::INTERNAL_ERROR,
                            reason: Some(
                                ByteStr::from_owned(err.to_string(), &mut owned)
                                    .expect("err reason"),
                            ),
                        });
                    let ack = match result {
                        Ok(_) => request.ack(),
                        Err(necronomicon::Response { code, reason }) => request.nack(code, reason),
                    };
                    responses.push_back(Response::Create(ack));
                }
                Ok(Request::Remove(request)) => {
                    let mut owned = err_pool.acquire(BufferOwner::Error);
                    let result = store.delete_deque(request.path().clone()).map_err(|err| {
                        necronomicon::Response {
                            code: necronomicon::INTERNAL_ERROR,
                            reason: Some(
                                ByteStr::from_owned(err.to_string(), &mut owned)
                                    .expect("err reason"),
                            ),
                        }
                    });
                    let ack = match result {
                        Ok(_) => request.ack(),
                        Err(necronomicon::Response { code, reason }) => request.nack(code, reason),
                    };
                    responses.push_back(Response::Remove(ack));
                }
                Ok(Request::Push(request)) => {
                    let mut owned = err_pool.acquire(BufferOwner::Error);
                    let result = store
                        .push_back(request.path().clone(), request.value().clone())
                        .map_err(|err| necronomicon::Response {
                            code: necronomicon::INTERNAL_ERROR,
                            reason: Some(
                                ByteStr::from_owned(err.to_string(), &mut owned)
                                    .expect("err reason"),
                            ),
                        });
                    let ack = match result {
                        Ok(_) => request.ack(),
                        Err(necronomicon::Response { code, reason }) => request.nack(code, reason),
                    };
                    responses.push_back(Response::Push(ack));
                }
                Ok(Request::Pop(request)) => {
                    let mut owned = pool.acquire(BufferOwner::PopFront);
                    let mut owned_err = err_pool.acquire(BufferOwner::Error);
                    let result =
                        store
                            .pop_front(request.path().clone(), &mut owned)
                            .map_err(|err| necronomicon::Response {
                                code: necronomicon::INTERNAL_ERROR,
                                reason: Some(
                                    ByteStr::from_owned(err.to_string(), &mut owned_err)
                                        .expect("err reason"),
                                ),
                            });
                    let ack = match result {
                        Ok(value) => match value {
                            Some(value) => request.ack(value.into_inner()),
                            None => request.nack(QUEUE_EMPTY, None),
                        },
                        Err(necronomicon::Response { code, reason }) => request.nack(code, reason),
                    };
                    responses.push_back(Response::Pop(ack));
                }
                Ok(Request::Peek { .. }) => {
                    unimplemented!("peek")
                    // let mut owned = err_pool.acquire(BufferOwner::Error);
                    // let result = store
                    //     .peek(dir, &pool)
                    //     .map_err(|err| necronomicon::Response {
                    //         code: necronomicon::INTERNAL_ERROR,
                    //         reason: Some(
                    //             ByteStr::from_owned(err.to_string(), &mut owned)
                    //                 .expect("err reason"),
                    //         ),
                    //     });
                    // responses.push_back(Response::Peek { result });
                }

                // Store
                Ok(Request::Delete(request)) => {
                    let mut owned = err_pool.acquire(BufferOwner::Error);
                    let result =
                        store
                            .delete(request.key().clone())
                            .map_err(|err| necronomicon::Response {
                                code: necronomicon::INTERNAL_ERROR,
                                reason: Some(
                                    ByteStr::from_owned(err.to_string(), &mut owned)
                                        .expect("err reason"),
                                ),
                            });
                    let ack = match result {
                        Ok(_) => request.ack(),
                        Err(necronomicon::Response { code, reason }) => request.nack(code, reason),
                    };
                    responses.push_back(Response::Delete(ack));
                }
                Ok(Request::Get(request)) => {
                    let mut owned = pool.acquire(BufferOwner::Get);
                    let mut owned_err = err_pool.acquire(BufferOwner::Error);
                    let result = store.get(request.key().clone(), &mut owned).map_err(|err| {
                        necronomicon::Response {
                            code: necronomicon::INTERNAL_ERROR,
                            reason: Some(
                                ByteStr::from_owned(err.to_string(), &mut owned_err)
                                    .expect("err reason"),
                            ),
                        }
                    });
                    let ack = match result {
                        Ok(value) => match value {
                            Some(value) => request.ack(value.into_inner()),
                            None => request.nack(KEY_DOES_NOT_EXIST, None),
                        },
                        Err(necronomicon::Response { code, reason }) => request.nack(code, reason),
                    };
                    responses.push_back(Response::Get(ack));
                }
                Ok(Request::Put(request)) => {
                    let mut owned = err_pool.acquire(BufferOwner::Error);
                    // NOTE: double check the bytestr for the err. If it is good,
                    // then it means that the decode error is from something else.
                    let result = store
                        .put(request.key().clone(), request.value().clone())
                        .map_err(|err| necronomicon::Response {
                            code: necronomicon::INTERNAL_ERROR,
                            reason: Some(
                                ByteStr::from_owned(err.to_string(), &mut owned)
                                    .expect("err reason"),
                            ),
                        });
                    let ack = match result {
                        Ok(_) => request.ack(),
                        Err(necronomicon::Response { code, reason }) => {
                            trace!("put nack: {} {:?}", code, reason);
                            request.nack(code, reason)
                        }
                    };
                    responses.push_back(Response::Put(ack));
                }

                // Errors
                Err(TryRecvError::Disconnected) => {
                    error!("store at {:?} disconnected", &config.dir);
                    return;
                }
                Err(TryRecvError::Empty) => {
                    for response in responses.drain(..) {
                        responses_tx.send(response).expect("send response");
                    }

                    // Sleep? or something?
                    std::thread::sleep(Duration::from_millis(10));
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
    use crossbeam::channel::bounded;
    use necronomicon::{Ack, BinaryData, SUCCESS};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn store_put_get_full() {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let dir3 = tempdir().unwrap();
        let dir4 = tempdir().unwrap();
        let configs = vec![
            Config {
                dir: dir1.path().to_str().unwrap().to_string(),
                shards: 100,
                meta_store: MetaConfig::test(0x4000 * 0x1000),
                data_store: DataConfig::test(0x4000, 0x8000 * 0x1000),
                pool: PoolConfig {
                    block_size: 0x4000,
                    capacity: 0x1000,
                },
            },
            Config {
                dir: dir2.path().to_str().unwrap().to_string(),
                shards: 100,
                meta_store: MetaConfig::test(0x4000 * 0x1000),
                data_store: DataConfig::test(0x4000, 0x8000 * 0x1000),
                pool: PoolConfig {
                    block_size: 0x4000,
                    capacity: 0x1000,
                },
            },
            Config {
                dir: dir3.path().to_str().unwrap().to_string(),
                shards: 100,
                meta_store: MetaConfig::test(0x4000 * 0x1000),
                data_store: DataConfig::test(0x4000, 0x8000 * 0x1000),
                pool: PoolConfig {
                    block_size: 0x4000,
                    capacity: 0x1000,
                },
            },
            Config {
                dir: dir4.path().to_str().unwrap().to_string(),
                shards: 100,
                meta_store: MetaConfig::test(0x4000 * 0x1000),
                data_store: DataConfig::test(0x4000, 0x8000 * 0x1000),
                pool: PoolConfig {
                    block_size: 0x4000,
                    capacity: 0x1000,
                },
            },
        ];
        let (requests_tx, requests_rx) = bounded(1024);
        let (responses_tx, responses_rx) = bounded(1024);

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
            requests_tx
                .send(Request::Put(Put::new(1, 1, key, value)))
                .unwrap();
        }

        let responses = responses_rx.iter().take(100_000);
        for response in responses {
            match response {
                Response::Put(ack) => {
                    assert!(ack.response().code() == SUCCESS);
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
            requests_tx.send(Request::Get(Get::new(1, 1, key))).unwrap();
        }

        for response in responses_rx.iter().take(100_000) {
            match response {
                Response::Get(ack) => {
                    assert!(ack.response().code() == SUCCESS);
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

    #[test]
    fn store_deque_full() {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let dir3 = tempdir().unwrap();
        let dir4 = tempdir().unwrap();
        let configs = vec![
            Config {
                dir: dir1.path().to_str().unwrap().to_string(),
                shards: 100,
                meta_store: MetaConfig::test(0x4000 * 0x1000),
                data_store: DataConfig::test(0x4000, 0x8000 * 0x1000),
                pool: PoolConfig {
                    block_size: 0x4000,
                    capacity: 0x1000,
                },
            },
            Config {
                dir: dir2.path().to_str().unwrap().to_string(),
                shards: 100,
                meta_store: MetaConfig::test(0x4000 * 0x1000),
                data_store: DataConfig::test(0x4000, 0x8000 * 0x1000),
                pool: PoolConfig {
                    block_size: 0x4000,
                    capacity: 0x1000,
                },
            },
            Config {
                dir: dir3.path().to_str().unwrap().to_string(),
                shards: 100,
                meta_store: MetaConfig::test(0x4000 * 0x1000),
                data_store: DataConfig::test(0x4000, 0x8000 * 0x1000),
                pool: PoolConfig {
                    block_size: 0x4000,
                    capacity: 0x1000,
                },
            },
            Config {
                dir: dir4.path().to_str().unwrap().to_string(),
                shards: 100,
                meta_store: MetaConfig::test(0x4000 * 0x1000),
                data_store: DataConfig::test(0x4000, 0x8000 * 0x1000),
                pool: PoolConfig {
                    block_size: 0x4000,
                    capacity: 0x1000,
                },
            },
        ];
        let (requests_tx, requests_rx) = bounded(1024);
        let (responses_tx, responses_rx) = bounded(1024);

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

        let path = ByteStr::new(BinaryData::new(SharedImpl::test_new(b"kittens")));
        requests_tx
            .send(Request::Create(Create::new(
                1,
                1,
                path.clone(),
                1024 * 1024,
                1024 * 8096,
            )))
            .unwrap();
        let response = responses_rx.recv().unwrap();
        match response {
            Response::Create(ack) => {
                assert_eq!(ack.response().code(), SUCCESS);
            }
            _ => panic!("unexpected response"),
        }

        println!("create done");
        let now = std::time::Instant::now();
        for i in 0..100_000 {
            let value = BinaryData::new(SharedImpl::test_new(format!("value-{}", i).as_bytes()));
            requests_tx
                .send(Request::Push(Enqueue::new(1, 1, path.clone(), value)))
                .unwrap();
        }

        println!("push done");
        let responses = responses_rx.iter().take(100_000);
        for response in responses {
            match response {
                Response::Push(ack) => {
                    assert_eq!(ack.response().code(), SUCCESS);
                }
                _ => panic!("unexpected response"),
            }
        }

        let elapsed = now.elapsed();
        println!("100,000 push: {:?}", elapsed);
        // crate::store::tree(&dir_path);

        let now = std::time::Instant::now();
        for _ in 0..100_000 {
            requests_tx
                .send(Request::Pop(Dequeue::new(1, 1, path.clone())))
                .unwrap();
        }

        let mut i = 0;
        let responses = responses_rx.iter().take(100_000);
        for response in responses {
            match response {
                Response::Pop(ack) => {
                    assert_eq!(ack.response().code(), SUCCESS);
                    let value =
                        BinaryData::new(SharedImpl::test_new(format!("value-{}", i).as_bytes()));
                    assert_eq!(ack.value().expect("some").clone(), value);
                    // assert_eq!(
                    //     data.into_inner().data().as_slice(),
                    //     format!("value-{}", i).as_bytes()
                    // );
                }
                _ => panic!("unexpected response"),
            }
            i += 1;
        }

        let elapsed = now.elapsed();
        println!("{i} pop: {:?}", elapsed);
        drop(handle);
    }
}
