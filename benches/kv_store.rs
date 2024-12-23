use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use crossbeam::channel::unbounded;
use necronomicon::{
    binary_data,
    kv_store_codec::{Delete, Get, Put},
    Ack, PoolImpl, SUCCESS,
};
use phylactery::store::{Config, DataConfig, MetaConfig, PoolConfig, Request, Response, Store};
use tempfile::tempdir;

pub fn put_get_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("KV Store PutGetDelete");

    let shards = 100;
    let meta_store = MetaConfig {
        size: "10 MiB".parse().unwrap(),
    };
    let data_store = DataConfig {
        node_size: "10 MiB".parse().unwrap(),
        max_disk_usage: "100 MiB".parse().unwrap(),
    };
    let pool = PoolConfig {
        block_size: "1 MiB".parse().unwrap(),
        capacity: 2048,
    };

    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();
    let dir4 = tempdir().unwrap();
    let configs = vec![
        Config {
            dir: dir1.path().to_str().unwrap().to_string(),
            shards,
            meta_store,
            data_store,
            pool,
        },
        Config {
            dir: dir2.path().to_str().unwrap().to_string(),
            shards,
            meta_store,
            data_store,
            pool,
        },
        Config {
            dir: dir3.path().to_str().unwrap().to_string(),
            shards,
            meta_store,
            data_store,
            pool,
        },
        Config {
            dir: dir4.path().to_str().unwrap().to_string(),
            shards,
            meta_store,
            data_store,
            pool,
        },
    ];
    let (requests_tx, requests_rx) = unbounded();
    let (responses_tx, responses_rx) = unbounded();
    let _ = std::thread::spawn(move || {
        let store = Store::new(
            configs,
            requests_rx,
            responses_tx,
            PoolImpl::new(0x8000, 0x8000),
        )
        .unwrap();
        store.run();
    });

    for i in (1u32..=10).into_iter().map(|i| 2u64.pow(i)) {
        group.bench_with_input(
            BenchmarkId::new("test 1 - push then get then delete", i),
            &i,
            |b, i| {
                let value = binary_data(&vec![7; *i as usize]);
                b.iter(|| {
                    let key = binary_data(b"cat");
                    let uuid = uuid::Uuid::new_v4().as_u128();
                    let put = Put::new(1, uuid, key.clone(), value.clone());
                    requests_tx.send(Request::Put(put)).expect("send failed");

                    let response = responses_rx.recv().expect("recv failed");
                    let Response::Put(ack) = response else {
                        panic!("put failed");
                    };
                    assert_eq!(ack.response().reason(), &None);
                    assert_eq!(ack.response().code(), SUCCESS);

                    let uuid = uuid::Uuid::new_v4().as_u128();
                    let get = Get::new(1, uuid, key.clone());
                    requests_tx.send(Request::Get(get)).expect("send failed");

                    let response = responses_rx.recv().expect("recv failed");
                    let Response::Get(get) = response else {
                        panic!("get failed");
                    };
                    assert_eq!(get.response().code(), SUCCESS);
                    assert_eq!(get.value(), Some(&value));

                    let uuid = uuid::Uuid::new_v4().as_u128();
                    let delete = Delete::new(1, uuid, key);
                    requests_tx
                        .send(Request::Delete(delete))
                        .expect("send failed");

                    let response = responses_rx.recv().expect("recv failed");
                    let Response::Delete(ack) = response else {
                        panic!("delete failed");
                    };
                    assert_eq!(ack.response().code(), SUCCESS);
                });
            },
        );
    }

    requests_tx.send(Request::Shutdown).expect("send failed");

    group.finish();
}

criterion_group!(kvstore, put_get_delete);
criterion_main!(kvstore);
