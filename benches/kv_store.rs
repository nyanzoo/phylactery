use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use necronomicon::{binary_data, Pool, PoolImpl, Shared};
use phylactery::{
    buffer::MmapBuffer,
    entry::Version,
    kv_store::{
        config::{self, Config},
        Graveyard, Lookup, Store,
    },
    ring_buffer::ring_buffer,
};

pub fn put_get_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("KV Store PutGetDelete");

    let temp_dir = tempfile::tempdir().unwrap();
    let path = format!("{}", temp_dir.into_path().display());

    let mmap_path = format!("{}/graveyard.bin", path);
    let buffer = MmapBuffer::new(mmap_path, 1024 * 1024 * 100).expect("mmap buffer failed");

    let (pusher, popper) = ring_buffer(buffer, Version::V1).expect("ring buffer failed");

    let config = Config {
        path: path.clone(),
        meta: config::Metadata {
            max_disk_usage: 1024,
            max_key_size: 256,
        },
        data: config::Data { node_size: 4096 },
        version: Version::V1,
    };

    let pool = PoolImpl::new(1024, 1024);

    let mut store = Store::new(config, pusher).expect("KVStore::new failed");

    let pclone = path.clone();
    let _ = std::thread::spawn(move || {
        let graveyard = Graveyard::new(format!("{}/data/", pclone).into(), popper);
        graveyard.bury(1);
    });

    for i in (1u32..=10).into_iter().map(|i| 2u64.pow(i)) {
        group.bench_with_input(BenchmarkId::new("pgd", i), &i, |b, _i| {
            b.iter(|| {
                let key = binary_data(b"cat");
                store.insert(key.clone(), b"yes").expect("insert failed");

                let mut buf = pool.acquire("get").expect("acquire");
                let value = store.get(&key, &mut buf).expect("get failed");
                let Lookup::Found(value) = value else {
                    panic!("value not found");
                };

                assert_eq!(value.into_inner().data().as_slice(), b"yes");
                store.delete(&key).expect("delete failed");
            });
        });
    }

    group.finish();
}

criterion_group!(kvstore, put_get_delete);
criterion_main!(kvstore);
