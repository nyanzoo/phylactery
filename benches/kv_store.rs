use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use necronomicon::kv_store_codec::Key;
use phylactery::{
    buffer::MmapBuffer,
    entry::Version,
    kv_store::{Graveyard, KVStore, Lookup},
    ring_buffer::ring_buffer,
};

pub fn put_get_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("KV Store PutGetDelete");

    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.into_path();

    let mmap_path = path.join("mmap.bin");
    let buffer = MmapBuffer::new(mmap_path, 1024 * 1024 * 10).expect("mmap buffer failed");

    let (pusher, popper) = ring_buffer(buffer, Version::V1).expect("ring buffer failed");

    let meta_path = path.join("meta.bin");
    let meta_path = meta_path.to_str().unwrap();

    let data_path = path.join("data");
    let data_path = data_path.to_str().unwrap();

    let mut store = KVStore::new(meta_path, 1024, data_path, 4096, Version::V1, pusher)
        .expect("KVStore::new failed");

    let pclone = path.clone();
    let _ = std::thread::spawn(move || {
        let graveyard = Graveyard::new(pclone.join("data"), popper);
        graveyard.bury(1);
    });

    for i in (1u32..=10).into_iter().map(|i| 2u64.pow(i)) {
        group.bench_with_input(BenchmarkId::new("pgd", i), &i, |b, _i| {
            b.iter(|| {
                let key = Key::try_from("cat").expect("key");
                store.insert(key, b"yes").expect("insert failed");

                let mut buf = vec![];
                let value = store.get(&key, &mut buf).expect("get failed");
                let Lookup::Found(value) = value else {
                    panic!("value not found");
                };

                assert_eq!(value.into_inner(), b"yes");

                store.delete(&key).expect("delete failed");
            });
        });
    }

    group.finish();
}

criterion_group!(kvstore, put_get_delete);
criterion_main!(kvstore);
