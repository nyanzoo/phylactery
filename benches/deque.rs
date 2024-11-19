use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use necronomicon::{Pool, PoolImpl, Shared};
use phylactery::{
    deque::{Deque, Push},
    entry::Version,
};

pub fn push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("Deque Push/Pop");

    let path = tempfile::tempdir().expect("failed to create temp dir");
    let path = path.path().join("deque");
    let path = path.to_str().unwrap();

    const MAX_DATA_SIZE: usize = 2usize.pow(12);
    let mut deque = Deque::new(
        path.to_owned(),
        u64::try_from(MAX_DATA_SIZE).expect("usize -> u64"),
        u64::try_from(MAX_DATA_SIZE).expect("usize -> u64") * 1024 * 1024,
        Version::V1,
    )
    .expect("failed to create deque");
    let pool = PoolImpl::new(MAX_DATA_SIZE, 1000);

    for i in (1..=10).into_iter().map(|i| 2usize.pow(i)) {
        group.bench_with_input(BenchmarkId::new("mmap", i), &i, |b, i| {
            let data = vec![7; *i];

            b.iter(|| {
                let push = deque.push(&data).expect("failed to push");
                match push {
                    Push::Entry { flush, .. } => {
                        flush.flush().expect("failed to flush");
                    }
                    _ => panic!("failed to push"),
                }

                let mut buf = pool.acquire("pop", "pop");
                let Some(readable) = deque.pop(&mut buf).expect("failed to pop") else {
                    panic!("failed to pop");
                };
                assert!(readable.verify().is_ok());
                let result = readable.into_inner();
                assert_eq!(result.data().as_slice(), data);
            });
        });
    }

    group.finish();
}

criterion_group!(deque, push_pop);
criterion_main!(deque);
