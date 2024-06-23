use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use necronomicon::{Pool, PoolImpl, Shared};
use phylactery::{
    dequeue::{Dequeue, Pop, Popper, Pusher},
    entry::Version,
};

pub fn push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("Dequeue Push/Pop");

    let path = tempfile::tempdir().expect("failed to create temp dir");
    let path = path.path().join("dequeue");
    let path = path.to_str().unwrap();

    const MAX_DATA_SIZE: usize = 2usize.pow(12);
    let dequeue = Dequeue::new(
        path,
        u64::try_from(MAX_DATA_SIZE).expect("usize -> u64"),
        u64::try_from(MAX_DATA_SIZE).expect("usize -> u64") * 1024 * 1024,
        Version::V1,
    )
    .expect("failed to create dequeue");
    let pool = PoolImpl::new(MAX_DATA_SIZE, 1000);

    for i in (1..=10).into_iter().map(|i| 2usize.pow(i)) {
        group.bench_with_input(BenchmarkId::new("mmap", i), &i, |b, i| {
            let pusher = Pusher::new(dequeue.clone());
            let popper = Popper::new(dequeue.clone());

            let data = vec![7; *i];

            b.iter(|| {
                _ = pusher.push(&data).expect("failed to push");
                pusher.flush().expect("failed to flush");

                let mut buf = pool.acquire("pop");
                let Pop::Popped(result) = popper.pop(&mut buf).expect("failed to pop") else {
                    panic!("failed to pop");
                };
                assert!(result.verify().is_ok());
                let result = result.into_inner();
                assert_eq!(result.data().as_slice(), data);
            });
        });
    }

    group.finish();
}

criterion_group!(dequeue, push_pop);
criterion_main!(dequeue);
