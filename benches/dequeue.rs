use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use phylactery::{
    dequeue::{Dequeue, Pop, Popper, Pusher},
    entry::Version,
};

pub fn push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("Dequeue Push/Pop");

    let path = tempfile::tempdir().expect("failed to create temp dir");
    let path = path.path().join("dequeue");
    let path = path.to_str().unwrap();

    let dequeue = Dequeue::new(path, 4096, Version::V1).expect("failed to create dequeue");

    for i in (1u32..=10).into_iter().map(|i| 2u64.pow(i)) {
        group.bench_with_input(BenchmarkId::new("mmap", i), &i, |b, i| {
            let pusher = Pusher::new(dequeue.clone());
            let popper = Popper::new(dequeue.clone());

            let data = vec![7; *i as usize];
            let mut buf = [0; 2u64.pow(12) as usize];

            b.iter(|| {
                _ = pusher.push(&data).expect("failed to push");
                pusher.flush().expect("failed to flush");
                

                assert_eq!(Pop::Popped, popper.pop(&mut buf).expect("failed to pop"));
            });
        });
    }

    group.finish();
}

criterion_group!(dequeue, push_pop);
criterion_main!(dequeue);
