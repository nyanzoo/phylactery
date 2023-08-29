use criterion::{criterion_group, criterion_main, Criterion};

use necronomicon::{Pool, PoolImpl, Shared};
use phylactery::{
    buffer::{InMemBuffer, MmapBuffer},
    entry::Version,
    ring_buffer::{Popper, Pusher, RingBuffer},
};

pub fn in_mem_push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("ringbuffer");
    group.bench_function("in_mem_push_pop", |b| {
        let rb = RingBuffer::new(InMemBuffer::new(1024), Version::V1)
            .expect("failed to create ring buffer");

        let pusher = Pusher::new(rb.clone());
        let popper = Popper::new(rb);

    const MAX_DATA_SIZE: usize = 2usize.pow(17);
    const BUFFER_SIZE: u64 = 2u64.pow(20);
    let pool = PoolImpl::new(MAX_DATA_SIZE, 1000);
    let rb_mmap = RingBuffer::new(
        MmapBuffer::new(path, BUFFER_SIZE).expect("mmap buffer"),
        Version::V1,
    )
    .expect("failed to create ring buffer");

    let rb_in_mem = RingBuffer::new(InMemBuffer::new(BUFFER_SIZE), Version::V1)
        .expect("failed to create ring buffer");

    for i in (1..=16).into_iter().map(|i| 2usize.pow(i)) {
        group.bench_with_input(BenchmarkId::new("in mem", i), &i, |b, i| {
            let pusher = Pusher::new(rb_in_mem.clone());
            let popper = Popper::new(rb_in_mem.clone());

            let data = vec![7; *i as usize];

            b.iter(|| {
                _ = pusher.push(&data).expect("failed to push");

                let mut buf = pool.acquire().expect("acquire");
                let result = popper.pop(&mut buf).expect("failed to pop");
                assert!(result.verify().is_ok());
                let result = result.into_inner();
                assert_eq!(result.data().as_slice(), data);
            });
        });

        group.bench_with_input(BenchmarkId::new("mmap", i), &i, |b, i| {
            let pusher = Pusher::new(rb_mmap.clone());
            let popper = Popper::new(rb_mmap.clone());

            let data = vec![7; *i as usize];

            b.iter(|| {
                _ = pusher.push(&data).expect("failed to push");

                let mut buf = pool.acquire().expect("acquire");
                let result = popper.pop(&mut buf).expect("failed to pop");
                assert!(result.verify().is_ok());
                let result = result.into_inner();
                assert_eq!(result.data().as_slice(), data);
            });
        });
    }

            let bytes = popper.pop(&mut buf).expect("failed to pop");
            assert_eq!(bytes, data.len());
            assert_eq!(&buf[..bytes], data);
        })
    });
    group.finish();
}

pub fn push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("RingBuffer Push/Pop");
    
    group.bench_function("in_mem_push_pop", |b| {
        let path = tempfile::tempdir().expect("failed to create temp dir");
        let path = path.path().join("ringbuffer");

        let rb = RingBuffer::new(
            MmapBuffer::new(path, 1024).expect("mmap buffer"),
            Version::V1,
        )
        .expect("failed to create ring buffer");

        let pusher = Pusher::new(rb.clone());
        let popper = Popper::new(rb);

        let data = b"hello world".to_vec();
        let mut buf = vec![0; 1024];

        b.iter(|| {
            _ = pusher.push(data.clone()).expect("failed to push");

            let bytes = popper.pop(&mut buf).expect("failed to pop");
            assert_eq!(bytes, data.len());
            assert_eq!(&buf[..bytes], data);
        })
    });
    group.finish();
}

criterion_group!(ringbuffer, in_mem_push_pop, mmap_push_pop);
criterion_main!(ringbuffer);
