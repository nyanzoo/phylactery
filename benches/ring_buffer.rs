use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use phylactery::{
    buffer::{InMemBuffer, MmapBuffer},
    entry::Version,
    ring_buffer::{Popper, Pusher, RingBuffer},
};

pub fn push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("RingBuffer Push/Pop");

    let path = tempfile::tempdir().expect("failed to create temp dir");
    let path = path.path().join("ringbuffer");

    let rb_mmap = RingBuffer::new(
        MmapBuffer::new(path, 1024 * 1024).expect("mmap buffer"),
        Version::V1,
    )
    .expect("failed to create ring buffer");

    let rb_in_mem = RingBuffer::new(InMemBuffer::new(1024 * 1024), Version::V1)
        .expect("failed to create ring buffer");

    for i in (1u32..=16).into_iter().map(|i| 2u64.pow(i)) {
        group.bench_with_input(BenchmarkId::new("in mem", i), &i, |b, i| {
            let pusher = Pusher::new(rb_in_mem.clone());
            let popper = Popper::new(rb_in_mem.clone());

            let data = vec![7; *i as usize];
            let mut buf = [0; 2u64.pow(17) as usize];

            b.iter(|| {
                _ = pusher.push(data.clone()).expect("failed to push");

                let bytes = popper.pop(&mut buf).expect("failed to pop");
                assert_eq!(bytes, data.len());
                assert_eq!(&buf[..bytes], data);
            });
        });

        group.bench_with_input(BenchmarkId::new("mmap", i), &i, |b, i| {
            let pusher = Pusher::new(rb_mmap.clone());
            let popper = Popper::new(rb_mmap.clone());

            let data = vec![7; *i as usize];
            let mut buf = [0; 2u64.pow(17) as usize];

            b.iter(|| {
                _ = pusher.push(data.clone()).expect("failed to push");

                let bytes = popper.pop(&mut buf).expect("failed to pop");
                assert_eq!(bytes, data.len());
                assert_eq!(&buf[..bytes], data);
            });
        });
    }

    group.finish();
}

criterion_group!(ringbuffer, push_pop);
criterion_main!(ringbuffer);
