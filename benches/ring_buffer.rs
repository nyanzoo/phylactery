use criterion::{criterion_group, criterion_main, Criterion};

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
