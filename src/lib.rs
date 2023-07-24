#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

mod alloc;

pub mod buffer;

pub mod codec;

pub mod dequeue;

pub mod entry;

mod error;
pub use error::Error;

pub mod kv_store;

pub mod ring_buffer;
