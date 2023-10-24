#![cfg_attr(nightly, feature(no_coverage))]

mod alloc;

pub mod buffer;

pub mod dequeue;

pub mod entry;

mod error;
pub use error::Error;

pub mod kv_store;

pub mod ring_buffer;
