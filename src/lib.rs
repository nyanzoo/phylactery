#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

mod alloc;

pub mod buffer;

pub mod dequeue;

pub mod entry;
pub mod error;
pub mod kv_store;
pub mod ring_buffer;
