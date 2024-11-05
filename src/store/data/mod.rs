use human_size::{Byte, SpecificSize};

mod error;
pub use error::Error;
mod shard;
pub mod store;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Config {
    /// The max size of a file containing data. This is the same across shards.
    /// This **must** fit within the block size of the pool.
    pub node_size: SpecificSize<Byte>,
    /// The maximum amount of disk space that can be used by a **single** shard.
    /// E.g. if you have 10 shards, then the total disk usage can be 10 * max_disk_usage.
    pub max_disk_usage: SpecificSize<Byte>,
}

impl Config {
    #[cfg(test)]
    pub fn test(node_size: u32, max_disk_usage: u32) -> Self {
        Self {
            node_size: SpecificSize::new(f64::try_from(node_size).expect("u32 -> f64"), Byte)
                .expect("valid size"),
            max_disk_usage: SpecificSize::new(
                f64::try_from(max_disk_usage).expect("u32 -> f64"),
                Byte,
            )
            .expect("valid size"),
        }
    }
}
