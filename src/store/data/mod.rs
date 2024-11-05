mod error;
pub use error::Error;

mod shard;
pub mod store;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Config {
    /// The max size of a file containing data. This is the same across shards.
    /// This **must** fit within the block size of the pool.
    pub node_size: u64,
    /// The maximum amount of disk space that can be used by a **single** shard.
    /// E.g. if you have 10 shards, then the total disk usage can be 10 * max_disk_usage.
    pub max_disk_usage: u64,
}

impl Config {
    #[cfg(test)]
    pub const fn test(node_size: u64, max_disk_usage: u64) -> Self {
        Self {
            node_size,
            max_disk_usage,
        }
    }
}
