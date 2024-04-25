use crate::entry::Version;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Config {
    pub path: String,
    pub meta: Metadata,
    pub data: Data,
    pub version: Version,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
    pub max_disk_usage: u64,
    pub max_key_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Data {
    pub node_size: u64,
}
