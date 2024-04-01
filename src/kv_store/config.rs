use crate::entry::Version;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Config {
    pub meta: Metadata,
    pub data: Data,
    pub version: Version,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
    pub meta_path: String,
    pub meta_size: u64,
    pub max_key_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Data {
    pub data_path: String,
    pub node_size: u64,
}
