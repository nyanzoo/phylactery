use std::{collections::BTreeMap, ops::Range};

use necronomicon::Owned;

use crate::{
    deque::{Location, Push},
    entry::{Readable, Version},
    store::data::Error,
    usize_to_u64,
};

use super::{shard::Shard, Config};

pub struct Store {
    /// The path to the data directory.
    dir: String,
    /// A sharded set of files for storing data entries.
    shards: Vec<Shard>,
}

impl Store {
    pub fn new(dir: String, shards: usize, config: Config) -> Result<Self, Error> {
        let Config {
            node_size,
            max_disk_usage,
        } = config;
        let mut shards_v = vec![];
        for shard in 0..shards {
            let shard = Shard::new(
                dir.clone(),
                usize_to_u64(shard),
                node_size.to_bytes(),
                max_disk_usage.to_bytes(),
            )?;
            shards_v.push(shard);
        }

        Ok(Self {
            dir,
            shards: shards_v,
        })
    }

    pub fn compact(
        &mut self,
        shard: usize,
        ranges_to_delete: &BTreeMap<Location, Vec<Range<usize>>>,
    ) -> Result<(), Error> {
        self.shards[shard]
            .compact(ranges_to_delete)
            .map_err(Error::Shard)
    }

    pub fn get<O>(
        &self,
        shard: usize,
        file: u64,
        offset: u64,
        buffer: &mut O,
        version: Version,
    ) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.shards[shard]
            .get(file, offset, buffer, version)
            .map_err(Error::Shard)
    }

    pub fn put(&mut self, shard: usize, value: &[u8]) -> Result<Push, Error> {
        self.shards[shard].put(value).map_err(Error::Shard)
    }
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use necronomicon::{Pool as _, PoolImpl, Shared as _};
    use tempfile::tempdir;

    use super::*;

    const BLOCK_SIZE: usize = 0x1000;
    const MAX_DISK_USAGE: u32 = 0x8000;
    const POOL_SIZE: usize = 0x1000;
    const SHARDS: usize = 100;
    const SHARD_LEN: u32 = 0x1000;
    static CONFIG: LazyLock<Config> = LazyLock::new(|| Config::test(SHARD_LEN, MAX_DISK_USAGE));

    #[test]
    fn store_put_get() {
        let dir = tempdir().unwrap();
        let dir_path_s = dir.path().to_str().unwrap().to_string();
        let mut store = Store::new(dir_path_s, SHARDS, CONFIG.clone()).unwrap();

        let Push::Entry {
            file,
            offset,
            len,
            crc,
            flush,
        } = store.put(42, b"kittens").unwrap()
        else {
            panic!("Expected Push::Entry");
        };

        assert_eq!(file, 0);
        assert_eq!(offset, 0);
        assert_eq!(len, 49);
        flush.flush().unwrap();

        // let dir_path = dir.path().to_path_buf();
        // crate::store::tree(&dir_path);

        let pool = PoolImpl::new(BLOCK_SIZE, POOL_SIZE);
        let mut buffer = pool.acquire("cat", "test");
        let get = store
            .get(42, file, offset, &mut buffer, Version::V1)
            .unwrap();
        get.verify().unwrap();
        assert_eq!(get.crc(), crc);
        assert_eq!(get.into_inner().data().as_slice(), b"kittens");
    }
}
