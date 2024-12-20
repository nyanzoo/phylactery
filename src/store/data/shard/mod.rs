use std::{collections::BTreeMap, ops::Range};

use necronomicon::Owned;

use crate::{
    deque::{Deque, Location, Push},
    entry::{Readable, Version},
};

mod error;
pub use error::Error;

pub(super) struct Shard {
    deque: Deque,
}

impl Shard {
    pub(super) fn new(
        dir: String,
        shard: u64,
        node_size: u64,
        max_disk_usage: u64,
    ) -> Result<Self, Error> {
        let path = format!("{}/{}", dir, shard);
        let deque = Deque::new(path, node_size, max_disk_usage, Version::V1)?;

        Ok(Self { deque })
    }

    pub(super) fn compact(
        &mut self,
        ranges_to_delete: &BTreeMap<Location, Vec<Range<usize>>>,
    ) -> Result<(), Error> {
        self.deque.compact(ranges_to_delete).map_err(Error::Deque)
    }

    pub(super) fn get<O>(
        &self,
        file: u64,
        offset: u64,
        buffer: &mut O,
        version: Version,
    ) -> Result<Readable<O::Shared>, Error>
    where
        O: Owned,
    {
        self.deque
            .get(file, offset, buffer, version)
            .map_err(Error::Deque)
    }

    pub(super) fn put(&mut self, value: &[u8]) -> Result<Push, Error> {
        self.deque.push(value).map_err(Error::Deque)
    }
}
