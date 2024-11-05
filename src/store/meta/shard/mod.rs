use std::{
    collections::{BTreeMap, VecDeque},
    ops::Range,
};

use necronomicon::{Pool as _, PoolImpl};

use crate::{
    buffer::{Buffer, Error as BufferError, MmapBuffer},
    calculate_hash,
    store::MetaState,
};

use super::{decode_key, Metadata};

mod buffer_owner;
use buffer_owner::BufferOwner;

pub(crate) mod delete;

mod error;
pub use error::Error;

pub(crate) mod get;
pub(crate) mod put;

#[derive(Copy, Clone, Debug)]
pub(super) struct Tombstone {
    len: usize,
    offset: usize,
}

impl From<Tombstone> for Range<usize> {
    fn from(tombstone: Tombstone) -> Self {
        tombstone.offset..tombstone.offset + tombstone.len
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Lookup {
    pub shard: usize,
    pub offset: usize,
}

pub(super) struct Shard {
    buffer: MmapBuffer,
    dir: String,
    shard: usize,
    cursor: usize,
    tombstones: Vec<Tombstone>,
    /// Entries are stored in a BTreeMap to allow for efficient queries.
    /// We hash the key and use the the hash as the key in the BTreeMap.
    /// The value is a VecDeque of Lookups, which are used to locate the
    /// entry in the file that will correspond to the real key.
    /// The VecDeque is used to allow for multiple entries with the same
    /// hash.
    entries: BTreeMap<u64, VecDeque<Lookup>>,
}

impl Shard {
    pub(super) fn new(dir: String, shard: usize, len: u64, pool: &PoolImpl) -> Result<Self, Error> {
        let buffer = MmapBuffer::new(format!("{dir}/{shard}.bin"), len)?;
        // Scan the file for tombstones.
        let mut tombstones = Vec::new();
        let mut entries: BTreeMap<u64, VecDeque<Lookup>> = BTreeMap::new();
        let mut start = 0usize;
        loop {
            let mut owned = pool.acquire(BufferOwner::Init);

            let meta: Metadata = match buffer.decode_at(start, Metadata::size()) {
                Ok(meta) => meta,
                Err(BufferError::Necronomicon(err)) => match err {
                    necronomicon::Error::Decode(_) => break,
                    necronomicon::Error::Io(err) => match err.kind() {
                        std::io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(Error::Io(err)),
                    },
                    _ => return Err(Error::Necronomicon(err)),
                },
                Err(err) => return Err(Error::Buffer(err)),
            };

            let offset = start;
            start += Metadata::size();

            let key = decode_key(&buffer, offset, &mut owned)?;
            start += key.len();

            if meta.state == MetaState::Compacting {
                let tombstone = Tombstone {
                    len: Metadata::size() + key.len(),
                    offset,
                };

                tombstones.push(tombstone);
            } else {
                let lookup = Lookup { shard, offset };

                entries
                    .entry(calculate_hash(&key))
                    .or_default()
                    .push_back(lookup);
            }
        }

        Ok(Self {
            buffer,
            dir,
            shard,
            cursor: start,
            tombstones,
            entries,
        })
    }

    pub(super) fn compact(&mut self) -> Result<(), Error> {
        if self.tombstones.is_empty() {
            return Ok(());
        }

        let mut tombstones = std::mem::take(&mut self.tombstones);
        tombstones.sort_by_key(|lookup| lookup.offset);

        let mut reduced_tombs = vec![];
        for tomb in tombstones {
            if reduced_tombs.is_empty() {
                reduced_tombs.push(tomb);
            } else {
                let last = reduced_tombs.last_mut().unwrap();
                if last.offset + last.len == tomb.offset {
                    last.len += tomb.len;
                } else if last.offset == tomb.offset {
                    // skip
                } else {
                    reduced_tombs.push(tomb);
                }
            }
        }

        // This is safe because we have to have at least one tombstone.
        // As we checked for this earlier.
        let delete_size = reduced_tombs.iter().map(|tomb| tomb.len).sum::<usize>();
        let new_size = self.cursor - delete_size;
        let ranges = reduced_tombs
            .into_iter()
            .map(Range::from)
            .collect::<Vec<_>>();
        let flush = self.buffer.compact(&ranges)?;
        flush.flush()?;

        // TODO: double check this.
        self.cursor = new_size;

        Ok(())
    }
}
