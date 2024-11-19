use std::{
    io::{Read, Write},
    mem::size_of,
};

use human_size::{Byte, SpecificSize};
use necronomicon::{BinaryData, Decode, DecodeOwned, Encode, Owned, OwnedImpl, Shared, SharedImpl};

use crate::{
    buffer::{self, Buffer},
    BAD_MASK, MASK,
};

use super::MetaState;

mod error;
pub use error::Error;

pub mod shard;
pub mod store;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Config {
    /// The max size of a file containing metadata. This is the same across shards.
    /// Please note that this holds the keys and the file pointers to the data store.
    /// So it is possible to run out of space in metadata store before the data store.
    pub size: SpecificSize<Byte>,
}

impl Config {
    #[cfg(test)]
    pub fn test(size: u32) -> Self {
        Self {
            size: SpecificSize::new(size, Byte).expect("valid size"),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Metadata {
    pub mask: u32,
    pub crc: u32,
    pub file: u64,
    pub offset: u64,
    pub len: u64,
    // We set the tombstone and do not accept additional data until we have compacted.
    pub state: MetaState,
}

impl Metadata {
    pub fn new(file: u64, offset: u64, len: u64) -> Self {
        let crc = Self::generate_crc(file, offset, len, MetaState::Full.into());
        Self {
            mask: MASK,
            crc,
            file,
            offset,
            len,
            state: MetaState::Full,
        }
    }

    pub fn tombstone() -> Self {
        Self {
            mask: BAD_MASK,
            crc: 0,
            file: 0,
            offset: 0,
            len: 0,
            state: MetaState::Compacting,
        }
    }

    fn generate_crc(file: u64, offset: u64, len: u64, state: u8) -> u32 {
        let mut crc = crc32fast::Hasher::new();
        crc.update(&file.to_be_bytes());
        crc.update(&offset.to_be_bytes());
        crc.update(&len.to_be_bytes());
        crc.update(&state.to_be_bytes());
        crc.finalize()
    }
}

impl<W> Encode<W> for Metadata
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.mask.encode(writer)?;
        self.crc.encode(writer)?;
        self.file.encode(writer)?;
        self.offset.encode(writer)?;
        self.len.encode(writer)?;
        self.state.encode(writer)
    }
}

impl<R> Decode<R> for Metadata
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error> {
        let meta = Self {
            mask: u32::decode(reader)?,
            crc: u32::decode(reader)?,
            file: u64::decode(reader)?,
            offset: u64::decode(reader)?,
            len: u64::decode(reader)?,
            state: MetaState::decode(reader)?,
        };

        if meta.mask != MASK {
            return Err(necronomicon::Error::Decode {
                kind: "Metadata",
                buffer: None,
                source: "bad mask".into(),
            });
        }

        Ok(meta)
    }
}

impl Metadata {
    pub const fn size() -> usize {
        Self::state_offset() + size_of::<MetaState>()
    }

    pub const fn state_offset() -> usize {
        size_of::<u32>() * 2 + size_of::<u64>() * 3
    }
}

#[derive(Debug)]
pub(crate) struct MetadataWithKey<S>
where
    S: Shared,
{
    pub meta: Metadata,
    pub key: BinaryData<S>,
}

impl<S> MetadataWithKey<S>
where
    S: Shared,
{
    pub fn new(meta: Metadata, key: BinaryData<S>) -> Self {
        Self { meta, key }
    }

    pub fn size(&self) -> usize {
        // NOTE: keep in sync with `BinaryData` struct
        // we need to use `size_of::<usize>()` to store the length of the key
        Metadata::size() + size_of::<usize>() + self.key.len()
    }
}

impl<S, W> Encode<W> for MetadataWithKey<S>
where
    S: Shared,
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.meta.encode(writer)?;
        self.key.encode(writer)
    }
}

impl<R, O> DecodeOwned<R, O> for MetadataWithKey<O::Shared>
where
    R: Read,
    O: Owned,
{
    fn decode_owned(reader: &mut R, buffer: &mut O) -> Result<Self, necronomicon::Error> {
        let meta = Metadata::decode(reader)?;
        let key = BinaryData::decode_owned(reader, buffer)?;
        Ok(Self { meta, key })
    }
}

pub(crate) fn decode_key<B>(
    buffer: &B,
    off: usize,
    owned: &mut OwnedImpl,
) -> Result<BinaryData<SharedImpl>, buffer::Error>
where
    B: Buffer,
{
    let len: usize = buffer.decode_at(off, size_of::<usize>())?;
    let key = buffer.decode_at_owned(off, size_of::<usize>() + len, owned)?;
    Ok(key)
}
