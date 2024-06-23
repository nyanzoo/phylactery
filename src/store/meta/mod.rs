use std::{
    io::{Read, Write},
    mem::size_of,
};

use necronomicon::{BinaryData, Decode, DecodeOwned, Encode, Owned, OwnedImpl, Shared, SharedImpl};

use crate::{
    buffer::{self, Buffer},
    BAD_MASK, MASK,
};

use super::MetaState;

mod buffer_owner;

mod error;
pub use error::Error;

pub mod shard;
pub mod store;

#[derive(Debug)]
pub(super) struct Metadata {
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

    pub fn dig_up(self, file: u64, offset: u64, len: u64) -> Self {
        Self {
            mask: MASK,
            crc: Self::generate_crc(file, offset, len, MetaState::Full.into()),
            file,
            offset,
            len,
            state: MetaState::Full,
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

        assert_eq!(meta.mask, MASK, "Invalid mask");

        Ok(meta)
    }
}

impl Metadata {
    pub const fn size() -> usize {
        size_of::<u32>() * 2 + size_of::<u64>() * 3 + size_of::<MetaState>()
    }

    pub const fn state_offset() -> usize {
        size_of::<u32>() * 2 + size_of::<u64>() * 3
    }
}

pub(crate) struct MetadataRead<S>
where
    S: Shared,
{
    pub meta: Metadata,
    pub key: BinaryData<S>,
}

#[derive(Debug)]
pub(crate) struct MetadataWrite<'a> {
    pub meta: Metadata,
    pub key: &'a [u8],
}

impl<'a> MetadataWrite<'a> {
    pub fn size(&self) -> usize {
        Metadata::size() + self.key.len()
    }
}

// NOTE: keep in sync with `Metadata` struct
pub(super) const fn metadata_block_size(key_size: usize) -> usize {
    Metadata::size() + key_size
}

// impl<S> From<MetadataRead<S>> for Tombstone
// where
//     S: Shared,
// {
//     fn from(val: MetadataRead<S>) -> Self {
//         Self {
//             crc: val.crc,
//             file: val.file,
//             offset: val.offset,
//             len: val.len,
//         }
//     }
// }

impl<'a, S> From<&'a MetadataRead<S>> for MetadataWrite<'a>
where
    S: Shared,
{
    fn from(val: &'a MetadataRead<S>) -> Self {
        Self {
            meta: val.meta,
            key: val.key.data().as_slice(),
        }
    }
}

impl<'a, W> Encode<W> for MetadataWrite<'a>
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.meta.encode(writer)?;
        self.key.encode(writer)?;
        Ok(())
    }
}

impl<R, O> DecodeOwned<R, O> for MetadataRead<O::Shared>
where
    R: Read,
    O: Owned,
{
    fn decode_owned(reader: &mut R, buffer: &mut O) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
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
