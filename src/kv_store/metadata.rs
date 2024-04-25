use std::{
    io::{Read, Write},
    mem::size_of,
};

use necronomicon::{BinaryData, Decode, DecodeOwned, Encode, Owned, Shared};

use super::{graveyard::Tombstone, MetaState};

pub(crate) struct MetadataRead<S>
where
    S: Shared,
{
    pub crc: u32,
    pub file: u64,
    pub offset: u64,
    pub len: u64,
    pub key: BinaryData<S>,
    // We set the tombstone and do not accept additional data until we have compacted.
    pub state: MetaState,
}

#[derive(Debug)]
pub(crate) struct MetadataWrite<'a> {
    pub crc: u32,
    pub file: u64,
    pub offset: u64,
    pub len: u64,
    pub key: &'a [u8],
    // We set the tombstone and do not accept additional data until we have compacted.
    pub state: MetaState,
}

// NOTE: keep in sync with `Metadata` struct
pub(crate) const fn metadata_block_size(max_key_size: usize) -> usize {
    size_of::<u32>() + size_of::<u64>() * 3 + max_key_size + size_of::<MetaState>()
}

impl<S> From<MetadataRead<S>> for Tombstone
where
    S: Shared,
{
    fn from(val: MetadataRead<S>) -> Self {
        Self {
            crc: val.crc,
            file: val.file,
            offset: val.offset,
            len: val.len,
        }
    }
}

impl<'a, S> From<&'a MetadataRead<S>> for MetadataWrite<'a>
where
    S: Shared,
{
    fn from(val: &'a MetadataRead<S>) -> Self {
        Self {
            crc: val.crc,
            file: val.file,
            offset: val.offset,
            len: val.len,
            key: val.key.data().as_slice(),
            state: val.state,
        }
    }
}

impl<'a, W> Encode<W> for MetadataWrite<'a>
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.crc.encode(writer)?;
        self.file.encode(writer)?;
        self.offset.encode(writer)?;
        self.len.encode(writer)?;
        self.key.encode(writer)?;
        self.state.encode(writer)?;
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
        let crc = u32::decode(reader)?;
        let file = u64::decode(reader)?;
        let offset = u64::decode(reader)?;
        let len = u64::decode(reader)?;
        let key = BinaryData::decode_owned(reader, buffer)?;
        let state = MetaState::decode(reader)?;
        Ok(Self {
            crc,
            file,
            offset,
            len,
            key,
            state,
        })
    }
}
