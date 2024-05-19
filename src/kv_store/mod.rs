use std::io::{Read, Write};

use log::trace;
use necronomicon::{Decode, Encode};

pub mod config;

mod graveyard;
pub use graveyard::Graveyard;

mod metadata;

mod store;
pub use store::{Lookup, Store};

use crate::{buffer::MmapBuffer, ring_buffer::ring_buffer, Error};

use self::config::Config;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) enum MetaState {
    // We need to compact and cannot accept new metadata in this slot
    Compacting,
    // Has live data associated with it
    Full,
}

impl<W> Encode<W> for MetaState
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        match self {
            Self::Compacting => 0u8.encode(writer),
            Self::Full => 1u8.encode(writer),
        }
    }
}

impl<R> Decode<R> for MetaState
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        match u8::decode(reader)? {
            0 => Ok(Self::Compacting),
            1 => Ok(Self::Full),
            _ => Err(necronomicon::Error::Decode(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid meta state",
            ))),
        }
    }
}

pub(super) enum BufferOwner {
    Graveyard,
    Delete,
    Get,
    Init,
    Insert,
}

impl necronomicon::BufferOwner for BufferOwner {
    fn why(&self) -> &'static str {
        match self {
            Self::Graveyard => "graveyard",
            Self::Delete => "store delete",
            Self::Get => "store get",
            Self::Init => "store init",
            Self::Insert => "store insert",
        }
    }
}

pub fn create_store_and_graveyar(
    mut config: Config,
    path_fn: impl FnOnce(String) -> String,
    graveyard_buffer_size: u64,
) -> Result<(Store, Graveyard), Error> {
    let path = path_fn(config.path.clone());
    trace!(
        "creating store at {}, res {:?}",
        path,
        std::fs::create_dir_all(&path)?
    );

    config.path = path.clone();

    let graveyard_path = format!("{}/graveyard.bin", path);
    trace!("creating mmap buffer at {}", graveyard_path);

    let graveyard_buffer = MmapBuffer::new(graveyard_path, graveyard_buffer_size)?;
    let (pusher, popper) = ring_buffer(graveyard_buffer, config.version)?;

    let graveyard = Graveyard::new(path.into(), popper);
    let store = Store::new(config, pusher)?;
    Ok((store, graveyard))
}
