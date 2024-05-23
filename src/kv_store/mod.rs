use std::io::{Read, Write};

use log::trace;
use necronomicon::{Decode, Encode};

pub mod config;

mod graveyard;
pub use graveyard::Graveyard;

mod metadata;

mod store;
pub use store::{Lookup, Store};

use crate::{dequeue::dequeue, Error};

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

pub fn create_store_and_graveyard(
    config: Config,
    graveyard_buffer_size: u64,
) -> Result<(Store, Graveyard), Error> {
    log::error!("test1");
    trace!(
        "creating store at {}, res {:?}",
        config.path,
        std::fs::create_dir_all(&config.path)?
    );
    log::error!("test2");

    let graveyard_path = format!("{}/graveyard.bin", config.path);
    trace!("creating mmap buffer at {}", graveyard_path);

    let (pusher, popper) = dequeue(graveyard_path, 1024, graveyard_buffer_size, config.version)?;

    let graveyard = Graveyard::new(config.path.clone().into(), popper);
    let store = Store::new(config, pusher)?;
    Ok((store, graveyard))
}
