use std::io::{Read, Write};

use necronomicon::{Decode, Encode};

pub mod config;

mod graveyard;
pub use graveyard::Graveyard;

mod metadata;

mod store;
pub use store::{Lookup, Store};

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

#[cfg(test)]
mod test {
    use crate::{
        buffer::MmapBuffer,
        codec::Decode,
        entry::{Data, Version},
        ring_buffer::ring_buffer,
    };

    use super::KVStore;

    #[test]
    fn test_put_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.into_path();

        let mmap_path = path.join("mmap.bin");
        let buffer = MmapBuffer::new(mmap_path, 1024).expect("mmap buffer failed");

        let (pusher, _popper) = ring_buffer(buffer, Version::V1).expect("ring buffer failed");

        let meta_path = path.join("meta.bin");
        let meta_path = meta_path.to_str().unwrap();

        let data_path = path.join("data.bin");
        let data_path = data_path.to_str().unwrap();

        let mut store = KVStore::new(meta_path, 1024, data_path, 1024, Version::V1, pusher)
            .expect("KVStore::new failed");

        store
            .insert(b"pets", "cats".as_bytes())
            .expect("insert failed");
        let mut buf = vec![0; 64];
        store.get(b"pets", &mut buf).expect("key not found");

        let actual = Data::decode(&buf)
            .expect("failed to deserialize")
            .into_inner();
        assert_eq!(actual, b"cats");
    }
}
