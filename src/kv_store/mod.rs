use std::io::{Read, Write};

use necronomicon::{Decode, Encode};

pub mod config;

mod graveyard;
pub use graveyard::Graveyard;
use graveyard::Tombstone;

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
    use std::io::Write;

    use necronomicon::kv_store_codec::Key;

    use crate::{
        buffer::MmapBuffer,
        entry::Version,
        kv_store::{Graveyard, Lookup},
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

        let key = Key::try_from("pets").expect("key");

        store.insert(key, "cats".as_bytes()).expect("insert failed");

        let mut buf = vec![0; 64];
        let Lookup::Found(data) = store.get(&key, &mut buf).expect("key not found") else {
            panic!("key not found");
        };

        let actual = data.into_inner();
        assert_eq!(actual, b"cats");
    }

    #[test]
    fn test_put_get_delete() {
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

        let key = Key::try_from("pets").expect("key");

        store.insert(key, "cats".as_bytes()).expect("insert failed");

        let mut buf = vec![0; 64];
        let Lookup::Found(data) = store.get(&key, &mut buf).expect("key not found") else {
            panic!("key not found");
        };

        let actual = data.into_inner();
        assert_eq!(actual, b"cats");

        store.delete(&key).expect("delete failed");

        let mut buf = vec![0; 64];
        let Lookup::Absent = store.get(&key, &mut buf).expect("key not found") else {
            panic!("key found");
        };
    }

    #[test]
    fn test_graveyard() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.into_path();

        let mmap_path = path.join("mmap.bin");
        let buffer = MmapBuffer::new(mmap_path, 1024).expect("mmap buffer failed");

        let (pusher, popper) = ring_buffer(buffer, Version::V1).expect("ring buffer failed");

        let meta_path = path.join("meta.bin");
        let meta_path = meta_path.to_str().unwrap();

        let data_path = path.join("data");
        let data_path = data_path.to_str().unwrap();

        let mut store = KVStore::new(meta_path, 1024, data_path, 1024, Version::V1, pusher)
            .expect("KVStore::new failed");

        let pclone = path.clone();
        let _ = std::thread::spawn(move || {
            let graveyard = Graveyard::new(pclone.join("data"), popper);
            graveyard.bury(1);
        });

        let key = Key::try_from("pets").expect("key");

        store.insert(key, "cats".as_bytes()).expect("insert failed");

        store.insert(key, "dogs".as_bytes()).expect("insert failed");

        let mut buf = vec![0; 64];
        let Lookup::Found(data) = store.get(&key, &mut buf).expect("key not found") else {
            panic!("key not found");
        };

        let actual = data.into_inner();
        assert_eq!(actual, b"dogs");

        store.delete(&key).expect("delete failed");
        // Wait long enough for graveyard to run
        std::thread::sleep(std::time::Duration::from_secs(5));
        // assert that the data folder is empty
        let mut buf = vec![0; 64];
        let Lookup::Absent = store.get(&key, &mut buf).expect("key not found") else {
            panic!("key not found");
        };

        // For debugging:
        // tree(&path);

        assert!(!std::path::Path::exists(&path.join("data").join("0.bin")));
        assert!(!std::path::Path::exists(&path.join("data").join("1.bin")));
    }

    #[allow(dead_code)]
    fn tree(path: &std::path::Path) {
        std::io::stdout()
            .write_all(
                &std::process::Command::new("tree")
                    .arg(path)
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap();
    }

    #[allow(dead_code)]
    fn hexyl(path: &std::path::Path) {
        std::io::stdout()
            .write_all(
                &std::process::Command::new("hexyl")
                    .arg(path)
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap();
    }
}
