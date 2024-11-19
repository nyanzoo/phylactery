use std::{
    collections::{BTreeMap, VecDeque},
    fmt::{self, Debug, Display, Formatter},
    io::{Cursor, Read, Write},
};

use memmap2::MmapMut;
use necronomicon::{Decode, Encode};

use super::Error;

const ENTRY_SIZE: usize = size_of::<Entry>();
const ENTRY_FILE_SIZE: usize = size_of::<EntryPath>();
const ENTRY_PATH_LEN: usize = 256 - size_of::<u64>();

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub(crate) struct EntryPath([u8; ENTRY_PATH_LEN]);

impl EntryPath {
    fn as_bytes(&self) -> &[u8] {
        let len = self.0[0] as usize;
        assert!(len <= ENTRY_PATH_LEN, "invalid entry path length");
        &self.0[1..=len]
    }
}

impl Debug for EntryPath {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "EntryPath({:?})",
            self.as_bytes()
                .iter()
                .map(|c| *c as char)
                .collect::<String>()
        )
    }
}

impl Display for EntryPath {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "EntryPath({:?})",
            self.as_bytes()
                .iter()
                .map(|c| *c as char)
                .collect::<String>()
        )
    }
}

impl AsRef<str> for EntryPath {
    fn as_ref(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).expect("valid utf-8 string")
    }
}

impl From<EntryPath> for String {
    fn from(value: EntryPath) -> Self {
        value.as_ref().to_string()
    }
}

impl TryFrom<&str> for EntryPath {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value_str = value.to_string();
        let value = value.as_bytes();
        let value_len = value.len();
        if value_len >= ENTRY_PATH_LEN {
            return Err(Error::EntryPathTooLong(value_str));
        }

        let mut entry_path = [0; ENTRY_PATH_LEN];
        entry_path[0] = value_len as u8;
        entry_path[1..=value_len].copy_from_slice(value);

        Ok(Self(entry_path))
    }
}

impl TryFrom<String> for EntryPath {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        EntryPath::try_from(value.as_str())
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) struct Entry {
    // `char` is 4 bytes, so this is 256 - 8 bytes to keep the entry size 256 bytes.
    file_path: EntryPath,
    // a hash for determining if the entry is different
    // across nodes.
    // NOTE: this is set to 0 if the entry is a tombstone.
    hash: u64,
}

impl Display for Entry {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Entry {{ file_path: {:?}, hash: {} }}",
            self.file_path, self.hash
        )
    }
}

impl<W> Encode<W> for Entry
where
    W: Write,
{
    fn encode(&self, buffer: &mut W) -> Result<(), necronomicon::Error> {
        buffer.write_all(&self.file_path.0)?;
        buffer.write_all(&self.hash.to_be_bytes())?;

        Ok(())
    }
}

impl<R> Decode<R> for Entry
where
    R: Read,
{
    fn decode(buffer: &mut R) -> Result<Self, necronomicon::Error> {
        let mut file_path = [0; ENTRY_PATH_LEN];
        buffer.read_exact(&mut file_path)?;

        let file_path = EntryPath(file_path);

        let mut hash_bytes = [0; size_of::<u64>()];
        buffer.read_exact(&mut hash_bytes)?;
        let hash = u64::from_be_bytes(hash_bytes);

        Ok(Self { file_path, hash })
    }
}

pub(crate) struct Log {
    mmap: MmapMut,
    free: VecDeque<usize>,
    used: BTreeMap<EntryPath, usize>,
}

impl Log {
    pub fn new(dir: impl AsRef<str>, entries: usize) -> Self {
        assert_eq!(std::mem::size_of::<Entry>(), 256);

        let len = entries * ENTRY_SIZE;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}/log.bin", dir.as_ref()))
            .expect("failed to open log file");
        file.set_len(len as u64)
            .expect("failed to set transaction log size");
        let mmap = unsafe { MmapMut::map_mut(&file).expect("failed to map log file") };
        let mut free = VecDeque::with_capacity(entries);
        let used = BTreeMap::default();

        for i in 0..len {
            free.push_back(i);
        }

        Self { mmap, free, used }
    }

    pub fn add(&mut self, entry: Entry) -> Result<usize, Error> {
        if let Some(free) = self.free.pop_front() {
            entry.encode(&mut Cursor::new(&mut self.mmap[free * ENTRY_SIZE..]))?;
            self.used.insert(entry.file_path, free);
            Ok(free)
        } else {
            Err(Error::LogFull(entry.into()))
        }
    }

    pub fn get(
        &self,
        path: impl TryInto<EntryPath, Error = Error>,
    ) -> Result<Option<Entry>, Error> {
        let path: EntryPath = path.try_into()?;
        if let Some(index) = self.used.get(&path) {
            let start = index * ENTRY_SIZE;
            let end = start + ENTRY_SIZE;
            let entry = Entry::decode(&mut Cursor::new(&self.mmap[start..end]))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    pub fn remove(
        &mut self,
        path: impl TryInto<EntryPath, Error = Error>,
    ) -> Result<Option<usize>, Error> {
        let path: EntryPath = path.try_into()?;
        if let Some(start) = self.used.remove(&path) {
            let end = start + ENTRY_SIZE;
            self.mmap[start..end].fill(0);
            self.free.push_back(start);
            Ok(Some(start))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_entry_path() {
        let entry_path = EntryPath::try_from("hello").unwrap();
        assert_eq!(entry_path.as_ref(), "hello");
        assert_eq!(String::from(entry_path), "hello");
    }

    #[test]
    fn test_entry_path_error_on_str_longer_than_entry_len() {
        let data = ['a' as u8; ENTRY_PATH_LEN + 1];
        let data_str = std::str::from_utf8(&data).unwrap();
        let entry_path = EntryPath::try_from(data_str);
        assert!(entry_path.is_err());
    }

    #[test]
    fn test_entry() {
        let entry = Entry {
            file_path: EntryPath::try_from("hello").unwrap(),
            hash: 0,
        };
        let mut buffer = Vec::new();
        entry.encode(&mut buffer).unwrap();
        let decoded = Entry::decode(&mut Cursor::new(&buffer)).unwrap();
        assert_eq!(entry.file_path.as_ref(), decoded.file_path.as_ref());
        assert_eq!(entry.hash, decoded.hash);
    }

    #[test]
    fn test_log() {
        let directory = tempfile::tempdir().unwrap();
        let directory_s = directory.path().to_str().unwrap();
        let mut log = Log::new(&directory_s, 10);
        let entry = Entry {
            file_path: EntryPath::try_from("hello").unwrap(),
            hash: 0,
        };
        let index = log.add(entry).unwrap();
        assert_eq!(index, 0);
        let entry = log.get("hello").unwrap().unwrap();
        assert_eq!(entry.file_path.as_ref(), "hello");
        assert_eq!(entry.hash, 0);
        let index = log.remove("hello").unwrap().unwrap();
        assert_eq!(index, 0);
        let entry = log.get("hello").unwrap();
        assert!(entry.is_none());
    }
}
