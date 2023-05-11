use std::{fs::OpenOptions, mem::size_of, path::Path, sync::Arc};

use futures_util::lock::Mutex;
use memmap2::MmapMut;

use crate::error::Error;

use super::location::Location;

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[repr(C)]
struct Checkpoint {
    read: Location,
    write: Location,
}

impl Checkpoint {
    fn crc(&self) -> u32 {
        let mut crc = crc32fast::Hasher::new();
        let mut bytes = vec![0; size_of::<Self>()];
        bytes[..size_of::<Location>()].copy_from_slice(&bincode::serialize(&self.read).unwrap());
        bytes[size_of::<Location>()..].copy_from_slice(&bincode::serialize(&self.write).unwrap());

        crc.update(&bytes);

        crc.finalize()
    }
}

// TODO(rojang) how do we deal with queues going away?
// we basically need malloc here or a custom allocator.
// also, using btree is not great because if we delete a queue,
// we need to update all the ptrs if different sizes,
// so we should use same size for all lookups.
// right now 12 bytes are used for Checkpoint + crc...
// so maybe 20 bytes for queue name?
//
// Checkpoint needs a free list now.
// where all the free locations are stored.
// if we remove a queue, we should zero the memory/slot.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[repr(C)]
struct Lookup {
    checkpoint: Checkpoint,
    crc: u32,
    pad: u32,
}

impl Lookup {
    fn verify(&self) -> Result<(), Error> {
        let expected_crc = self.checkpoint.crc();

        if expected_crc != self.crc {
            return Err(Error::CheckpointCrcMismatch(expected_crc, self.crc));
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct Checkpointer(Arc<Mutex<CheckpointerInner>>);

impl Checkpointer {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Error> {
        let inner = CheckpointerInner::from_file(path)?;
        Ok(Self(Arc::new(Mutex::new(inner))))
    }

    pub async fn write(&self) -> Result<Location, Error> {
        let inner = self.0.lock().await;
        inner.write()
    }

    pub async fn update_write(&self, write: Location) -> Result<(), Error> {
        let mut inner = self.0.lock().await;
        inner.update_write(write)
    }

    pub async fn read(&self) -> Result<Location, Error> {
        let inner = self.0.lock().await;
        inner.read()
    }

    pub async fn update_read(&self, read: Location) -> Result<(), Error> {
        let mut inner = self.0.lock().await;
        inner.update_read(read)
    }
}

struct CheckpointerInner {
    inner: MmapMut,
}

impl CheckpointerInner {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(size_of::<Lookup>() as u64)?;
        let inner = unsafe { MmapMut::map_mut(&file)? };

        if !inner[..].iter().all(|b| b == &0) {
            let lookup: Lookup = bincode::deserialize(&inner[..])?;

            lookup.verify()?;
        }

        Ok(Self { inner })
    }

    pub fn write(&self) -> Result<Location, Error> {
        let start = size_of::<Location>();
        let end = start + size_of::<Location>();

        let current = &self.inner[start..end];
        let current = bincode::deserialize::<Location>(&current)?;

        Ok(current)
    }

    pub fn update_write(&mut self, write: Location) -> Result<(), Error> {
        let start = size_of::<Location>();
        let end = start + size_of::<Location>();

        let current = &self.inner[start..end];
        let current = bincode::deserialize::<Location>(&current)?;

        if write > current {
            let bytes = bincode::serialize(&write)?;
            self.inner[start..end].copy_from_slice(&bytes);

            self.flush()
        } else {
            Ok(())
        }
    }

    pub fn read(&self) -> Result<Location, Error> {
        let start = 0;
        let end = start + size_of::<Location>();

        let current = &self.inner[start..end];
        let current = bincode::deserialize::<Location>(&current)?;

        Ok(current)
    }

    pub fn update_read(&mut self, read: Location) -> Result<(), Error> {
        let start = 0;
        let end = start + size_of::<Location>();

        let current = &self.inner[start..end];
        let current = bincode::deserialize::<Location>(&current)?;

        if read > current {
            let bytes = bincode::serialize(&read)?;
            self.inner[start..end].copy_from_slice(&bytes);

            self.flush()
        } else {
            Ok(())
        }
    }

    fn flush(&mut self) -> Result<(), Error> {
        let mut crc = crc32fast::Hasher::new();
        let start = 0;
        let end = start + size_of::<Checkpoint>();

        crc.update(&self.inner[start..end]);
        let bytes = bincode::serialize(&crc.finalize())?;

        let start = end;
        let end = start + 4;
        self.inner[start..end].copy_from_slice(&bytes[..]);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile;

    use crate::queue::location::Location;

    use super::Checkpointer;

    #[tokio::test]
    async fn test_checkpointer() {
        let file = tempfile::tempdir().unwrap();
        let path = file.path().join("checkpointer");

        {
            let checkpointer = Checkpointer::from_file(&path).unwrap();
            checkpointer
                .update_read(Location::new(1234, 5678))
                .await
                .unwrap();
            checkpointer
                .update_write(Location::new(5678, 1234))
                .await
                .unwrap();
        }

        let checkpointer = Checkpointer::from_file(&path).unwrap();
        assert_eq!(
            checkpointer.read().await.unwrap(),
            Location::new(1234, 5678)
        );
        assert_eq!(
            checkpointer.write().await.unwrap(),
            Location::new(5678, 1234)
        );
    }
}
