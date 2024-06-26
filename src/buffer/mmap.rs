use std::{cell::UnsafeCell, io::Cursor, path::Path};

use memmap2::MmapMut;

use necronomicon::{Decode, DecodeOwned, Encode, Owned};

use crate::Error;

use super::Buffer;

pub struct MmapBuffer(UnsafeCell<MmapMut>);

impl MmapBuffer {
    pub fn new<P>(path: P, size: u64) -> std::io::Result<Self>
    where
        P: AsRef<Path>,
    {
        // Don't truncate to allow for recovery.
        #[allow(clippy::suspicious_open_options)]
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(size)?;
        let mmap = unsafe { MmapMut::map_mut(&file) }?;

        Ok(Self(UnsafeCell::new(mmap)))
    }
}

impl Buffer for MmapBuffer {
    fn decode_at<'a, T>(&'a self, off: usize, len: usize) -> Result<T, Error>
    where
        T: Decode<Cursor<&'a [u8]>>,
    {
        let shared = unsafe { &mut *self.0.get() };
        let shared = &mut shared[off..(off + len)];
        let res = T::decode(&mut Cursor::new(shared))?;
        Ok(res)
    }

    fn decode_at_owned<'a, T, O>(
        &'a self,
        off: usize,
        len: usize,
        buffer: &mut O,
    ) -> Result<T, Error>
    where
        O: Owned,
        T: DecodeOwned<Cursor<&'a [u8]>, O>,
    {
        let shared = unsafe { &mut *self.0.get() };
        let shared = &mut shared[off..(off + len)];
        let res = T::decode_owned(&mut Cursor::new(shared), buffer)?;
        Ok(res)
    }

    fn encode_at<'a, T>(&'a self, off: usize, len: usize, data: &T) -> Result<(), Error>
    where
        T: Encode<Cursor<&'a mut [u8]>>,
    {
        let exclusive = unsafe { &mut *self.0.get() };
        if len == 0 {
            return Ok(());
        }
        if len > exclusive.len() {
            return Err(Error::OutOfBounds {
                offset: off,
                len,
                capacity: exclusive.len(),
            });
        }

        let exclusive = &mut exclusive[off..(off + len)];
        data.encode(&mut Cursor::new(exclusive))?;

        Ok(())
    }

    fn read_at(&self, buf: &mut [u8], off: usize) -> Result<u64, Error> {
        let len = buf.len();
        if len == 0 {
            return Ok(0);
        }

        let start = off;
        let end = off + len;
        let shared = unsafe { &*self.0.get() };
        buf[..(end - start)].copy_from_slice(&shared[start..end]);
        Ok(len as u64)
    }

    fn write_at(&self, buf: &[u8], off: usize) -> Result<u64, Error> {
        let len = buf.len();
        if len == 0 {
            return Ok(0);
        }

        let start = off;
        let end = off + len;
        let exclusive = unsafe { &mut *self.0.get() };
        exclusive[start..end].copy_from_slice(&buf[..(end - start)]);
        exclusive.flush_range(start, len)?;
        Ok(len as u64)
    }

    fn capacity(&self) -> u64 {
        unsafe { &*self.0.get() }.len() as u64
    }
}

impl AsRef<[u8]> for MmapBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { &*self.0.get() }
    }
}

impl AsMut<[u8]> for MmapBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { &mut *self.0.get() }
    }
}
