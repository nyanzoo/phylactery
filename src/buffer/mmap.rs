use std::{cell::UnsafeCell, io::Cursor, path::Path};

use memmap2::MmapMut;

use necronomicon::{Decode, DecodeOwned, Encode, Owned};

use super::{Buffer, Error, Flush, FlushOp};

pub struct MmapBuffer {
    inner: UnsafeCell<MmapMut>,
    dirty: bool,
}

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

        Ok(Self {
            inner: UnsafeCell::new(mmap),
            dirty: false,
        })
    }
}

impl Buffer for MmapBuffer {
    fn decode_at<'a, T>(&'a self, off: usize, len: usize) -> Result<T, Error>
    where
        T: Decode<Cursor<&'a [u8]>>,
    {
        let shared = unsafe { &mut *self.inner.get() };
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
        let shared = unsafe { &mut *self.inner.get() };
        let shared = &mut shared[off..(off + len)];
        let res = T::decode_owned(&mut Cursor::new(shared), buffer)?;
        Ok(res)
    }

    fn encode_at<'a, T>(
        &'a mut self,
        off: usize,
        len: usize,
        data: &T,
    ) -> Result<Flush<'a, Self>, Error>
    where
        T: Encode<Cursor<&'a mut [u8]>>,
    {
        if len == 0 {
            return Ok(Flush::NoOp);
        }

        let exclusive = unsafe { &mut *self.inner.get() };
        if len > exclusive.len() {
            return Err(Error::OutOfBounds {
                offset: off,
                len,
                capacity: exclusive.len(),
            });
        }

        let exclusive = &mut exclusive[off..(off + len)];
        data.encode(&mut Cursor::new(exclusive))?;
        self.dirty = true;

        Ok(Flush::Flush(FlushOp(self)))
    }

    fn read_at(&self, buf: &mut [u8], off: usize) -> Result<u64, Error> {
        let len = buf.len();
        if len == 0 {
            return Ok(0);
        }

        let start = off;
        let end = off + len;
        let shared = unsafe { &*self.inner.get() };
        buf[..(end - start)].copy_from_slice(&shared[start..end]);
        Ok(len as u64)
    }

    fn write_at<'a>(&'a mut self, buf: &[u8], off: usize) -> Result<Flush<'a, Self>, Error> {
        let len = buf.len();
        if len == 0 {
            return Ok(Flush::NoOp);
        }

        let start = off;
        let end = off + len;
        let exclusive = unsafe { &mut *self.inner.get() };
        exclusive[start..end].copy_from_slice(&buf[..(end - start)]);
        self.dirty = true;
        Ok(Flush::Flush(FlushOp(self)))
    }

    fn capacity(&self) -> u64 {
        unsafe { &*self.inner.get() }.len() as u64
    }

    fn flush(&mut self) -> Result<(), Error> {
        if self.dirty {
            unsafe { &*self.inner.get() }.flush()?;
            self.dirty = false;
        }
        Ok(())
    }

    fn is_dirty(&self) -> bool {
        todo!()
    }
}

impl AsRef<[u8]> for MmapBuffer {
    fn as_ref(&self) -> &[u8] {
        &unsafe { &*self.inner.get() } as &_
    }
}
