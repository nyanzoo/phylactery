use std::{
    cell::UnsafeCell,
    io::{Cursor, Read, Write},
    path::PathBuf,
};

use necronomicon::{Decode, DecodeOwned, Encode, Owned};

use super::{Buffer, Error, Flush, Flushable};

// TODO: need to read file on create and then can just write to file on flush
pub struct FileBuffer {
    inner: UnsafeCell<Vec<u8>>,
    dirty: bool,
    file: PathBuf,
}

impl FileBuffer {
    #[must_use]
    pub fn new(size: u64, file: PathBuf) -> std::io::Result<Self> {
        let mut buffer = vec![0; size as usize];
        {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file)?;
            file.read_exact(&mut buffer)?;
        }

        Ok(Self {
            inner: UnsafeCell::new(buffer),
            dirty: false,
            file,
        })
    }
}

impl Buffer for FileBuffer {
    fn decode_at<'a, T>(&'a self, off: usize, len: usize) -> Result<T, Error>
    where
        T: Decode<Cursor<&'a [u8]>>,
    {
        let shared = unsafe { (*self.inner.get()).as_mut_slice() };
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
        let shared = unsafe { (*self.inner.get()).as_mut_slice() };
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
        {
            let exclusive = unsafe { &mut *self.inner.get() };

            let mut file = std::fs::OpenOptions::new().write(true).open(&self.file)?;
            file.write_all(&exclusive)?;
            file.flush()?;
        }
        self.dirty = false;
        Ok(())
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}
