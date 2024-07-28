use std::{
    cell::RefCell,
    io::{Read, Write},
    ops::Range,
    path::PathBuf,
    rc::Rc,
};

use necronomicon::{Decode, DecodeOwned, Encode, Owned};

use super::{inverse_ranges, Buffer, Error, Flush, Flushable, Inner, Reader, Writer};

type InnerFile = Inner<LazyWriteFile>;

pub struct LazyWriteFileFlush(Rc<RefCell<InnerFile>>);

impl Flushable for LazyWriteFileFlush {
    fn flush(&self) -> Result<(), Error> {
        let mut inner = self.0.borrow_mut();
        inner.flush()
    }
}

// TODO: need to read file on create and then can just write to file on flush
pub struct FileBuffer(Rc<RefCell<InnerFile>>);

impl FileBuffer {
    #[must_use]
    pub fn new(size: u64, file: PathBuf) -> std::io::Result<Self> {
        let buffer = vec![0; size as usize];
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file)?;
        }
        let inner = InnerFile::new(LazyWriteFile(buffer, file));
        let rc = Rc::new(RefCell::new(inner));
        Ok(Self(rc))
    }

    pub fn read(size: u64, file: PathBuf) -> std::io::Result<Self> {
        let mut buffer = vec![0; size as usize];
        {
            let mut file = std::fs::OpenOptions::new().read(true).open(&file)?;
            // let file_size = usize::try_from(file.metadata()?.len()).expect("u64 -> usize");
            file.read_exact(&mut buffer[..size as usize])?;
        }

        let inner = InnerFile::new(LazyWriteFile(buffer, file));
        let rc = Rc::new(RefCell::new(inner));
        Ok(Self(rc))
    }
}

impl Buffer for FileBuffer {
    type Flushable = LazyWriteFileFlush;
    type Inner = LazyWriteFile;

    fn decode_at<T>(&self, off: usize, len: usize) -> Result<T, Error>
    where
        T: Decode<Reader<Self::Inner>>,
    {
        let mut read = Reader::new(self.0.clone(), off, len);
        let res = T::decode(&mut read)?;
        Ok(res)
    }

    fn decode_at_owned<T, O>(&self, off: usize, len: usize, buffer: &mut O) -> Result<T, Error>
    where
        O: Owned,
        T: DecodeOwned<Reader<Self::Inner>, O>,
    {
        let mut read = Reader::new(self.0.clone(), off, len);
        let res = T::decode_owned(&mut read, buffer)?;
        Ok(res)
    }

    fn encode_at<T>(
        &self,
        off: usize,
        len: usize,
        data: &T,
    ) -> Result<Flush<Self::Flushable>, Error>
    where
        T: Encode<Writer<Self::Inner>>,
    {
        if len == 0 {
            return Ok(Flush::NoOp);
        }

        let capacity = self.capacity();
        if len as u64 > capacity {
            return Err(Error::OutOfBounds {
                offset: off,
                len,
                capacity,
            });
        }

        let mut writer = Writer::new(self.0.clone(), off, len);
        data.encode(&mut writer)?;

        let flush = LazyWriteFileFlush(self.0.clone());
        Ok(Flush::Flush(flush))
    }

    fn read_at(&self, buf: &mut [u8], off: usize) -> Result<u64, Error> {
        let len = buf.len();
        if len == 0 {
            return Ok(0);
        }

        let start = off;
        let end = off + len;
        let shared = self.0.borrow();
        let shared = shared.as_ref();
        buf[..(end - start)].copy_from_slice(&shared[start..end]);
        Ok(len as u64)
    }

    fn write_at<'a>(&self, buf: &[u8], off: usize) -> Result<Flush<Self::Flushable>, Error> {
        let len = buf.len();
        if len == 0 {
            return Ok(Flush::NoOp);
        }

        let start = off;
        let end = off + len;
        let mut exclusive = self.0.borrow_mut();
        exclusive.dirty = true;
        let exclusive = exclusive.as_mut();
        exclusive[start..end].copy_from_slice(&buf[..(end - start)]);

        let flush = LazyWriteFileFlush(self.0.clone());
        Ok(Flush::Flush(flush))
    }

    fn capacity(&self) -> u64 {
        self.0.borrow().capacity()
    }

    fn is_dirty(&self) -> bool {
        self.0.borrow().dirty
    }

    fn compact(&self, ranges: &[Range<usize>]) -> Result<Flush<Self::Flushable>, Error> {
        if ranges.is_empty() {
            return Ok(Flush::NoOp);
        }

        let copy_ranges = inverse_ranges(ranges, self.capacity().try_into().expect("u64 -> usize"));

        let mut exclusive = self.0.borrow_mut();
        exclusive.dirty = true;
        let exclusive = exclusive.as_mut();

        let mut dst = 0;
        for range in copy_ranges {
            let len = range.end - range.start;
            exclusive.copy_within(range, dst);
            dst += len;
        }

        Ok(Flush::Flush(LazyWriteFileFlush(self.0.clone())))
    }
}

impl InnerFile {
    fn capacity(&self) -> u64 {
        unsafe { &*self.inner.get() }.len() as u64
    }

    fn flush(&mut self) -> Result<(), Error> {
        if self.dirty {
            let inner = unsafe { &mut *self.inner.get() };
            inner.flush()?;
            self.dirty = false;
        }

        Ok(())
    }
}

pub struct LazyWriteFile(Vec<u8>, PathBuf);

impl LazyWriteFile {
    fn delete(&mut self) -> Result<(), Error> {
        std::fs::remove_file(&self.1)?;
        self.0 = vec![];
        Ok(())
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn flush(&mut self) -> Result<(), Error> {
        let mut file = std::fs::OpenOptions::new().write(true).open(&self.1)?;
        file.write_all(&self.0)?;
        file.flush()?;
        // self.0 = vec![0; self.0.len()];
        Ok(())
    }
}

impl AsRef<[u8]> for LazyWriteFile {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for LazyWriteFile {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}
