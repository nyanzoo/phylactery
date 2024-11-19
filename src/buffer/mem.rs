use std::{cell::RefCell, ops::Range, rc::Rc};

use necronomicon::{Decode, DecodeOwned, Encode, Owned};

use super::{Buffer, Error, Flush, Flushable, Inner, Reader, Writer};

type InnerMem = Inner<Vec<u8>>;

pub struct MemFlush(Rc<RefCell<InnerMem>>);

impl Flushable for MemFlush {
    fn flush(&self) -> Result<(), Error> {
        let mut inner = self.0.borrow_mut();
        inner.flush();
        Ok(())
    }
}

pub struct InMemBuffer(Rc<RefCell<Inner<Vec<u8>>>>);

impl InMemBuffer {
    #[must_use]
    pub fn new(size: u64) -> Self {
        let inner = InnerMem::new(vec![0; size as usize]);
        let rc = Rc::new(RefCell::new(inner));
        Self(rc)
    }
}

impl Buffer for InMemBuffer {
    type Flushable = MemFlush;
    type Inner = Vec<u8>;

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

        Ok(Flush::Flush(MemFlush(self.0.clone())))
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
        Ok(Flush::Flush(MemFlush(self.0.clone())))
    }

    fn capacity(&self) -> u64 {
        self.0.borrow().capacity()
    }

    fn is_dirty(&self) -> bool {
        self.0.borrow().dirty
    }

    fn compact(&self, ranges: &[Range<usize>]) -> Result<Flush<Self::Flushable>, Error> {
        let copy_ranges =
            super::inverse_ranges(ranges, self.capacity().try_into().expect("u64 -> usize"));
        let mut exclusive = self.0.borrow_mut();
        exclusive.dirty = true;
        let exclusive = exclusive.as_mut();

        let mut dst = 0;
        for range in copy_ranges {
            let len = range.end - range.start;
            exclusive.copy_within(range, dst);
            dst += len;
        }
        Ok(Flush::Flush(MemFlush(self.0.clone())))
    }
}

impl InnerMem {
    fn capacity(&self) -> u64 {
        unsafe { &*self.inner.get() }.len() as u64
    }

    fn flush(&mut self) {
        if self.dirty {
            self.dirty = false;
        }
    }
}
