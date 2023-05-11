use std::cell::UnsafeCell;

use crate::codec::{Decode, Encode};

use super::{Buffer, Error};

pub struct InMemBuffer(UnsafeCell<Vec<u8>>);

#[cfg(test)]
impl Clone for InMemBuffer {
    fn clone(&self) -> Self {
        let shared = unsafe { &*self.0.get() };

        Self(UnsafeCell::new(shared.clone()))
    }
}

impl InMemBuffer {
    #[must_use]
    pub fn new(size: u64) -> Self {
        Self(UnsafeCell::new(vec![0; size as usize]))
    }
}

impl Buffer for InMemBuffer {
    fn decode_at<'a, T>(&'a self, off: usize, len: usize) -> Result<T, Error>
    where
        T: Decode<'a>,
    {
        let shared = unsafe { &*self.0.get() };
        let res = T::decode(&shared[off..(off + len)])?;
        Ok(res)
    }

    fn encode_at<T>(&self, off: usize, len: usize, data: &T) -> Result<(), Error>
    where
        T: Encode,
    {
        let exclusive = unsafe { &mut *self.0.get() };
        data.encode(&mut exclusive[off..(off + len)])?;
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
        Ok(len as u64)
    }

    fn capacity(&self) -> u64 {
        unsafe { &*self.0.get() }.len() as u64
    }
}

impl AsRef<[u8]> for InMemBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { &*self.0.get() }
    }
}

impl AsMut<[u8]> for InMemBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { &mut *self.0.get() }
    }
}