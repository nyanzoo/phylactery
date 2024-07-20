use std::{
    cell::{RefCell, UnsafeCell},
    fmt::Debug,
    io::{self, ErrorKind, Read, Write},
    ops::Range,
    rc::Rc,
};

use log::error;

use necronomicon::{Decode, DecodeOwned, Encode, Owned};

mod error;
pub use error::Error;

mod file;
pub use file::FileBuffer;

mod mem;
pub use mem::InMemBuffer;

mod mmap;
pub use mmap::MmapBuffer;

pub trait Flushable {
    /// # Description
    /// Flushes the entire buffer to the underlying storage (if any).
    /// Resets the dirty flag.
    ///
    /// # Errors
    /// Returns an error if could not be flushed.
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// Returns `Ok(())` if the buffer was flushed successfully.
    fn flush(&self) -> Result<(), Error>;
}

pub enum Flush<F>
where
    F: Flushable,
{
    Flush(F),
    NoOp,
}

impl<F> Flush<F>
where
    F: Flushable,
{
    /// # Description
    /// Flushes the entire buffer to the underlying storage (if any).
    /// Resets the dirty flag.
    ///
    /// # Errors
    /// Returns an error if could not be flushed.
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// Returns `Ok(())` if the buffer was flushed successfully.
    pub fn flush(&self) -> Result<(), Error> {
        match self {
            Flush::Flush(f) => f.flush(),
            Flush::NoOp => Ok(()),
        }
    }
}

impl<F> Debug for Flush<F>
where
    F: Flushable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Flush::Flush(_) => write!(f, "Flush"),
            Flush::NoOp => write!(f, "NoOp"),
        }
    }
}

impl<F> Drop for Flush<F>
where
    F: Flushable,
{
    fn drop(&mut self) {
        if let Flush::Flush(f) = self {
            if let Err(e) = f.flush() {
                error!("Error flushing buffer: {:?}", e);
            }
        }
    }
}

pub trait Buffer {
    type Flushable: Flushable;
    type Inner: AsRef<[u8]> + AsMut<[u8]>;

    /// # Description
    /// Given an offset and length, decode a value of type `T`.
    ///
    /// This function will decode a value of type `T` from the given `self` buffer
    /// at the given `off`set and `len`gth. It will do this by calling `Decode::decode`
    /// on the given `self` buffer with the given `off`set and `len`gth.
    ///
    /// # Arguments
    /// * `off`: The offset in the buffer to start decoding at.
    /// * `len`: The length of the buffer to decode.
    ///
    /// # Errors
    /// This function will return an error if the data cannot be decoded from the buffer.
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// The decoded value of type `T`.
    fn decode_at<T>(&self, off: usize, len: usize) -> Result<T, Error>
    where
        T: Decode<Reader<Self::Inner>>;

    /// # Description
    /// Given an offset and length, decode a value of type `T` and use owned buffer to manage the memory of T.
    ///
    /// This function will decode a value of type `T` from the given `self` buffer
    /// at the given `off`set and `len`gth. It will do this by calling `DecodeOwned::decode_owned`
    /// on the given `self` buffer with the given `off`set and `len`gth.
    ///
    /// # Arguments
    /// * `off`: The offset in the buffer to start decoding at.
    /// * `len`: The length of the buffer to decode.
    ///
    /// # Errors
    /// This function will return an error if the data cannot be decoded from the buffer.
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// The decoded value of type `T`.
    fn decode_at_owned<T, O>(&self, off: usize, len: usize, buffer: &mut O) -> Result<T, Error>
    where
        O: Owned,
        T: DecodeOwned<Reader<Self::Inner>, O>;

    /// # Description
    /// Encodes the given `data` at the given `off`set and `len`gth.
    ///
    /// This function will encode the given `data` into the given `off`set and `len`gth
    /// of the `self` buffer. It will do this by calling `Encode::encode` on the given
    /// `data` with the given `self` buffer.
    ///
    /// Sets the dirty flag.
    ///
    /// # Arguments
    /// * `off` - The offset in the buffer at which to encode the data.
    /// * `len` - The length of the buffer into which to encode the data.
    /// * `data` - The data to encode into the buffer.
    ///
    /// # Errors
    /// This function will return an error if the data cannot be encoded into the buffer.
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// A [`Flushable`], or an error if the encoding failed.
    fn encode_at<T>(
        &self,
        off: usize,
        len: usize,
        data: &T,
    ) -> Result<Flush<Self::Flushable>, Error>
    where
        T: Encode<Writer<Self::Inner>>;

    /// # Description
    /// Read from the buffer at the given offset into the `buf`
    ///
    /// # Arguments
    /// * `buf` - The buffer to read into.
    /// * `off` - The offset from the beginning of the buffer to read from.
    ///
    /// # Errors
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// The number of bytes read, or an error if the read failed.
    fn read_at(&self, buf: &mut [u8], off: usize) -> Result<u64, Error>;

    /// # Description
    /// Write into the buffer at the given offset with `buf`
    /// Set the dirty flag.
    ///
    /// # Arguments
    /// * `buf` - The buffer to read from.
    /// * `off` - The offset from the beginning of the buffer to read from.
    ///
    /// # Errors
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// A [`Flushable`], or an error if the read failed.
    fn write_at(&self, buf: &[u8], off: usize) -> Result<Flush<Self::Flushable>, Error>;

    /// # Description
    /// Returns the capacity of the queue.
    ///
    /// # Return Value
    /// The capacity of the queue.
    fn capacity(&self) -> u64;

    /// # Description
    /// Whether the buffer is dirty or not.
    ///
    /// # Returns
    /// True if the buffer is dirty, false otherwise.
    fn is_dirty(&self) -> bool;

    /// # Description
    /// Compacts the buffer.
    ///
    /// # Arguments
    /// * `ranges` - The ranges to compact.
    ///
    /// # Errors
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// A [`Flushable`], or an error if the compaction failed.
    fn compact(&self, ranges: &[Range<usize>]) -> Result<Flush<Self::Flushable>, Error>;
}

struct Inner<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    inner: UnsafeCell<T>,
    dirty: bool,
}

impl<T> Inner<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    fn new(inner: T) -> Self {
        Inner {
            inner: UnsafeCell::new(inner),
            dirty: false,
        }
    }
}

impl<T> AsRef<[u8]> for Inner<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        unsafe { &*self.inner.get() }.as_ref()
    }
}

impl<T> AsMut<[u8]> for Inner<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { &mut *self.inner.get() }.as_mut()
    }
}

pub struct Reader<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    seek: usize,
    off: usize,
    len: usize,
    inner: Rc<RefCell<Inner<T>>>,
}

impl<T> Reader<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    fn new(inner: Rc<RefCell<Inner<T>>>, off: usize, len: usize) -> Self {
        Reader {
            seek: 0,
            off,
            len,
            inner,
        }
    }
}

impl<T> Read for Reader<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.len() == 0 {
            return Ok(0);
        }

        let inner = self.inner.borrow();
        let shared = inner.as_ref();
        let shared = &shared[self.off..(self.off + self.len)];
        let len = buf.len().min(shared.len() - self.seek);
        let start = self.seek;
        let end = self.seek + len;
        if start == end {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        buf[..len].copy_from_slice(&shared[start..end]);
        self.seek += len;
        Ok(len)
    }
}

pub struct Writer<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    seek: usize,
    off: usize,
    len: usize,
    inner: Rc<RefCell<Inner<T>>>,
}

impl<T> Writer<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    fn new(inner: Rc<RefCell<Inner<T>>>, off: usize, len: usize) -> Self {
        Writer {
            seek: 0,
            off,
            len,
            inner,
        }
    }
}

impl<T> Write for Writer<T>
where
    T: AsRef<[u8]> + AsMut<[u8]>,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() == 0 {
            return Ok(0);
        }

        let mut inner = self.inner.borrow_mut();
        let len = {
            let exclusive = inner.as_mut();
            let exclusive = &mut exclusive[self.off..(self.off + self.len)];
            let len = buf.len().min(exclusive.len() - self.seek);
            let start = self.seek;
            let end = self.seek + len;
            if start == end {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            exclusive[start..end].copy_from_slice(&buf[..len]);
            self.seek += len;
            len
        };
        inner.dirty = true;
        Ok(len)
    }

    // Ignore because we use `Flush` to flush the buffer.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn inverse_ranges(ranges: &[Range<usize>], len: usize) -> Vec<Range<usize>> {
    let mut inverse = Vec::new();
    let mut start = 0;
    for range in ranges {
        if range.start > start {
            inverse.push(start..range.start);
        }
        start = range.end;
    }
    if start < len {
        inverse.push(start..len);
    }
    inverse
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_inverse_ranges() {
        use super::inverse_ranges;
        let ranges = vec![0..1, 3..4, 6..7];
        let len = 10;
        let inverse = inverse_ranges(&ranges, len);
        assert_eq!(inverse, vec![1..3, 4..6, 7..10]);
    }
}
