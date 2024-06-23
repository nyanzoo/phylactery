use std::{fmt::Debug, io::Cursor};

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
    fn flush(&self) -> Result<(), Error>;
}

pub struct FlushOp<'a, B>(&'a mut B)
where
    B: Buffer + ?Sized;

impl<'a, B> FlushOp<'a, B>
where
    B: Buffer + ?Sized,
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
    fn flush(&mut self) -> Result<(), Error> {
        self.0.flush()
    }
}

pub enum Flush<'a, B>
where
    B: Buffer + ?Sized,
{
    Flush(FlushOp<'a, B>),
    NoOp,
}

impl<'a, B> Flush<'a, B>
where
    B: Buffer + ?Sized,
{
    pub fn flush(&mut self) -> Result<(), Error> {
        match self {
            Flush::Flush(f) => f.flush(),
            Flush::NoOp => Ok(()),
        }
    }
}

impl<'a, B> Debug for Flush<'a, B>
where
    B: Buffer + ?Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Flush::Flush(_) => write!(f, "Flush"),
            Flush::NoOp => write!(f, "NoOp"),
        }
    }
}

impl<'a, B> Drop for Flush<'a, B>
where
    B: Buffer + ?Sized,
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
    fn decode_at<'a, T>(&'a self, off: usize, len: usize) -> Result<T, Error>
    where
        T: Decode<Cursor<&'a [u8]>>;

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
    fn decode_at_owned<'a, T, O>(
        &'a self,
        off: usize,
        len: usize,
        buffer: &mut O,
    ) -> Result<T, Error>
    where
        O: Owned,
        T: DecodeOwned<Cursor<&'a [u8]>, O>;

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
    fn encode_at<'a, T>(
        &'a mut self,
        off: usize,
        len: usize,
        data: &T,
    ) -> Result<Flush<'a, Self>, Error>
    where
        T: Encode<Cursor<&'a mut [u8]>>;

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
    fn write_at<'a>(&'a mut self, buf: &[u8], off: usize) -> Result<Flush<'a, Self>, Error>;

    /// # Description
    /// Returns the capacity of the queue.
    ///
    /// # Return Value
    /// The capacity of the queue.
    fn capacity(&self) -> u64;

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
    fn flush(&mut self) -> Result<(), Error>;

    /// # Description
    /// Whether the buffer is dirty or not.
    ///
    /// # Returns
    /// True if the buffer is dirty, false otherwise.
    fn is_dirty(&self) -> bool;
}
