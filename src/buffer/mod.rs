use std::io::Cursor;

use necronomicon::{Decode, Encode};

mod error;
pub use error::Error;

mod mem;
pub use mem::InMemBuffer;

mod mmap;
pub use mmap::MmapBuffer;

pub trait Buffer: AsRef<[u8]> + AsMut<[u8]> {
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
    /// Encodes the given `data` at the given `off`set and `len`gth.
    ///
    /// This function will encode the given `data` into the given `off`set and `len`gth
    /// of the `self` buffer. It will do this by calling `Encode::encode` on the given
    /// `data` with the given `self` buffer.
    ///
    /// # Arguments
    /// * `off` - The offset in the buffer at which to encode the data.
    /// * `len` - The length of the buffer into which to encode the data.
    /// * `data` - The data to encode into the buffer.
    ///
    /// # Errors
    /// This function will return an error if the data cannot be encoded into the buffer.
    /// See [`error::Error`] for more details.
    fn encode_at<'a, T>(&'a self, off: usize, len: usize, data: &T) -> Result<(), Error>
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
    ///
    /// # Arguments
    /// * `buf` - The buffer to read from.
    /// * `off` - The offset from the beginning of the buffer to read from.
    ///
    /// # Errors
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// The number of bytes read, or an error if the read failed.
    fn write_at(&self, buf: &[u8], off: usize) -> Result<u64, Error>;

    /// # Description
    /// Returns the capacity of the queue.
    ///
    /// # Return Value
    /// The capacity of the queue.
    fn capacity(&self) -> u64;
}
