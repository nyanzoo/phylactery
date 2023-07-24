mod error;
pub use error::Error;

pub trait Decode<'a> {
    /// # Description
    /// Decode a value from `buf`.
    ///
    /// # Arguments
    /// * `buf`: The buffer to decode.
    ///
    /// # Errors
    /// This function will return an error if the data cannot be decoded from the buffer.
    /// See [`error::Error`] for more details.
    ///
    /// # Returns
    /// The decoded value of type `T`.
    fn decode(buf: &'a [u8]) -> Result<Self, Error>
    where
        Self: Sized;
}

pub trait Encode {
    /// # Description
    /// Encode a value into `buf`.
    ///
    /// # Arguments
    /// * `buf`: The buffer to encode into.
    ///
    /// # Errors
    /// This function will return an error if the data cannot be encoded into the buffer.
    /// See [`error::Error`] for more details.
    fn encode(&self, buf: &mut [u8]) -> Result<(), Error>;
}


// Integers
impl<'a> Decode<'a> for usize {
    fn decode(buf: &'a [u8]) -> Result<Self, Error> {
        bincode::deserialize(buf).map_err(Error::from)
    }
}

impl Encode for usize {
    fn encode(&self, buf: &mut [u8]) -> Result<(), Error> {
        bincode::serialize_into(buf, self).map_err(Error::from)
    }
}