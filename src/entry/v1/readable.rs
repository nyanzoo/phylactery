use std::{io::Read, mem::size_of};

use necronomicon::{BinaryData, Decode, DecodeOwned, Owned, Shared};

use super::{generate_crc, Error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Readable<S>
where
    S: Shared,
{
    // The data of the entry.
    pub(crate) data: BinaryData<S>,
    // The crc of the data.
    pub(crate) crc: u32,
}

impl<R, O> DecodeOwned<R, O> for Readable<O::Shared>
where
    R: Read,
    O: Owned,
{
    fn decode_owned(reader: &mut R, buffer: &mut O) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let data = BinaryData::decode_owned(reader, buffer)?;
        let crc = u32::decode(reader)?;
        Ok(Self { data, crc })
    }
}

impl<S> Readable<S>
where
    S: Shared,
{
    pub fn new(data: BinaryData<S>) -> Self {
        let crc = generate_crc(data.data().as_slice());
        Self { data, crc }
    }

    pub fn data(&self) -> &BinaryData<S> {
        &self.data
    }

    pub fn crc(&self) -> u32 {
        self.crc
    }

    pub fn struct_size(&self) -> u32 {
        2 + self.data.len() as u32 + size_of::<u32>() as u32
    }

    pub(crate) fn verify(&self) -> Result<(), Error> {
        let crc = generate_crc(self.data.data().as_slice());
        if crc != self.crc {
            return Err(Error::DataCrcMismatch {
                expected: self.crc,
                actual: crc,
            });
        }
        Ok(())
    }
}
