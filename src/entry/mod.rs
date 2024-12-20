use crate::{buffer::Buffer, MASK_0, MASK_1, MASK_2, MASK_3};

mod error;
pub use error::Error;

pub mod v1;

mod meta;
pub use meta::Metadata;

mod readable;
pub use readable::Readable;

mod version;
pub use version::Version;

mod writable;
pub use writable::Writable;

pub fn crc_check(expected: u32, data: &[u8]) -> Result<(), Error> {
    let mut actual = crc32fast::Hasher::new();
    actual.update(data);
    let actual = actual.finalize();
    if expected != actual {
        Err(Error::DataCrcMismatch { expected, actual })
    } else {
        Ok(())
    }
}

pub fn last_metadata<B>(buffer: &B, version: Version) -> Result<Option<Metadata>, Error>
where
    B: Buffer,
{
    let mut off = 0;
    let has_data = loop {
        let mut header = [0u8; 5];
        buffer.read_at(&mut header, off)?;
        // TODO: should be little endian?
        // BAD5EED5
        match header {
            [_, MASK_3, MASK_2, MASK_1, MASK_0] => break true,
            [_, _, MASK_3, MASK_2, MASK_1] => off += 2,
            [_, _, _, MASK_3, MASK_2] => off += 3,
            [_, _, _, _, MASK_3] => off += 4,
            _ => off += 5,
        }

        if off as u64 + 5 >= buffer.capacity() {
            break false;
        }
    };

    if has_data {
        // rollback 5 bytes to get start of entry.
        // off -= 5;

        let mut metas = vec![];
        while (off as u64 + Metadata::struct_size(version) as u64) < buffer.capacity() {
            // read the metadata.
            if let Ok(metadata) = buffer.decode_at::<Metadata>(off, Metadata::struct_size(version))
            {
                metas.push(metadata);

                // increment the offset by the size of the metadata.
                off += Metadata::struct_size(version);
                // increment the offset by the size of the data.
                off += metadata.data_size() as usize;
            } else {
                break;
            }
        }

        let metadata = metas.last().expect("ptrs should not be empty");
        return Ok(Some(*metadata));

        // what happens right now is that if we write to disk, and then read from disk, we cannot know
        // what the last read entry was... is that okay? Let's think about this carefully.
    }

    Ok(None)
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use coverage_helper::test;

    use necronomicon::{DecodeOwned, Encode, Pool, PoolImpl};

    use crate::entry::{Readable, Writable};

    use super::{v1, Metadata, Version};

    #[test]
    fn metadata_size() {
        // 1 bytes for the enum variant
        // rest from actual struct
        assert_eq!(Metadata::struct_size(Version::V1), 29);
    }

    #[test]
    fn test_data_write() {
        // create a Data instance to write
        let data = Writable::Version1(v1::Writable {
            data: b"kittens",
            crc: 2940700499,
        });

        // create a buffer to write the data to
        let mut buf = vec![];

        // write the data to the buffer
        let result = data.encode(&mut buf);

        // ensure that the write operation succeeded
        assert!(result.is_ok());

        let pool = PoolImpl::new(1024, 1);
        let mut owned = pool.acquire("readable", "readable decode");

        // verify that if deserialized, the data is the same
        let result = Readable::decode_owned(&mut Cursor::new(&mut buf), &mut owned);
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(data.crc(), deserialized.crc());
        assert!(deserialized.verify().is_ok());
    }
}
