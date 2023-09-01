use std::{
    io::{Cursor, Read, Write},
    path::PathBuf,
    time::Duration,
};

use log::trace;
use necronomicon::{Decode, Encode, Pool, PoolImpl, Shared};

use crate::{
    buffer::{InMemBuffer, MmapBuffer},
    entry::Metadata,
    ring_buffer,
};

pub const TOMBSTONE_LEN: usize = size_of::<Tombstone>();

#[derive(Debug)]
#[repr(C, packed)]
pub(crate) struct Tombstone {
    pub crc: u32,
    pub file: u64,
    pub offset: u64,
    pub len: u64,
}

impl<W> Encode<W> for Tombstone
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        let crc: u32 = unsafe { std::ptr::addr_of!(self.crc).read_unaligned() };
        crc.encode(writer)?;

        let file: u64 = unsafe { std::ptr::addr_of!(self.file).read_unaligned() };
        file.encode(writer)?;

        let offset: u64 = unsafe { std::ptr::addr_of!(self.offset).read_unaligned() };
        offset.encode(writer)?;

        let len: u64 = unsafe { std::ptr::addr_of!(self.len).read_unaligned() };
        len.encode(writer)?;

        Ok(())
    }
}

impl<R> Decode<R> for Tombstone
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let crc = u32::decode(reader)?;
        let file = u64::decode(reader)?;
        let offset = u64::decode(reader)?;
        let len = u64::decode(reader)?;
        Ok(Self {
            crc,
            file,
            offset,
            len,
        })
    }
}

// Only compacts the data files, we just write over the meta file.
// Several things to look out for:
// - crash during compaction of files and moving into new file(s)
// - crash during deletion of old file(s)
// - crash during update of meta file
//
// We will do the following:
// 0. write all tombstones to compact meta file
// 1. copy live data from nodes into new file(s) but not part of dequeue yet! (don't intersperse with incoming data)
// 2. swap the new file(s) into the dequeue (these should be less than the node size)
// 3. delete old file(s) (may not be there if read out before compaction completes)
// 4. update meta file to say old slots are available for new data
// 5. update compact meta file to remove tombstones
//
pub struct Graveyard {
    dir: PathBuf,
    popper: ring_buffer::Popper<MmapBuffer>,
    pool: PoolImpl,
}

impl Graveyard {
    pub fn new(dir: PathBuf, popper: ring_buffer::Popper<MmapBuffer>) -> Self {
        let block_size = usize::try_from(Metadata::struct_size(crate::entry::Version::V1))
            .expect("u32 -> usize")
            + TOMBSTONE_LEN;

        trace!("block_size: {}", block_size);
        Self {
            dir,
            popper,
            pool: PoolImpl::new(block_size, 1024 * 1024),
        }
    }

    // The problem is we also need to update the metadata for the dequeue.
    // We can't just delete the data, we need to update the metadata to say
    // that the data is no longer there and somewhere else (if just moved).;p[''']
    pub fn bury(self, interval: u64) -> ! {
        let interval = Duration::from_secs(interval);
        loop {
            // Collect all the files to compact.
            let tombs = self.collect();

            for tomb in tombs {
                let file = tomb[0].file;
                let path = format!("{}.bin", file);
                let out = self.dir.join(format!("{}.new", path));
                let path = self.dir.join(path);

                // If file doesn't exist, then we have already compacted it, or removed it.
                if let Ok(mut file) = std::fs::File::open(path.clone()) {
                    let len = file.metadata().expect("no file metadata").len();
                    trace!("compacting file: {} len: {}", path.display(), len);
                    let mut in_buf = InMemBuffer::new(len);

                    file.read_exact(in_buf.as_mut())
                        .expect("failed to read file");

                    let out_buf = Self::compact_buf(tomb, in_buf);

                    let mut has_data = !out_buf.is_empty();
                    has_data &= out_buf.iter().cloned().map(u64::from).sum::<u64>() != 0;

                    if has_data {
                        let mut out =
                            std::fs::File::create(out.clone()).expect("failed to create file");

                        out.write_all(&out_buf).expect("failed to write file");
                    }

                    std::fs::remove_file(path.clone()).expect("failed to remove file");
                    if has_data {
                        std::fs::rename(out, path.clone()).expect("failed to rename file");
                    }
                }
            }

            std::thread::sleep(interval);
        }
    }

    fn collect(&self) -> Vec<Vec<Tombstone>> {
        let mut nodes = vec![];
        let mut node = 0;

        loop {
            let mut buf = self.pool.acquire().expect("failed to acquire buffer");

            // If we crash and it happens to be that tombstones map to same spot as different data,
            // then we will delete data we should keep. Is this true still?
            if let Ok(data) = self.popper.pop(&mut buf) {
                data.verify().expect("failed to verify data");
                let data = data.into_inner();
                let tomb = Tombstone::decode(&mut Cursor::new(data.data().as_slice()))
                    .expect("failed to decode tombstone");

                if nodes.is_empty() {
                    nodes.push(vec![]);
                }

                if nodes[node].is_empty() {
                    nodes[node].push(tomb);
                } else {
                    let last = nodes[node].last().expect("no tombstones in node");
                    if tomb.file == last.file {
                        nodes[node].push(tomb);
                    } else {
                        node += 1;
                        nodes.push(vec![]);
                        nodes[node].push(tomb);
                    }
                }
            } else {
                break;
            }
        }

        nodes
    }

    // We can maybe fix the problem of accidentally deleting data we don't want by
    // comparing crcs of the data and tombstones.
    fn compact_buf(tombs: Vec<Tombstone>, in_buf: InMemBuffer) -> Vec<u8> {
        let mut out_buf = vec![];
        let mut begin = 0;

        for tomb in tombs {
            let end = tomb.offset as usize;
            out_buf.extend_from_slice(&in_buf.as_ref()[begin..end]);
            begin = (tomb.offset + tomb.len) as usize;
        }

        out_buf.extend_from_slice(&in_buf.as_ref()[begin..]);

        out_buf
    }
}
