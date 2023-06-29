use std::{
    io::{Read, Write},
    mem::size_of,
    path::PathBuf,
    time::Duration,
};

use crate::{
    buffer::{InMemBuffer, MmapBuffer},
    codec::{self, Decode, Encode},
    ring_buffer,
};

#[derive(serde::Deserialize, serde::Serialize)]
struct Tombstone {
    dir: u64,
    file: u64,
    offset: u64,
    len: u64,
}

impl Encode for Tombstone {
    fn encode(&self, buf: &mut [u8]) -> Result<(), codec::Error> {
        Ok(bincode::serialize_into(buf, self)?)
    }
}

impl Decode<'_> for Tombstone {
    fn decode(buf: &[u8]) -> Result<Self, codec::Error> {
        Ok(bincode::deserialize(buf)?)
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
}

impl Graveyard {
    pub fn new(dir: PathBuf, popper: ring_buffer::Popper<MmapBuffer>) -> Self {
        Self { dir, popper }
    }

    pub fn bury(self, interval: u64) -> ! {
        let interval = Duration::from_secs(interval);
        loop {
            // Collect all the files to compact.
            let tombs = self.collect();

            for tomb in tombs {
                let file = tomb[0].file;
                let dir = tomb[0].dir;
                let file = format!("{}/{}.bin", dir, file);
                let out = self.dir.join(format!("{}/{}.new", dir, file));
                let file = self.dir.join(file);
                // If file doesn't exist, then we have already compacted it, or removed it.
                if let Ok(mut file) = std::fs::File::open(file.clone()) {
                    let len = file.metadata().expect("no file metadata").len();
                    let mut in_buf = InMemBuffer::new(len);

                    file.read_exact(in_buf.as_mut())
                        .expect("failed to read file");

                    let out_buf = Self::compact_buf(tomb, in_buf);

                    let mut out =
                        std::fs::File::create(out.clone()).expect("failed to create file");

                    out.write_all(&out_buf).expect("failed to write file");
                }

                std::fs::remove_file(file.clone()).expect("failed to remove file");
                std::fs::rename(out, file).expect("failed to rename file");
            }

            std::thread::sleep(interval);
        }
    }

    fn collect(&self) -> Vec<Vec<Tombstone>> {
        let len = size_of::<Tombstone>();

        let mut nodes = vec![];
        let mut node = 0;

        let mut buf = vec![0; 32];
        // If we crash and it happens to be that tombstones map to same spot as different data,
        // then we will delete data we should keep.
        while let Ok(bytes) = self.popper.pop(&mut buf) {
            assert!(bytes == len, "invalid tombstone length");
            let tomb = Tombstone::decode(&buf).expect("failed to decode tombstone");

            if nodes.is_empty() {
                nodes.push(vec![]);
            }

            if nodes[node].is_empty() {
                nodes[node].push(tomb);
            } else {
                let last = nodes[node].last().expect("no tombstones in node");
                if tomb.file == last.file && tomb.dir == last.dir {
                    nodes[node].push(tomb);
                } else {
                    node += 1;
                    nodes.push(vec![]);
                    nodes[node].push(tomb);
                }
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
