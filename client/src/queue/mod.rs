use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

use futures_util::lock::Mutex;
use memmap2::MmapMut;
use tonic::transport::Channel;

use crate::{
    communication::{self, submission, submission_queue_client::SubmissionQueueClient},
    entry::{Data, Metadata, Read, Version, Write},
    error::Error,
};

use self::{checkpoint::Checkpointer, location::Location};

mod checkpoint;
mod location;

pub async fn queue(
    dir: PathBuf,
    file_size: u32,
    client: &mut SubmissionQueueClient<Channel>,
) -> Result<(QueueWriter, QueueReader), Error> {
    let checkpoint = dir.join("checkpoint");
    let checkpointer = Checkpointer::from_file(checkpoint)?;

    let path = dir.join("writer");
    let writer = create_queue(&path, file_size, client).await?;
    let writer = QueueWriter::new(
        dir.to_str()
            .expect("dir is not a valid UTF-8 string")
            .to_string(),
        writer,
        checkpointer.clone(),
        client.clone(),
    );

    let path = dir.join("reader");
    let reader = create_queue(&path, file_size, client).await?;
    let reader = QueueReader::new(
        dir.to_str()
            .expect("dir is not a valid UTF-8 string")
            .to_string(),
        reader,
        checkpointer,
        client.clone(),
    );

    Ok((writer, reader))
}

async fn create_queue(
    path: &PathBuf,
    file_size: u32,
    client: &mut SubmissionQueueClient<Channel>,
) -> Result<MmapMut, Error> {
    let queue = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    queue.set_len(file_size as u64)?;

    let _ = client
        .submit(communication::Submission {
            operation: Some(communication::submission::Operation::Open(
                communication::Open {
                    file: Some(communication::File {
                        name: path.to_str().unwrap().to_string(),
                        size: file_size,
                    }),
                },
            )),
        })
        .await?;

    Ok(unsafe { MmapMut::map_mut(&queue)? })
}

// TODO: later we can keep a list of files and make finding the next file easier;
//       this will be useful for fragmentation use cases.
#[derive(Clone)]
pub struct QueueReader {
    inner: Arc<Mutex<QueueReaderInner>>,
    checkpointer: Checkpointer,
    file_size: u32,
}

struct QueueReaderInner {
    // name of the queue
    dir: String,
    // For holding the data after read from disk.
    buf: MmapMut,
    // For submitting IO.
    client: SubmissionQueueClient<Channel>,
}

impl QueueReaderInner {
    async fn pop(
        &mut self,
        buf: &mut [u8],
        file_size: u32,
        mut location: Location,
    ) -> Result<Location, Error> {
        let mut start = location.index() as usize;
        let mut end = start + (Metadata::size(Version::V1) as usize);

        // We have reached the end of the file, move to the next file.
        if end > file_size as usize {
            location.to_next_file();
            start = 0;
            end = Metadata::size(Version::V1) as usize;
        }

        if is_zeroed(&self.buf[start..end]) {
            // Get the data from disk.
            let communication::SubmissionResponse {} = self
                .client
                .submit(communication::Submission {
                    operation: Some(communication::submission::Operation::Read(
                        communication::Read {
                            src: format!("{}/{:X}", self.dir, location.file()),
                            dst: Some(communication::File {
                                name: format!("{}/reader", self.dir),
                                size: file_size,
                            }),
                            range: Some(communication::Range {
                                start: 0,
                                end: file_size,
                            }),
                        },
                    )),
                })
                .await?
                .into_inner();
            // update ptrs
            start = 0;
            end = Metadata::size(Version::V1) as usize;
        }

        if is_zeroed(&self.buf[start..end]) {
            return Err(Error::QueueEmpty);
        }

        let metadata = Metadata::read(&self.buf[start..end])?;
        metadata.verify()?;

        let mut start = end;
        let mut end = start + metadata.data_size() as usize;

        // We have reached the end of the file, move to the next file.
        if end > file_size as usize {
            location.to_next_file();
            // Get the data from disk.
            let communication::SubmissionResponse {} = self
                .client
                .submit(communication::Submission {
                    operation: Some(communication::submission::Operation::Read(
                        communication::Read {
                            src: format!("{}/{:X}", self.dir, location.file()),
                            dst: Some(communication::File {
                                name: format!("{}/reader", self.dir),
                                size: file_size,
                            }),
                            range: Some(communication::Range {
                                start: 0,
                                end: file_size,
                            }),
                        },
                    )),
                })
                .await?
                .into_inner();
            // update ptrs
            start = 0;
            end = metadata.data_size() as usize;
        }

        let data = Data::read(&self.buf[start..end])?;
        data.verify()?;
        match data {
            Data::Version1(data) => buf[..(data.data.len())].copy_from_slice(data.data),
        }

        location.next((end - start) as u32, file_size);

        Ok(location)
    }
}

// we could read twice where the second beats the first [2 pages] vs [1 page]
// 1. we could block the second read until the first is done
// 2. we could return the entry and let user decide what to do
// 3. ???
//
// note: we need to share mmap with the writer in case nothing pushes mmap to disk(new file).
impl QueueReader {
    pub fn new(
        dir: String,
        buf: MmapMut,
        checkpointer: Checkpointer,
        client: SubmissionQueueClient<Channel>,
    ) -> Self {
        let file_size = buf.len() as u32;
        Self {
            inner: Arc::new(Mutex::new(QueueReaderInner { dir, buf, client })),
            checkpointer,
            file_size,
        }
    }

    pub async fn pop(&self, buf: &mut [u8]) -> Result<(), Error> {
        let location = self.checkpointer.read().await?;
        let new_location = {
            let mut inner = self.inner.lock().await;
            inner.pop(buf, self.file_size, location).await?
        };

        self.checkpointer.update_read(new_location).await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct QueueWriter {
    inner: Arc<Mutex<QueueWriterInner>>,
    checkpointer: Checkpointer,
    file_size: u32,
}

impl QueueWriter {
    pub fn new(
        dir: String,
        buf: MmapMut,
        checkpointer: Checkpointer,
        client: SubmissionQueueClient<Channel>,
    ) -> Self {
        let file_size = buf.len() as u32;
        Self {
            inner: Arc::new(Mutex::new(QueueWriterInner { dir, buf, client })),
            checkpointer,
            file_size,
        }
    }

    // TODO: batch write
    // TODO: instead of a lock, we might be able to do something like
    //       prepared ptrs and committed, where commit happens after the write and prepare before.
    pub async fn push(&self, data: &[u8]) -> Result<(), Error> {
        if data.len() > self.file_size as usize {
            return Err(Error::EntryTooBig(data.len(), self.file_size as usize));
        }

        let location = self.checkpointer.write().await?;
        let new_location = {
            let mut inner = self.inner.lock().await;
            inner.push(data, self.file_size as usize, location).await?
        };

        // This could end up updating the checkpoint with data of a newer entry that finished before a previous one.
        // This leads to two possibilities:
        //
        // - The previous entry is OK
        //   In this case, who cares, it all worked out eventually.
        //
        // - The previous entry is ERR
        //   In this case, we have three options:
        //   1. Ignore during read and leave the bad data that does exist.
        //   2. Trim out the bad files and data.
        //   3. 'Compaction' where data is shifted down to fill in the gap.
        //
        //   1 is the easiest option, and since we can queue up all the reads at once, we can ignore fairly easily.
        self.checkpointer.update_write(new_location).await?;

        Ok(())
    }
}

struct QueueWriterInner {
    // queue location
    dir: String,
    // For holding the data before it is written to disk.
    buf: MmapMut,
    // For submitting IO.
    client: SubmissionQueueClient<Channel>,
}

impl QueueWriterInner {
    async fn push(
        &mut self,
        data: &[u8],
        file_size: usize,
        mut location: Location,
    ) -> Result<Location, Error> {
        // We don't do partial writes of metadata, so we need to skip to the next file if it would happen
        if location.index() + Metadata::size(Version::V1) > file_size as u32 {
            location.to_next_file();
            // zero out write buffer
            self.buf[..].copy_from_slice(&vec![0; file_size]);
        }

        let metadata = Metadata::new(Version::V1, data.len() as u32);

        // needed to write whole metadata.
        let mut orig_start = location.index() as usize;
        let start = location.index() as usize;
        let end = start + Metadata::size(Version::V1) as usize;
        metadata.write(&mut self.buf[start..end])?;
        location = location.next(Metadata::size(Version::V1), file_size as u32);

        if location.index() + data.len() as u32 > file_size as u32 {
            location.to_next_file();
            // zero out write buffer
            self.buf[..].copy_from_slice(&vec![0; file_size]);
        }

        let data = Data::new(Version::V1, data);

        let mut start = location.index() as usize;
        let mut end = start + data.size() as usize;

        if end > file_size {
            location.to_next_file();
            start = 0;
            orig_start = 0;
            end = data.size() as usize;
        }

        data.write(&mut self.buf[start..end])?;
        self.buf.flush_range(start, end - start)?;

        let communication::SubmissionResponse {} = self
            .client
            .submit(communication::Submission {
                operation: Some(submission::Operation::Write(communication::Write {
                    src: Some(communication::File {
                        name: format!("{}/writer", self.dir),
                        size: file_size as u32,
                    }),
                    range: Some(communication::Range {
                        start: orig_start as u32,
                        end: end as u32,
                    }),
                    dst: format!("{}/{:X}", self.dir, location.file()),
                })),
            })
            .await?
            .into_inner();

        location.next(data.size(), file_size as u32);

        Ok(location)
    }
}

fn is_zeroed(buf: &[u8]) -> bool {
    buf.iter().all(|x| *x == 0)
}
