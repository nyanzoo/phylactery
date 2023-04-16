use std::{
    io::{Read as _, Write as _},
    path::Path,
    result::Result,
};

use communication::submission_queue_server::SubmissionQueueServer;
use dashmap::DashMap;
use memmap2::MmapMut;
use tonic::{transport::Server, Request, Response, Status};

pub mod communication {
    include!("phylactery.rs");
}

mod msg;

#[derive(Debug, Default)]
struct SubmissionQueue {
    maps: DashMap<String, MmapMut>,
}

impl SubmissionQueue {
    fn read_into_mmap(&self, read: &msg::Read) -> std::io::Result<()> {
        log::info!("reading {read:?}");
        if let Some(mut orig) = self.maps.get_mut(&read.dst.name) {
            let orig = orig.value_mut();

            let mut file = std::fs::File::open(&read.src)?;
            log::trace!("reading {read:?} into {orig:?}");
            file.read_exact(&mut orig[..])?;

            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dst not found",
            ))
        }
    }

    fn write_from_mmap(&self, write: &msg::Write) -> std::io::Result<()> {
        log::info!("writing {write:?}");
        if let Some(mut orig) = self.maps.get_mut(&write.src.name) {
            let mut file = create_file(&write.dst, orig.value().len() as u64)?;
            log::trace!("writing {orig:?} into {write:?}");
            let orig = orig.value_mut();
            file.write_all(&orig[..])?;

            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "src not found",
            ))
        }
    }

    // This is for closing mmaps
    fn close(&self, close: &msg::Close) -> std::io::Result<()> {
        log::info!("closing {close:?}");
        if let Some((_, mmap)) = self.maps.remove(&close.name) {
            log::trace!("flushing {close:?}");
            mmap.flush()?;
        }

        Ok(())
    }

    // This is used only for compaction purposes.
    fn copy(&self, copy: &msg::Copy) -> std::io::Result<()> {
        log::info!("copying {copy:?}");
        if copy.dst.size < copy.src.iter().map(|f| f.size).sum() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "dst is too small",
            ));
        }

        let mut dst = create_file(&copy.dst.name, copy.dst.size as u64)?;
        for src in copy.src.iter() {
            let mut src = std::fs::File::open(&src.name)?;
            log::trace!("copying {src:?} to {dst:?}");
            std::io::copy(&mut src, &mut dst)?;
        }

        Ok(())
    }

    fn delete(&self, delete: &msg::Delete) -> std::io::Result<()> {
        log::info!("deleting {delete:?}");
        std::fs::remove_file(&delete.name)
    }

    // This is for opening mmaps
    fn open(&self, open: &msg::Open) -> std::io::Result<()> {
        log::info!("opening {open:?}");
        if self.maps.contains_key(&open.file.name) {
            log::debug!("{open:?} already open");
            return Ok(());
        }

        let file = create_file(&open.file.name, open.file.size as u64)?;
        self.maps
            .insert(open.file.name.clone(), unsafe { MmapMut::map_mut(&file)? });

        Ok(())
    }
}

#[tonic::async_trait]
impl communication::submission_queue_server::SubmissionQueue for SubmissionQueue {
    async fn submit(
        &self,
        request: Request<communication::Submission>,
    ) -> Result<Response<communication::SubmissionResponse>, Status> {
        let communication::Submission { operation, id: _ } = request.into_inner();

        if let Some(operation) = operation {
            log::debug!("received operation: {operation:?}");
            let operation: msg::Operation = operation.try_into()?;
            let res = match &operation {
                msg::Operation::Copy(copy) => self.copy(copy),
                msg::Operation::Close(close) => self.close(close),
                msg::Operation::Delete(delete) => self.delete(delete),
                msg::Operation::Open(open) => self.open(open),
                msg::Operation::Read(read) => self.read_into_mmap(read),
                msg::Operation::Write(write) => self.write_from_mmap(write),
            };
            log::trace!("operation completed: {operation:?} with {res:?}");
            res.map_err(|err| Status::internal(err.to_string()))?;
        } else {
            // TODO: Error
            return Err(Status::invalid_argument("operation must be set"));
        };

        Ok(Response::new(communication::SubmissionResponse {}))
    }
}

// NOTE: this is multi-threaded for performance reasons but does have the
// side-effect of making some request sequences non-deterministic.
// For example, [open, read, close] may complete in any order!
// The expectation is that client will wait for completion of each operation
// before sending the next.
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let addr = "[::1]:9999".parse()?;
    let sq = SubmissionQueue::default();

    Server::builder()
        .add_service(SubmissionQueueServer::new(sq))
        .serve(addr)
        .await?;

    Ok(())
}

fn create_file(path: impl AsRef<Path>, size: u64) -> std::io::Result<std::fs::File> {
    let path = path.as_ref();
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(path)?;
    log::trace!("created file at {path:?}");
    file.set_len(size)?;
    log::trace!("set_len at {path:?} to {size:?}");
    Ok(file)
}
