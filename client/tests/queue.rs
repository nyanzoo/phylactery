use std::time::Instant;

use client::{communication::submission_queue_client::SubmissionQueueClient, queue::queue};

#[tokio::test]
async fn read_write() {
    let dir = tempfile::tempdir().unwrap();
    let file_size = 4096;
    let mut client = SubmissionQueueClient::connect("http://[::1]:9999")
        .await
        .unwrap();
    let (writer, reader) = queue(dir.into_path(), file_size, &mut client)
        .await
        .unwrap();

    for _ in 0..10_000 {
        writer.push(b"kittens").await.unwrap();
        let mut buf = vec![0u8; 1024];
        reader.pop(&mut buf).await.unwrap();
        assert_eq!(&buf[..7], b"kittens");
    }
}

// at 50s to write 10k 7byte messages, it is obvious a different approach is needed...
// this just cannot scale. 
// Let's try instead to write in a separate thread, and see if that helps.
// If not, then we need to get creative.
//
// New idea:
// at high level we will have IO thread for IO, an input-network thread and an output-network thread.
// We will need to have some thread-safe store for holding the data
// Let's give up on async, we are not trying to fit into iotedge, so who cares now.
// This means that all the store primitives need to be thread-safe where we gaurantee that entries
// do not overlap and that anytime we do a read there was something written there for an entry.
// queue needs to be more, we really need a deque, so that we can do random access.
// we likely need to be able to remove from the middle and maybe even shuffle some entries around.
// later the transaction log will be based on ring-buffer and the table/store will be based on deque.
// the table will also need a btree impl that can be used for quickly finding the location of an entry in deque.
// think about this more later...
//
#[tokio::test]
async fn read_write_bench() {
    let dir = tempfile::tempdir().unwrap();
    let file_size = 4096;
    let mut client = SubmissionQueueClient::connect("http://[::1]:9999")
        .await
        .unwrap();
    let (writer, reader) = queue(dir.into_path(), file_size, &mut client)
        .await
        .unwrap();

    let start = Instant::now();
    for _ in 0..10_000 {
        writer.push(b"kittens").await.unwrap();
    }
    let end = Instant::now();
    println!("write: {:?}", end - start);

    let start = Instant::now();
    for _ in 0..10_000 {
        let mut buf = vec![0u8; 1024];
        reader.pop(&mut buf).await.unwrap();
        assert_eq!(&buf[..7], b"kittens");
    }
    let end = Instant::now();
    println!("read: {:?}", end - start);
}
