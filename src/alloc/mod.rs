use std::{
    cell::UnsafeCell,
    io::{Cursor, Read, Write},
    mem::size_of,
    ptr::NonNull,
};

use necronomicon::{Decode, Encode};

use crate::buffer::Buffer;

mod error;
pub(crate) use error::Error;

// We need to be able to recover from a crash.
// We can only do this if the Node contains info on
// whether it is free or not.
struct Node {
    next: usize,
    prev: usize,
}

const NODE_SIZE: usize = size_of::<Node>();
const NODE_DATA_OFFSET: usize = NODE_SIZE;
const SENTINEL_SIZE: usize = size_of::<usize>();

impl<R> Decode<R> for Node
where
    R: Read,
{
    fn decode(buf: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let next = usize::decode(buf)?;
        let prev = usize::decode(buf)?;
        Ok(Node { next, prev })
    }
}

impl<W> Encode<W> for Node
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        self.next.encode(writer)?;
        self.prev.encode(writer)?;
        Ok(())
    }
}

pub struct Entry<B>
where
    B: Buffer,
{
    data_start: usize,
    data_end: usize,
    node: usize,
    buffer: NonNull<B>,
}

impl<B> Entry<B>
where
    B: Buffer,
{
    pub fn data<'a, T>(&'a self) -> Result<T, Error>
    where
        T: Decode<&'a [u8]>,
    {
        let buf = self.buffer_ref().as_ref();
        let mut buf = &buf[self.data_start..self.data_end];
        let t = T::decode(&mut buf)?;
        Ok(t)
    }

    pub fn update<'a, T>(&'a mut self, t: &T) -> Result<(), Error>
    where
        T: Encode<Cursor<&'a mut [u8]>>,
    {
        let start = self.data_start;
        let end = self.data_end;
        let buf = self.buffer_mut().as_mut();
        let buf = &mut buf[start..end];
        let mut buf = Cursor::new(buf);
        t.encode(&mut buf)?;
        Ok(())
    }

    pub fn buffer_ref(&self) -> &B {
        unsafe { self.buffer.as_ref() }
    }

    pub fn buffer_mut(&mut self) -> &mut B {
        unsafe { self.buffer.as_mut() }
    }
}

impl<B> Drop for Entry<B>
where
    B: Buffer,
{
    fn drop(&mut self) {
        let buffer = self.buffer_ref();
        let mut next = buffer
            .decode_at::<Node>(self.node, NODE_SIZE)
            .expect("drop node");
        let mut sentinel = buffer.decode_at::<usize>(0, NODE_SIZE).expect("sentinel");

        if sentinel > self.node {
            next.next = sentinel;
            next.prev = 0;
            sentinel = self.node;
            buffer
                .encode_at(self.node, NODE_SIZE, &next)
                .expect("encode node");
            buffer
                .encode_at(0, SENTINEL_SIZE, &sentinel)
                .expect("encode sentinel");
        } else {
            next.prev = sentinel;
            next.next = usize::MAX;
            buffer
                .encode_at(self.node, NODE_SIZE, &next)
                .expect("encode node");
        }
    }
}

pub(crate) struct FixedSizeAllocator<B, const DATA_SIZE: usize>
where
    B: Buffer,
{
    buffer: UnsafeCell<B>,
}

impl<B, const DATA_SIZE: usize> FixedSizeAllocator<B, DATA_SIZE>
where
    B: Buffer,
{
    pub(crate) fn new(buffer: B) -> Result<Self, Error> {
        // sentinel node is first usize and always points to first
        // available node.
        let mut buf = [0; size_of::<usize>()];
        buffer.read_at(&mut buf, 0)?;
        let mut next_free = usize::from_be_bytes(buf);
        // We need to update to after sentinel node.
        // Prepopulate the buffer.
        if next_free == 0 {
            next_free += size_of::<usize>();
            buffer.encode_at(0, size_of::<usize>(), &next_free)?;
            let mut prev = 0;
            let mut next = next_free;
            while ((next + NODE_SIZE) as u64) < buffer.capacity() {
                let pos = next;
                next += NODE_SIZE + DATA_SIZE;
                let node = Node { next, prev };
                prev = pos;
                buffer
                    .encode_at(pos, NODE_SIZE, &node)
                    .expect("encode node");
            }
        }

        Ok(Self {
            buffer: UnsafeCell::new(buffer),
        })
    }

    // Should only be called on startup.
    pub(crate) fn recovered_entries(&mut self) -> Result<Vec<Entry<B>>, Error> {
        let mut frees = vec![];

        let mut start = SENTINEL_SIZE;
        while ((start + NODE_SIZE) as u64) <= self.capacity() {
            let pos = start;
            start += NODE_SIZE + DATA_SIZE;

            let node = self.decode_node(pos)?;

            if node.next != usize::MAX && node.prev != usize::MAX {
                continue;
            }

            let data_start = start + NODE_DATA_OFFSET;
            let data_end = data_start + DATA_SIZE;

            let free = Entry {
                data_start,
                data_end,
                node: pos,
                buffer: NonNull::new(self.buffer.get()).expect("null"),
            };

            frees.push(free);
        }

        Ok(frees)
    }

    pub(crate) fn alloc(&self) -> Result<Entry<B>, Error> {
        let mut next_free = self.sentinel()?;

        if ((next_free + NODE_SIZE) as u64) >= self.capacity() {
            return Err(Error::OutOfMemory {
                total: self.capacity(),
                used: next_free,
            });
        }

        let mut next = self.decode_node(next_free)?;

        let data_start = next_free + NODE_DATA_OFFSET;
        let data_end = data_start + DATA_SIZE;

        let free = Entry {
            data_start,
            data_end,
            node: next_free,
            buffer: NonNull::new(self.buffer.get()).expect("null"),
        };

        next_free = next.next;
        next.prev = usize::MAX;
        next.next = usize::MAX;

        self.update_sentinel(next_free)?;

        Ok(free)
    }

    fn capacity(&self) -> u64 {
        let buffer = unsafe { NonNull::new(self.buffer.get()).expect("null").as_ref() };
        buffer.capacity()
    }

    fn decode_node(&self, off: usize) -> Result<Node, Error> {
        if off + NODE_SIZE > self.capacity() as usize {
            return Err(Error::BadNode(off));
        }
        let buffer = unsafe { NonNull::new(self.buffer.get()).expect("null").as_ref() };
        let node = buffer.decode_at::<Node>(off, NODE_SIZE)?;
        Ok(node)
    }

    fn sentinel(&self) -> Result<usize, Error> {
        let buffer = unsafe { NonNull::new(self.buffer.get()).expect("null").as_ref() };
        let sentinel = buffer.decode_at(0, size_of::<usize>())?;

        Ok(sentinel)
    }

    fn update_sentinel(&self, sentinel: usize) -> Result<(), Error> {
        let buffer = unsafe { NonNull::new(self.buffer.get()).expect("null").as_ref() };
        buffer.encode_at(0, size_of::<usize>(), &sentinel)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::InMemBuffer;

    use super::{FixedSizeAllocator, NODE_SIZE, SENTINEL_SIZE};

    #[test]
    fn alloc_free() {
        const DATA_SIZE: usize = 8;

        let alloc =
            FixedSizeAllocator::<InMemBuffer, DATA_SIZE>::new(InMemBuffer::new(1024)).unwrap();

        let mut entries = vec![];

        let max_entries = (1024 - SENTINEL_SIZE) / (NODE_SIZE + DATA_SIZE);
        for _ in 0..max_entries {
            let entry = alloc.alloc().unwrap();
            entries.push(entry);
        }

        assert!(alloc.alloc().is_err());

        entries.remove(max_entries / 2); // should be some middle entry

        entries.push(alloc.alloc().unwrap());

        for free in entries.into_iter() {
            drop(free);
        }

        for _ in 0..max_entries {
            alloc.alloc().unwrap();
        }
    }
}
