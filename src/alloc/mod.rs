use std::{
    io::{Cursor, Read, Write},
    mem::size_of,
    rc::Rc,
};

use log::trace;
use necronomicon::{Decode, DecodeOwned, Encode, Owned};

use crate::{buffer::Buffer, Error};

// We need to be able to recover from a crash.
// We can only do this if the Node contains info on
// whether it is free or not.
#[derive(Debug)]
struct Node {
    next: usize,
    prev: usize,
}

const NODE_SIZE: usize = size_of::<Node>();
const NODE_DATA_OFFSET: usize = NODE_SIZE;
const SENTINEL_SIZE: usize = size_of::<u64>() * 2;

#[derive(Debug)]
struct Sentinel {
    next: u64,
    count: u64,
}

impl From<(u64, u64)> for Sentinel {
    fn from((next, count): (u64, u64)) -> Self {
        Self { next, count }
    }
}

impl From<u128> for Sentinel {
    fn from(value: u128) -> Self {
        Self {
            next: (value >> 64) as u64,
            count: value as u64,
        }
    }
}

impl From<&Sentinel> for u128 {
    fn from(value: &Sentinel) -> Self {
        ((value.next as u128) << 64) | (value.count as u128)
    }
}

impl<R> Decode<R> for Sentinel
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let next = u64::decode(reader)?;
        let count = u64::decode(reader)?;
        Ok(Self { next, count })
    }
}

impl<W> Encode<W> for Sentinel
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), necronomicon::Error> {
        let value: u128 = self.into();
        value.encode(writer)?;
        Ok(())
    }
}

impl<R> Decode<R> for Node
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Self, necronomicon::Error>
    where
        Self: Sized,
    {
        let next = usize::decode(reader)?;
        let prev = usize::decode(reader)?;
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
    buffer: Rc<B>,
}

impl<B> Entry<B>
where
    B: Buffer,
{
    #[cfg(test)]
    pub fn data<'a, T>(&'a self) -> Result<T, Error>
    where
        T: Decode<Cursor<&'a [u8]>>,
    {
        let t = self
            .buffer
            .decode_at::<T>(self.data_start, self.data_end - self.data_start)?;
        Ok(t)
    }

    pub fn data_owned<'a, T, O>(&'a self, buffer: &mut O) -> Result<T, Error>
    where
        T: DecodeOwned<Cursor<&'a [u8]>, O>,
        O: Owned,
    {
        let t = self.buffer.decode_at_owned(
            self.data_start,
            self.data_end - self.data_start,
            buffer,
        )?;
        Ok(t)
    }

    pub fn update<'a, T>(&'a mut self, t: &T) -> Result<(), Error>
    where
        T: Encode<Cursor<&'a mut [u8]>>,
    {
        trace!(
            "data_start: {}, data_end: {}",
            self.data_start,
            self.data_end
        );
        self.buffer
            .encode_at(self.data_start, self.data_end - self.data_start, t)?;
        Ok(())
    }
}

impl<B> Drop for Entry<B>
where
    B: Buffer,
{
    fn drop(&mut self) {
        let mut sentinel = self
            .buffer
            .decode_at::<Sentinel>(0, NODE_SIZE)
            .expect("sentinel");

        let mut next = self
            .buffer
            .decode_at::<Node>(self.node, NODE_SIZE)
            .expect("drop node");

        if sentinel.next > self.node as u64 {
            next.next = sentinel.next as usize;
            next.prev = 0;
            sentinel.next = self.node as u64;
            sentinel.count -= 1;
            self.buffer
                .encode_at(self.node, NODE_SIZE, &next)
                .expect("encode node");
            self.buffer
                .encode_at(0, SENTINEL_SIZE, &sentinel)
                .expect("encode sentinel");
        } else {
            next.prev = sentinel.next as usize;
            next.next = usize::MAX;
            sentinel.count -= 1;
            self.buffer
                .encode_at(self.node, NODE_SIZE, &next)
                .expect("encode node");
            self.buffer
                .encode_at(0, SENTINEL_SIZE, &sentinel)
                .expect("encode sentinel");
        }
    }
}

// TODO: drop the const and make it a parameter to new. This way we can have it be parameterized
pub(crate) struct FixedSizeAllocator<B>
where
    B: Buffer,
{
    buffer: Rc<B>,
    data_size: usize,
}

impl<B> FixedSizeAllocator<B>
where
    B: Buffer,
{
    pub(crate) fn new(buffer: B, data_size: usize) -> Result<Self, Error> {
        // sentinel node is first usize and always points to first
        // available node.
        let mut sentinel = buffer.decode_at::<Sentinel>(0, SENTINEL_SIZE)?;
        // We need to update to after sentinel node.
        // Prepopulate the buffer.
        if sentinel.count == 0 {
            sentinel.next += SENTINEL_SIZE as u64;
            buffer.encode_at(0, SENTINEL_SIZE, &sentinel)?;
            let mut prev = 0;
            let mut next = sentinel.next as usize;
            while ((next + NODE_SIZE) as u64) < buffer.capacity() {
                let pos = next;
                next += NODE_SIZE + data_size;
                let node = Node { next, prev };
                prev = pos;
                buffer
                    .encode_at(pos, NODE_SIZE, &node)
                    .expect("encode node");
            }
        }

        Ok(Self {
            buffer: Rc::new(buffer),
            data_size,
        })
    }

    // Should only be called on startup.
    pub(crate) fn recovered_entries(&mut self) -> Result<Vec<Entry<B>>, Error> {
        let mut frees = vec![];

        let mut start = SENTINEL_SIZE;
        while ((start + NODE_SIZE) as u64) <= self.capacity() {
            let pos = start;
            start += NODE_SIZE + self.data_size;

            let node = self.decode_node(pos)?;

            if node.next != usize::MAX && node.prev != usize::MAX {
                continue;
            }

            // We skip a node + data above, so just rollback the data to be at node offset and get correct data offsets.
            let data_start = start - self.data_size;
            let data_end = data_start + self.data_size;

            let free = Entry {
                data_start,
                data_end,
                node: pos,
                buffer: self.buffer.clone(),
            };

            frees.push(free);
        }

        Ok(frees)
    }

    pub(crate) fn alloc(&self) -> Result<Entry<B>, Error> {
        let Sentinel {
            next: next_free,
            count,
        } = self.sentinel()?;
        if next_free == u64::MAX
            || (next_free + NODE_SIZE as u64 + self.data_size as u64) > self.capacity()
        {
            return Err(Error::OutOfMemory {
                total: self.capacity(),
                used: next_free,
            });
        }

        let next_free = next_free as usize;
        let mut next = self.decode_node(next_free)?;

        let data_start = next_free + NODE_DATA_OFFSET;
        let data_end = data_start + self.data_size;

        let free = Entry {
            data_start,
            data_end,
            node: next_free,
            buffer: self.buffer.clone(),
        };

        let sentinel = next.next;
        next.prev = usize::MAX;
        next.next = usize::MAX;
        self.update_next(next_free, &next)?;

        self.update_sentinel(Sentinel {
            next: sentinel as u64,
            count: count + 1,
        })?;

        Ok(free)
    }

    fn capacity(&self) -> u64 {
        self.buffer.capacity()
    }

    fn decode_node(&self, off: usize) -> Result<Node, Error> {
        if off + NODE_SIZE > self.capacity() as usize {
            return Err(Error::BadNode(off));
        }
        let node = self.buffer.decode_at::<Node>(off, NODE_SIZE)?;
        Ok(node)
    }

    fn sentinel(&self) -> Result<Sentinel, Error> {
        let sentinel = self.buffer.decode_at(0, SENTINEL_SIZE)?;

        Ok(sentinel)
    }

    fn update_sentinel(&self, sentinel: Sentinel) -> Result<(), Error> {
        self.buffer.encode_at(0, SENTINEL_SIZE, &sentinel)?;

        Ok(())
    }

    fn update_next(&self, pos: usize, node: &Node) -> Result<(), Error> {
        self.buffer.encode_at(pos, NODE_SIZE, node)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::{mem::size_of, rc::Rc};

    use matches::assert_matches;

    use crate::{buffer::InMemBuffer, Error};

    use super::{FixedSizeAllocator, NODE_SIZE, SENTINEL_SIZE};

    #[test_case::test_case(1)]
    #[test_case::test_case(8)]
    #[test_case::test_case(16)]
    #[test_case::test_case(32)]
    #[test_case::test_case(64)]
    #[test_case::test_case(128)]
    #[test_case::test_case(1024)]
    #[test_case::test_case(9)]
    #[test_case::test_case(57)]
    #[test_case::test_case(65)]
    fn alloc_free(data_size: usize) {
        let alloc =
            FixedSizeAllocator::<InMemBuffer>::new(InMemBuffer::new(4096), data_size).unwrap();

        let mut entries = vec![];

        loop {
            match alloc.alloc() {
                Ok(entry) => entries.push(entry),
                Err(Error::OutOfMemory { .. }) => break,
                Err(err) => panic!("unexpected error: {:?}", err),
            }
        }

        for entry in &mut entries {
            if data_size < size_of::<usize>() {
                assert_matches!(entry.update(&7u8), Ok(_));
            } else {
                // need space for the len
                assert_matches!(
                    entry.update(&vec![7u8; data_size - size_of::<usize>()]),
                    Ok(_)
                );
            }
        }

        // drop half the entries
        let mut entries = entries
            .into_iter()
            .enumerate()
            .skip_while(|(i, _)| i % 2 == 0)
            .map(|(_, entry)| entry)
            .collect::<Vec<_>>();

        let count = entries.len();

        loop {
            match alloc.alloc() {
                Ok(entry) => entries.push(entry),
                Err(Error::OutOfMemory { .. }) => break,
                Err(err) => panic!("unexpected error: {:?}", err),
            }
        }

        assert_ne!(entries.len(), count);
    }

    #[test]
    fn recover() {
        let data_size = 8;
        let max_entries = (1024 - SENTINEL_SIZE) / (NODE_SIZE + data_size);

        let buffer = InMemBuffer::new(1024);
        let new_buffer;
        let mut entries = vec![];
        {
            let alloc = FixedSizeAllocator::<InMemBuffer>::new(buffer, data_size).unwrap();

            for _ in 0..max_entries {
                let entry = alloc.alloc().unwrap();
                entries.push(entry);
            }

            assert!(alloc.alloc().is_err());

            entries.remove(max_entries / 2); // should be some middle entry

            entries.push(alloc.alloc().unwrap());
            new_buffer = alloc.buffer.clone();
        }

        for free in entries.iter_mut() {
            free.update(&42u64).unwrap();
        }

        let mut alloc = FixedSizeAllocator::<InMemBuffer>::new(
            unsafe { &*Rc::into_raw(new_buffer) }.clone(),
            data_size,
        )
        .unwrap();

        let recovered = alloc.recovered_entries().unwrap();

        assert_eq!(recovered.len(), max_entries);

        for free in recovered.into_iter() {
            assert_eq!(free.data::<u64>().unwrap(), 42);
        }
    }

    #[test]
    fn recover() {
        const DATA_SIZE: usize = 8;
        let max_entries = (1024 - SENTINEL_SIZE) / (NODE_SIZE + DATA_SIZE);

        let buffer = InMemBuffer::new(1024);
        let new_buffer;
        let mut entries = vec![];
        {
            let alloc = FixedSizeAllocator::<InMemBuffer, DATA_SIZE>::new(buffer).unwrap();

            for _ in 0..max_entries {
                let entry = alloc.alloc().unwrap();
                entries.push(entry);
            }

            assert!(alloc.alloc().is_err());

            entries.remove(max_entries / 2); // should be some middle entry

            entries.push(alloc.alloc().unwrap());
            new_buffer = alloc.buffer.clone();
        }

        for free in entries.iter_mut() {
            free.update(&42u64).unwrap();
        }

        let mut alloc = FixedSizeAllocator::<InMemBuffer, DATA_SIZE>::new(
            unsafe { &*Rc::into_raw(new_buffer) }.clone(),
        )
        .unwrap();

        let recovered = alloc.recovered_entries().unwrap();

        assert_eq!(recovered.len(), max_entries);

        for free in recovered.into_iter() {
            assert_eq!(free.data::<u64>().unwrap(), 42);
        }
    }
}
