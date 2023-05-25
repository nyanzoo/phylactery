# RingBuffer
A [ring buffer](https://en.wikipedia.org/wiki/Circular_buffer) is a data structure that has a fixed set of elements. When the buffer is full, old elements must be ejected/written over to make room for new elements.

## Design
The ring buffer is implemented as a wrapper over a [buffer](./buffer.md)'. It is a thread-safe, lock-free, fixed-size, circular buffer. The buffer allows for one reader and one writer.

To allow for lockless implementation, we use a set of atomic counters to track where the next read and write should occur. One can think of the buffer being divided into readable and writable sections. Where the readable section is always behind the writable section and the writable always behind the readable section.

For example:
```
The buffer has been written around the ring to be a wrap-around, where the read now appear after the write section.
+-----------------------+-----------------------+-----------------------+
| Write                 | Read                  | Read                  |
+-----------------------+-----------------------+-----------------------+
```

### Data Layout & Integrity
Every entry must have a layout *similar to the following:
```
+-------------------------------------------------------------------------------------+
| Metadata Hash -- 4 bytes, crc32 of the rest of Metadata                             |
+-------------------------------------------------------------------------------------+
| Version -- 8 bytes, version of the entry (8 bytes is also enough for small string)  |
+-------------------------------------------------------------------------------------+
| MASK -- 4 bytes, mask for the ring buffer (helps with reconstruction)               |
+-------------------------------------------------------------------------------------+
| Size -- 4 bytes, size of the data being stored (4 bytes is plenty)                  |
+-------------------------------------------------------------------------------------+
| RPTR -- 8 bytes, read ptr for the ring buffer                                       |
+-------------------------------------------------------------------------------------+
| WPTR -- 8 bytes, write ptr for the ring buffer                                      |
+-------------------------------------------------------------------------------------+
| Entry -- 8 bytes, entry number                                                      |
+-------------------------------------------------------------------------------------+
| Data Hash -- 4 bytes, crc32 of the data                                             |
+-------------------------------------------------------------------------------------+
| Data -- variable length, MUST match the size in the metadata                        |
+-------------------------------------------------------------------------------------+
```

The metadata hash is used to verify the integrity of the metadata and data hash is used to verify the integrity of the data. The version is used to allow for future changes to the metadata layout.

The metadata mask is used to allow for reconstruction of the ring buffer if using a persistent buffer as the backing buffer. The mask helps find the start of the first metadata entry in the buffer.

### Pointers
This implementation requires 2 pointers to map to the beginning of the readable and writable sections. The pointers are stored in the metadata, though might later be stored elsewhere, like the beginning of the buffer if speed is a problem. These pointers are atomic u64s; so only 16 total bytes are used to store the pointers. To know about wrap around possibilities, the entry field is used to see which metadata is last.

The read pointer might not be updated on reads, in the sense of persisting the value in a new metadata entry, this can mean that if the buffer is restored, data might be received more than once! This is a trade-off to avoid modifying the buffer in both paths.

Additionally, a non-stored peek pointer might be used to allow for peeking at the next entry without advancing the read pointer (non-destructive reads).

### API
The ring buffer exposes the following public methods:
- Push: adds a new entry to the buffer
- Pop: removes the next entry from the buffer
- Peek: returns the next entry from the buffer without removing it, can be called multiple times to iterate over the buffer
- Size: returns the number of entries in the buffer
- Capacity: returns the maximum number of entries the buffer can hold, for fixed size implentation only

### Configuration
The ring buffer should be configurable for the following options:
- **Path**: the path to the file to use for the ring buffer
- **Size**: the maximum number of bytes the buffer can hold
