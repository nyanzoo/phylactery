# RingBuffer
A [ring buffer](https://en.wikipedia.org/wiki/Circular_buffer) is a data structure that has a fixed set of elements. When the buffer is full, old elements must be ejected/written over to make room for new elements.

## Design
The ring buffer is implemented as a wrapper over a [mmap](https://man7.org/linux/man-pages/man2/mmap.2.html)'d file. It is a thread-safe, lock-free, fixed-size, circular buffer. The buffer allows for multiple readers and writers.

To allow for lockless multi-producer, multi-consumer, buffer, we use a set of atomic counters to track where the next read and write should occur. One can think of the buffer being divided into readable and writable sections. Where the readable section is always behind the writable section and the writable always behind the readable section.

For example:
```
The buffer has been written around the ring to be a wrap-around, where the read now appear after the write section.
+-----------------------+-----------------------+-----------------------+
| Write                 | Read                  | Read                  |
+-----------------------+-----------------------+-----------------------+
```

### Data Layout & Integrity
Every entry must have a layout similar to the following:
```
+-------------------------------------------------------------------------------------+
| Metadata Hash -- 4 bytes, crc32 of the rest of Metadata                             |
+-------------------------------------------------------------------------------------+
| Version -- 8 bytes, version of the entry (8 bytes is also enough for small string)  |
+-------------------------------------------------------------------------------------+
| Size -- 4 bytes, size of the data being stored (4 bytes is enough for 4GB)          |
+-------------------------------------------------------------------------------------+
| TTL -- 4 bytes, time to live in seconds (4 bytes is enough for 136 years)           |
+-------------------------------------------------------------------------------------+
| ID -- 8 bytes, unique id of the entry (for internal tracking)                       |
+-------------------------------------------------------------------------------------+
| Data Hash -- 4 bytes, crc32 of the data                                             |
+-------------------------------------------------------------------------------------+
| Data -- variable length, MUST match the size in the metadata                        |
+-------------------------------------------------------------------------------------+
```

The metadata hash is used to verify the integrity of the metadata and data hash is used to verify the integrity of the data. The version is used to allow for future changes to the metadata layout.

### Pointers
This implementation requires 2 pointers to map to the beginning of the readable and writable sections. The pointers are stored in front of the file to not waste space. These pointers are atomic u32s; so only 8 total bytes are used to store the pointers.

Additionally, a non-stored peek pointer is used to allow for peeking at the next entry without advancing the read pointer (non-destructive reads).

### API
The ring buffer exposes the following public methods:
- Push: adds a new entry to the buffer
- Pop: removes the next entry from the buffer
- Peek: returns the next entry from the buffer without removing it, can be called multiple times to iterate over the buffer
- Size: returns the number of entries in the buffer
- Capacity: returns the maximum number of entries the buffer can hold

### Configuration
The ring buffer should be configurable for the following options:
- **Path**: the path to the file to use for the ring buffer
- **Size**: the maximum number of bytes the buffer can hold
- **TTL**: the default time to live for entries in the buffer
