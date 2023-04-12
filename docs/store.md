# Store
The store is a simple key-value store that stores data to disk and tries to keep recently-used entries in memory for fast reads. 

## Design
Data will be stored to disk using [queues](./queue.md) and will use [mmap](https://man7.org/linux/man-pages/man2/mmap.2.html) for storing the location on disk from which an entry lies. 

```
+----------------------------------------------------------------+
| File -- 4 bytes, the file where data is stored                 |
+----------------------------------------------------------------+
| Offset -- 4 bytes, the offset in the file where data is stored |
+----------------------------------------------------------------+
```

At the very least, 8 bytes are required for a single entry. This might grow if it is decided to add integrity checks to the mmap data. 

In order to access an entry the following needs to happen:
```
                    | hash(key) -> location in mmap
                    ▼
+----------------------------------------------------------------+
| Read the mmap data for location matching hash                  | MMAP
+----------------------------------------------------------------+
                    |
                    | pass location to queue, and add/update/delete on disk
                    |
                    ▼
+----------------------------------------------------------------+
| Read the entry from the queue                                  | QUEUE
+----------------------------------------------------------------+
                    |
                    | update cache
                    |
                    ▼
+----------------------------------------------------------------+
| Add to Cache                                                   | Cache
+----------------------------------------------------------------+
```

With this design, repeated reads will be fast as the data will be in the cache. Writes will be slower as the write-side mmap will need to be flushed to disk; see [queue](./queue.md) for details.

However, in order to make this work, there needs to be a map-like data structure implemented over mmap. This will likely be a binary-tree or B-Tree.

## Data Layout & Integrity
same as [queue](./queue.md)

## MAPS!
As seen from above diagram there are a number mmaps used for both functionality and speed. The following is a list of all the mmaps used and their purpose:

- **LOCATION**: Used for storing the location of an entry on disk. This is used to quickly find the location of an entry on disk.
- **CACHE**: Used for storing the most recently used entries in memory. This is used to quickly find entries with hope of avoiding disk reads.
- **WRITE**: Used for writing entries to disk. This is used to limit syscalls and allow for async writes to disk. Dependency from [queue](./queue.md).
- **READ**: Used for reading entries from disk. This is used to limit syscalls and allow for async reads from disk. Dependency from [queue](./queue.md).

## Compaction
Because this is a key-value store, it is possible to have fragmentation on disk where the files from [queue](./queue.md) are not full. This can be solved by compacting the files. This can be done by copying the data from the files to a new file and then deleting the old files. This will require a lock to be held on the store to prevent any new writes to compacting files from happening while the compaction is happening. 

This also requires new API fns for the queue:
1. **Copy**: Copies the data from one or more files to another. This will be used to copy the data from the old files to the new file.
2. **Delete**: Deletes an entry in the queue at any location.

## API
The store will expose the following API:
- **Get**: Gets an entry from the store.
- **Set**: Sets an entry in the store.
- **Delete**: Deletes an entry from the store.

## Configuration
The store will be configurable with the following options:
- **Path**: The path to the store.
- **Cache Size**: The size of the cache in bytes.

Additional configuration options are found in [queue](./queue.md).
