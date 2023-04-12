# Queue
A FIFO persisted collection, which can grow as needed and should require little to no compaction.

## Design
At a high-level there are two layers to the queue implementation. First is a [mmap](https://man7.org/linux/man-pages/man2/mmap.2.html)'d buffer for reads and writes to offer performance at the cost of copying data when it comes to permanent data residence and potential flash memory degradation (TODO: confirm this is true). Second is a series of files that are the permanent locations of data.

The client side will use an [IPC](https://en.wikipedia.org/wiki/Inter-process_communication) to communicate with the IO execution side (daemon). The [daemon](./daemon.md) will be responsible for managing its own copy of the mmaps and doing actual IO operations to disk. The client may send requests to the daemon to perform IO and thus be able to treat all IO as asynchronous.

The file layout on disk will look something like this:
```
[File 1]->[File 2]->[File 3]->[File 4]->[File 5]->[File 6]->[File 7]->[File 8]->[File 9]->[File 10]
```

Each file is of a fixed side and multiple files maybe required to reconstruct a single entry or a file may contain multiple entries.

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

### API
The queue exposes the following public methods:
- Push: adds a new entry to the buffer
- Pop: removes the next entry from the buffer
- Peek: returns the next entry from the buffer without removing it, can be called multiple times to iterate over the buffer
- Size: returns the number of entries in the buffer
- Capacity: returns the maximum number of entries the buffer can hold

### Configuration
The queue should be configurable with the following options:
- **Path**: the path to the directory where the queue files will be stored
- **File Size**: the size of each file in the queue
- **Max Size**: the maximum amount of bytes the queue can hold
- **Default TTL**: the default TTL for entries in the queue
