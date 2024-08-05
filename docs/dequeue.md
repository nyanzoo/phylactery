# Queue
A FIFO persisted collection, which can grow as needed and should require little to no compaction.

## Design
At a high-level there are two layers to the deque implementation. First is an in-memory buffer (a node in the deque) for reads and writes to be performant at the cost of copying data when it comes to permanent data residence and potential data loss if crash before flush. Second is a series of files that are the permanent locations of data and managed at the deque level (above node).

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
- Size: returns the number of entries in the buffer
- Capacity: returns the maximum number of entries the buffer can hold

### Configuration
The queue should be configurable with the following options:
- **Path**: the path to the directory where the queue files will be stored
- **File Size**: the size of each file in the queue
- **Max Size**: the maximum amount of bytes the queue can hold
- **Default TTL**: the default TTL for entries in the queue
