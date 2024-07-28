# KV Store:
## Lookup table
The lookup table needs to be used as a way of matching key
to metadata. The metadata is all the info required to find the data in the dequeue.

Insert will add an entry to this table.
Get will search for an entry in this table.
Delete will remove an entry from this table.

This table needs to be repopulated on startup.

## Metadata
The metadata is a struct that contains the following:
* Key - needed to repopulate the lookup table
* file + offset - needed to find the data
* size - needed to know how much data to read/skip
* crc - needed to verify the data

we need a data structure that works well for insert, get and delete.
A ringbuffer has the problem of fragmentation; a delete in the middle of the buffer will leave a hole. A dequeue might work better, this would require compaction to remove the holes, much like if we were using a ringbuffer. But the advantage of dequeue is the updating of the lookup for any shuffled metadata is less and done in batches (files). ~~However, the dequeue has a lot of problems with GC, as nodes are currently in mem, meaning file swap does nothing after the read.~~

Also with a ringbuffer, the worst case situation is we think we are full, but we are not, and instead have lots of empty space in between the write&read ptrs for the ringbuffer. This might not be a regular issue though, as given a large enough ring buffer and small enough (on avg) data size, we should be fine? 

Maybe we need a new data structure. As well as setting a fixed size on metadata. If we do that then we can effectively manage fragmentation and deletion. But, what seems to be clear at this point is that we need to do small writes to the disk for the metadata (insert/update/delete) in all the cases I can think of... if this is true, we might as well make separate files for each metadata? Although this will surely be slow on reads as well as writes.

## Data

