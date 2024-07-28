Two ideas thinking about

1. Create an IO layer that can hold open several files at once and queue up reads and writes to them. This would allow us to asynchronously do the writes. 

2. We can have an event loop that can be used to schedule operations on the kv store. This would allow for optimizing the order of operations. 

The problem is that compaction doesn't work right and needs to be a blocking operation. we cannot read from a location that is being modified. so we need to make sure this ordered. We also need to be able to update the store metadata in a way that is atomic. because if do compaction then the store will point to the wrong location.

We can have an event loop that can be used to schedule operations on the kv store. This would allow for optimizing the order of operations. We can also have the event loop require events use a callback to signal completion. This would allow us to have a single thread that can do all the work.

Do we want to have a separate IO loop? maybe?


We need to break up the store into separate components now too.
We need to have:
1. the lookup table that might allow for chunking to make sure we don't have to keep all the keys in memory
2. the metadata store that keeps track of the location of the data
3. the data store that holds the data

To limit the memory usage we can have metadata folder that is broken down by first byte into 256 files. This would allow for relatively quick lookups. Realistically the metadata should be random enough for a uniform distribution and we should be able to use just a single file for each of the metadata files. We can also probably sort the metadata file by key to make lookups faster.

we will change the dequeue to work by using a dequeue underneath VecDequeue and then we can have a setup that looks like this:

```
pub struct Mapping {
    node: Node,
    file_id: u64,
}

pub struct Dequeue {
/// ...
mapping: VecDeque<Mapping>,
}
```

This way we can have read and write point to the same node for once.

If we also do move towards an event loop then we can make everything single-threaded too

we need to have a graveyard per shard with current design and either way need to have a way to convert from metadata to tombstone. then we need to decide whether we want to have data shards compact automatically or just signal they should be compacted