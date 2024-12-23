# phylactery
[Phylactery](./docs/design.md) is a collection of useful disk-based structures for [transaction log](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html), queue and kv-store.

[![codecov](https://codecov.io/gh/nyanzoo/phylactery/branch/master/graph/badge.svg?token=SJS7Y0CMGI)](https://codecov.io/gh/nyanzoo/phylactery)

## collections
All collections are configurable to control maximum disk usage.

### [Queue](./docs/queue.md)
A series of items stored in the order in which they are added.

### [RingBuffer](./docs/ring-buffer.md)
A staticly set sized buffer that functions as a doubly-linked-list.

### [KVStore](./docs/kvstore.md)
A key-value store that stores everything to disk.

### [TransactionLog](./docs/transaction-log.md)
A structure that represents the order in which operations are performed on a store.
This is useful in the event one wants to recreate an *exact* replica.

## Coverage
see `https://github.com/taiki-e/cargo-llvm-cov#installation` for instructions.

## Contributing
TODO

## TODO
- [ ] store needs to have patch prepare and commit semantics with the ability to determine if a duplicate request came in (idempotency). This also has to be persisted to disk. We could just have an mmap of some size to fit a reasonable amount of patches in memory. When a patch completes we zero out that location in the mmap. We can also use a bloom filter to try optimize the search for duplicates.
- [ ] need to be able to transfer the contents of the store efficiently to another store, this can be done by just copying the files, but it would be better to only send the files we care about. One way to do this is to limit the key size and other fields to make sure we can have a fixed size block for storing what files we care about. The other trick is have a counter/hash for the store to determine if the store has changed. But we can probably start with just zipping up the files and sending them over?
- [ ] need to be able to resend messages in the event of client failure, this can be done  by just keeping a queue of messages that have been sent
- [ ] would be good to add etags to the store to prevent overwriting data
