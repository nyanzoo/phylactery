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
