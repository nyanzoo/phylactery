# Buffer
A buffer is an abstraction of some storage entity, it could be in memory with a vector, it could be fixed-size like an array, or even backed by a file.

## Design
Buffers will provide an abstraction layer that allows for other components to be agnostic of the underlying storage mechanism. This will allow for the storage mechanism to be changed without having to change the components that use the buffer.

There will also, potentially, be an `AlignedBuffer` type that can wrap other buffers to maintain alignment.

### API
The buffer will have the following API (pending)

#### decode_at
Decode the entry of some type `T` at the given index into the given buffer.

#### encode_at
Encode the given entry of some type `T` at the given index into the given buffer.

#### read_at
Read at the given index into the given buffer.

#### write_at
Write at the given index into the given buffer.

#### capacity
Return the capacity, or how many bytes would fit, for the buffer.



