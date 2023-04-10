# IO Pros/Cons
It is important to understand the pros and cons of each IO method to make an informed decision on which to use. Some of these are system specific and some are more general, but all have the same fundamental problem of IO being slow.

To understand why IO is always slower than an in-memory implementation, consider the simplified diagram below:

```
+-------------------+
| Application       |
+-------------------+
        |
*================================*
| +---------------------------+  |
| | L(N) Cache                |  |
| +---------------------------+  |
|              |                 |
| +---------------------------+  |
| | RAM                       |  |
| +---------------------------+  |
|              |                 |
| +---------------------------+  |
| | Disk                      |  |
| +---------------------------+  |
*================================*
```

The diagram shows an application running in userland and the layers of access from the OS perspective. The OS has access to the L1-3 caches, which are the fastest but small. Ram is simplified here, but there is a collection of pages that are mapped to physical memory and in Ram there will be some subset of this virtual memory that holds relevant data for currently running processes. Finally, there is the disk, which is the slowest but largest. It holds long term data that is likely not currently being used. So, when a disk IO operation is performed to get data for use in the application, it must check if it is cached, in virtual memory, or on disk. If it is on disk, it must be read into virtual memory and then into the cache before it can be used by the application. This is a lot of overhead and is why IO is slow.

## Threadpool IO
Threadpool based IO is the practice of delegated threads running IO tasks and then sending alerts to callers when that IO completes, thus emulating async IO. The threadpool is highly portable and does not require any specific OS/kernel features to work. It does, however, require that users make efficient use of the threads and IO calls. By this, it is much better to send a single IO operation for 1MB of data than 1,000 IO operations for 1KB of data. This is because the threadpool is limited in size and the IO operations are expensive. The other reason for doing this is that, at least on flash, the IO operations do something called expansion which is where IO is performed in size of the granularity of [block](https://www.oo-software.com/en/docs/whitepaper/whitepaper_ssd.pdf)s. What this means is that if you write 1KB of data, it will actually write 4KB of data. This is because the smallest unit of IO is a block and the smallest unit of erasure is a page.

## AIO
This is a type of IO that exists on Nix-based systems. It is a kernel level async IO, where IO is submitted to the kernel and a [signal](https://man7.org/linux/man-pages/man7/signal.7.html) is used to be notified of IO completion. This has personally felt really awkward to use and whether signals or callbacks are used is system dependent. Furthermore, in order to reasonably use this option the kernel needs to be told to increase the total amount of AIO operations and even with this, it is probably required to track in userland the amount of AIO operations being performed so that they may be buffered or even blocked.

## IO_URING
[io_uring](https://man.archlinux.org/man/io_uring.7.en) is the cool new IO everyone is raving about, but don't get your hopes up that this is magically better than the other options. It is also a kernel level AIO implementation that utilizes mmap'd queues for submitting work to the kernel and for receiving notification of IO completion. What this means is that if one submits IO work (1,2,3,4,5) then it is possible to have a completion queue of (2,5,4,3,1). These queues also require synchronization with kernel for clearing out current entries and receiving new ones. The async nature of the api doesn't seem to be the best part, rather the ability to queue IO and have them all go in a single system call is the big leap forward here. Keep in mind, system calls are also slow, as they need to jump from userland to kernel land and then come back, so minimizing the amount of system calls is a good thing.

## Memory Mapped IO
[mmap](https://man7.org/linux/man-pages/man2/mmap.2.html) is where one builds a virtual file mapping from userland into the kernel for the ability to effectively write to memory and then flush that memory directly to disk. It also is useful for IPC, where one process writes to one end and the other reads from it. The biggest downside to mmap is that it requires an active file handle, which are limited on a machine for how many can be open at once, *and* it can only be as large as the architecture of the device. This means that on 32bit systems mmap can only be 4GiB in size, which is not a lot of data.

# TLDR
There are a lot of IO methods out there, and all of them have good things and bad things about them, the best thing is to assess what kind operations need to happen, whether data can be buffered, and what systems you are targeting. But always remember IO is slow!!!!

