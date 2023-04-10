# Daemon
This is a simple component that uses a [io](io.md) method to read and write data to disk. It is designed to allow for clients to perform async IO even when the implementation of the IO method might be blocking. Being a daemon also open ups possibilities for different implementations without having to change the client code and keeping the API for the client consistent and simple.

## Design
The daemon will be a simple process that will use a [IPC](https://en.wikipedia.org/wiki/Inter-process_communication) to communicate with the client. The client will send requests to the daemon to perform IO and thus be able to treat all IO as asynchronous. The daemon will then respond to requests with the results of the IO operation. IO operations will also be cancellable.

```
+-------------------+   +-------------------+
|                   |   |                   |
|      Client       |   |      Daemon       |
|                   |   |                   |
+-------------------+   +-------------------+
          |                       |
          |                       |
          |                       |
    +-----------+           +-----------+
    |  Client   |           |  Daemon   |
    |  SQ/CQ    |-----------|  SQ/CQ    |
    |           |           |           |
    +-----------+           +-----------+
```  