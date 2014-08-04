Development
===========

small c# library that can hook into a 3nd party synchronous non blocking messaging API. More specifically an API that would 
expose a shape of APIs like:
- void send(string msg): to send the message as a string
- bool received(): returns true if there are new incoming msg responses
- string read(): to read new messages as string

AND

- an assumption is made that it is possible to correlate a response to its request when using the 3nd party library.

The library offers the following features:
- 1 simple request/response pattern asynchronous API,
- client can define a timeout/message,
- dispatching for scaling (if necessary).

The implemenation uses ONLY 3 threads per message processor no matter how many clients are sending messages to the
processor and is modeled on a event-based queue.
Request messages are queued up by a single dedicated thread and an other dedicated thread listens for response messages,
handle correlation and then dispatch the response message onto a thread pool thread.
When using async/await, the thread that will run the continuation code is taken from the thread pool thread 
(windows IO completion port mechanism under the cover) and the library does not change this paradigm but introduces these
2 event based loops to send outgoing messages and read incoming ones.

