
This program could sort big file(for example, 10GB) with two nodes, each node has small(for example, 4GB) memory and communicate with Ethernet.

##psort.java
It's like a manager program, run it in the host machine, then it will wait the client to get connect. After the connection(socket connection) established, the program will start to work.

##Client.java
The worker program, it will receive the data from the manager, sort the data, then send it back.

##wakeUpClient
A shell program could wake up the worker.

##The high level design of this program:

*Use two nodes' memory to split the big original file to small sorted files, then merge sort the small files.

*Use sockets to send and receive data, create multiply threads for read and write data.

##The hard part of this program:
To handle multiple threads. Now this program still has a bug of multiple threads writing.