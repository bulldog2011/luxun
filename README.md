# Luxun


A high-throughput, distributed, publish-subscribe messaging system. Luxun is inspired by [Apache Kafka](http://kafka.apache.org/). The main design objectives of Luxun are the following:

1. ***Fast***: close to the speed of direct memory access, both produding and consuming are close to O(1) memory access.
2. ***Persistent*** : all data in the system is persisted on disk and is crash resistant.
3. ***High-throughput***: even with modest hardware Luxun can support handreds of thousands of messages per second.
4. ***Realtime*** : messages produced by producer threads should be immediately visible to consumer threads.
5. ***Distributed*** : explicit support for partitioning messages over Luxun servers and distributing consumption over a cluster of consumer machines while maintaining per-partition ordering semantics.
6. ***Multiple clients support*** : use [Thrift RPC](http://thrift.apache.org/) as communication infrastructure, easy to generate different clients for different platforms.
7. ***Flexible consuming semantics*** : supports typical consume once queue, fan out queue, even supports consume by index when needed.

The objectives above makes Luxun extremely suitable for nowadays popular big data or activity stream collecting and analytics.

Luxun is based on the [bigqueue](https://github.com/bulldog2011/bigqueue) library which uses [memory mapped file](http://en.wikipedia.org/wiki/Memory_mapped_file) internally to support fast while persistent queue operations.

## The Architecture

![The Architecture](http://bulldog2011.github.com/images/luxun/arch-2.png)



## Version History
#### 0.6.0 — *March 25, 2013* : [repository](https://github.com/bulldog2011/bulldog-repo/tree/master/repo/releases/com/leansoft/luxun/0.6.0)


  
## Docs

1. [The architecture and Design of a Publish & Subscribe Messaging System Tailored for Big Data Collecting & Analytics](http://bulldog2011.github.com/blog/2013/03/27/the-architecture-and-design-of-a-pub-sub-messaging-system/).
2. [Luxun Quick Start](http://bulldog2011.github.com/blog/2013/04/03/luxun-quick-start/).
3. [Configuration](https://github.com/bulldog2011/luxun/wiki/Configuration).
4. [Performance(ongoing)](https://github.com/bulldog2011/luxun/wiki/Performance-Test-on-Windows-7).


##Copyright and License
Copyright 2012 Leansoft Technology <51startup@sina.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

##Credits
Luxun borrowed design ideas and adapted source from following open source projects:

1. [Apache Kafka](http://kafka.apache.org/index.html), a distributed publish-subscribe messaging system using Scala as implementation language.
2. [Jafka](https://github.com/adyliu/jafka), a Kafka clone using Java as implementation language.
3. [Java Chronicel](https://github.com/peter-lawrey/Java-Chronicle), an ultra low latency, high throughput, persisted, messaging and event driven in memory database. using memory mapped file and index based access mode, implemented in Java.
4. [fqueue](http://code.google.com/p/fqueue/), fast and persistent queue based on memory mapped file and paging algorithm, implemented in Java.
5. [ashes-queue](http://code.google.com/p/ashes-queue/), FIFO queue based on memory mapped file and paging algorithm, implemented in Java.
6. [Kestrel](https://github.com/robey/kestrel), a simple, distributed message queue system implemented in Scala, supporting reliable, loosely ordered message queue.

Many thanks to the authors of these open source projects!


## Origin of the Name
Both [Franz Kafka](http://en.wikipedia.org/wiki/Franz_Kafka) and [Lu Xun](http://en.wikipedia.org/wiki/Franz_Kafka) lived almost in the same era, similar to Kafka, Lu Xun was a great Chinese writer. In spite of their differences, Kafka and Lu Xun share a common concern on what they saw as the oppressive nature of the past and the authority drawn from it.

Luxun messaging system is named after Lu Xun, in memorial of his great contributions to Chinese literature.
![Luxun](http://upload.wikimedia.org/wikipedia/commons/thumb/4/48/LuXun1930.jpg/200px-LuXun1930.jpg)







 














