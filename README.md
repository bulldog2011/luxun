# Luxun


A high-throughtput, distributed, publish-subscribe messaging system. Luxun is inspired by [Apache Kafka](http://kafka.apache.org/). The main design objectives of Luxun are the following:

1. ***Fast***: close to the speed of direct memory access, both produding and consuming are close to O(1) memeory access.
2. ***Persistent*** : all data in the system is persisted on disk and is crash resistant.
3. ***High-throughput***: even with modest hardware Luxun can support handreds of thousands of messages per second.
4. ***Realtime*** : messages produced by producer threads should be immediately visisble to consumer threads.
5. ***Distributed*** : explicit support for partitioning messages over Luxun servers and distributing consumption over a cluster of consuemr machines while maintaining per-partition ordering semantics.
6. ***Multiple clients support*** : use [Thrift RPC]() as communication infrastructure, easy to generate different clients for different platforms.

Luxun provides a light weight pub-sub style messaging solution taht can handle activity stream data in an effective and unified way. Luxun is based on the [bigqueue](https://github.com/bulldog2011/bigqueue) library which uses [memory mapped file](http://en.wikipedia.org/wiki/Memory_mapped_file) internally to support fast while persistent queue operations.

## The Architecture

// TODO



## Version History
#### 0.5.1 — *March 25, 2013* : [repository](https://github.com/bulldog2011/bulldog-repo/tree/master/repo/releases/com/leansoft/luxun/0.6.0)


  
## Docs

// TODO


##Copyright and License
Copyright 2012 Leansoft Technology <51startup@sina.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

##Contributions
Luxun borrowed design ideas and adapted source from following open source projects:

1. [Apache Kafka](http://kafka.apache.org/index.html), a distribued publish-subscribe messaging system using Scala as implementation language.
2. [Jafka](https://github.com/adyliu/jafka), a Kafka clone using Java as implementation langauge.
3. [Java Chronicel](https://github.com/peter-lawrey/Java-Chronicle), an ultra low latency, high throughput, persisted, messaging and event driven in memory database. using memory mapped file and index based access mode, implmentated in Java.
4. [fqueue](http://code.google.com/p/fqueue/), fast and persistent queue based on memory mapped file and paging algorithm, implemented in Java.
5. [ashes-queue](http://code.google.com/p/ashes-queue/), FIFO queue based on memory mapped file and paging algorithm, implemented in Java.
6. [Kesrel](https://github.com/robey/kestrel), a simple, distributed message queue system implemented in Scala, supporting reliable, loosely ordered message queue.

Many thanks to authors of these oos project!


## Origin of the Name
Similar to [Franz Kafka](http://en.wikipedia.org/wiki/Franz_Kafka), [Lu Xun](http://en.wikipedia.org/wiki/Franz_Kafka) is a great Chinese writer, they lived almost in the same era. In spite of their differences, Kafka and Lu Xun share a common concern on what they saw as the oppressive nature of the past and the authority drawn from it.

Luxun messaging system is created in memorial of Lu Xun, for his great contributions to Chinese literature.
![Luxun](http://upload.wikimedia.org/wikipedia/commons/thumb/4/48/LuXun1930.jpg/200px-LuXun1930.jpg)







 














