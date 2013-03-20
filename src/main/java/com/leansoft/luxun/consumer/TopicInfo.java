/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.leansoft.luxun.consumer;

import java.util.concurrent.BlockingQueue;

import com.leansoft.luxun.broker.Broker;
import com.leansoft.luxun.message.MessageList;

/**
 * 
 * @author bulldog
 * 
 */
public class TopicInfo {

    public final String topic;
    

    public final Broker broker;

    private final BlockingQueue<FetchedDataChunk> chunkQueue;

    public TopicInfo(String topic, Broker broker, //
            BlockingQueue<FetchedDataChunk> chunkQueue) {
        this.topic = topic;
        this.broker = broker;
        this.chunkQueue = chunkQueue;
    }

    // fetched an indexed item
    public int enqueue(MessageList messageList) throws InterruptedException {
        chunkQueue.put(new FetchedDataChunk(messageList, this));
        return messageList.size();
    }
}
