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


import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.serializer.Decoder;

/**
 * Factory interface for streaming consumer
 * 
 * @author bulldog
 * 
 */
public interface IStreamFactory extends Closeable {

    /**
     * Create a list of {@link MessageStream} for each topic
     * 
     * @param topicThreadNumMap a map of (topic, number of threads/streams) pair
     * @param decoder message decoder
     * @return a map of (topic,list of MessageStream) pair. The number of
     *         items in the list is the number of threads/streams. Each MessageStream supports
     *         an iterator of messages.
     */
    <T> Map<String, List<MessageStream<T>>> createMessageStreams(//
            Map<String, Integer> topicThreadNumMap, Decoder<T> decoder);
    
    /**
     * Create a list of {@link MessageStream} for each topic with default Luxun message decoder
     * 
     * @param topicThreadNumMap a map of (topic, number of threads/streams) pair
     * @return a map of (topic,list of MessageStream) pair. The number of
     *         items in the list is the number of threads/streams. Each MessageStream supports
     *         an iterator of messages.
     */
    <T> Map<String, List<MessageStream<Message>>> createMessageStreams(//
            Map<String, Integer> topicThreadNumMap);

    /**
     * Shut down the consumer
     */
    public void close() throws IOException;
}
