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

package com.leansoft.luxun.producer;

import java.io.Closeable;

import com.leansoft.luxun.common.exception.NoBrokersForTopicException;
import com.leansoft.luxun.serializer.Encoder;

/**
 * Producer interface
 * 
 * @author bulldog
 * @param <K> partition key
 * @param <V> real message
 * 
 */
public interface IProducer<K, V> extends Closeable {

    /**
     * Send messages
     * 
     * @param data message data
     * @throws NoBrokersForTopicException no broker for this topic
     */
    void send(ProducerData<K, V> data) throws NoBrokersForTopicException;

    /**
     * get message encoder
     * 
     * @return message encoder
     * @see Encoder
     */
    Encoder<V> getEncoder();
    
    /**
     * get partition chooser
     * 
     * @return partition chooser
     * @see IPartitioner
     */
    IPartitioner<K> getPartitioner();
}
