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

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leansoft.luxun.common.annotations.NotThreadSafe;
import com.leansoft.luxun.common.exception.ConsumerTimeoutException;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.mx.ConsumerTopicStat;
import com.leansoft.luxun.serializer.Decoder;
import com.leansoft.luxun.utils.IteratorTemplate;

/**
 * 
 * @author bulldog
 * 
 */
@NotThreadSafe
public class ConsumerIterator<T> extends IteratorTemplate<T> {

    private final Logger logger = LoggerFactory.getLogger(ConsumerIterator.class);

    final String topic;

    final BlockingQueue<FetchedDataChunk> queue;

    final int consumerTimeoutMs;

    final Decoder<T> decoder;

    private AtomicReference<Iterator<Message>> current = new AtomicReference<Iterator<Message>>(null);

    public ConsumerIterator(String topic, BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs,
            Decoder<T> decoder) {
        super();
        this.topic = topic;
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.decoder = decoder;
    }

    @Override
    public T next() {
        T decodedMessage = super.next();
        ConsumerTopicStat.getConsumerTopicStat(topic).recordMessagesPerTopic(1);
        return decodedMessage;
    }

    @Override
    protected T makeNext() {
        try {
            return makeNext0();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected T makeNext0() throws InterruptedException {
        FetchedDataChunk currentDataChunk = null;
        Iterator<Message> localCurrent = current.get();
        if (localCurrent == null || !localCurrent.hasNext()) {
            if (consumerTimeoutMs < 0) {
                currentDataChunk = queue.take();
            } else {
                currentDataChunk = queue.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS);
                if (currentDataChunk == null) {
                    resetState();
                    throw new ConsumerTimeoutException("consumer timeout in " + consumerTimeoutMs + " ms");
                }
            }
            if (currentDataChunk == StreamFactory.SHUTDOWN_COMMAND) {
                logger.warn("Now closing the message stream");
                queue.offer(currentDataChunk); // maybe there are other consuming threads waiting on the same queue
                return allDone();
            } else {
                // consume an message
                localCurrent = currentDataChunk.messageList.iterator();
                current.set(localCurrent);
            }
        }
        Message message = localCurrent.next();
        return decoder.toEvent(message);
    }
}
