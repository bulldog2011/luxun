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

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.leansoft.luxun.common.exception.InvalidConfigException;
import com.leansoft.luxun.utils.Utils;
import static com.leansoft.luxun.utils.Utils.*;


/**
 * the consumer configuration
 * <p>
 * The minimal configuration has these names:
 * <ul>
 * <li>broker.list : the target borker list</li>
 * <li>groupid: the consumer group name</li>
 * </ul>
 * </p>
 * 
 * @author bulldog
 * 
 */
public class ConsumerConfig {
	
	private Properties props;

    private String groupId;

    private String consumerId;

    private int socketTimeoutMs;

    private int socketBufferSize;

    private int fetchSize;

    private long fetchBackoffMs;

    private long maxFetchBackoffMs;

    private int maxQueuedChunks;

    private int consumerTimeoutMs;
    
    private String borkerList;
    
    private int consumerNumRetries;
    
    protected int get(String name,int defaultValue) {
        return getInt(props,name,defaultValue);
    }
    
    protected String get(String name,String defaultValue) {
        return getString(props,name,defaultValue);
    }

    /**
     * <p>
     * The minimal configurations have these names:
     * <ul>
     * <li>groupid: aka the consumer group name</li>
     * <li>broker.list: a list of target brokers</li>
     * </ul>
     * </p>
     * 
     * @param props config properties
     */
    public ConsumerConfig(Properties props) {
        this.props = props;
        this.groupId = Utils.getString(props, "groupid");
        this.consumerId = Utils.getString(props, "consumerid", null);
        this.socketTimeoutMs = get("socket.timeout.ms", 30 * 1000);
        this.socketBufferSize = get("socket.buffersize", 64 * 1024);//64KB
        this.fetchSize = get("fetch.size", 1024 * 1024);//1MB
        this.fetchBackoffMs = get("fetcher.backoff.ms", 1000);
        this.maxFetchBackoffMs = get("fetcher.backoff.ms.max", (int) fetchBackoffMs * 10);
        this.maxQueuedChunks = get("queuedchunks.max", 10);
        this.consumerTimeoutMs = get("consumer.timeout.ms", -1);
        this.borkerList = get("broker.list", null);
        this.consumerNumRetries = get("num.retries", 0);
        this.check();
    }
    
    private void check() {
        // If broker.list is not specified, throw an exception
        if (StringUtils.isEmpty(this.borkerList)) {
            throw new InvalidConfigException("broker.list must be specified in config");
        }
        // If fanoutId is not specified, throw an exception
        if (StringUtils.isEmpty(this.groupId)) {
            throw new InvalidConfigException("groupid(aka consuemr group name) must be specified in config");
        }
    }
    
    /**
     * use this config to pass in static broker. Format-
     * 
     * <pre>
     *      brokerid1:host1:port1, brokerid2:host2:port2
     * </pre>
     */
    public String getBrokerList() {
        return this.borkerList;
    }

    /**
     * a string that uniquely identifies a set of consumers within the same consumer group
     */
    public String getGroupId() {
        return this.groupId;
    }

    /**
     * consumer id: generated automatically if not set. Set this explicitly for only testing
     * purpose.
     */
    public String getConsumerId() {
        return consumerId;
    }

    /** the socket timeout for network requests */
    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    /** the socket receive buffer for network requests */
    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    /** the number of bytes of messages attempt to fetch */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * to avoid repeatedly polling a broker node which has no new data we will backoff every
     * time we get an empty set from the broker
     */
    public long getFetchBackoffMs() {
        return fetchBackoffMs;
    }

    /** max number of messages buffered for consumption */
    public int getMaxQueuedChunks() {
        return maxQueuedChunks;
    }

    /**
     * throw a timeout exception to the consumer if no message is available for consumption
     * after the specified interval
     */
    public int getConsumerTimeoutMs() {
        return consumerTimeoutMs;
    }

    public long getMaxFetchBackoffMs() {
        return maxFetchBackoffMs;
    }
    
    /**
     * Max numer of retires if underlying consumer got error
     * 
     * @return
     */
    public long getConsumerNumRetires() {
    	return this.consumerNumRetries;
    }
}
