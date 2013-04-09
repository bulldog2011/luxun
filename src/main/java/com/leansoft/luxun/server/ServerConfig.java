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

package com.leansoft.luxun.server;

import static com.leansoft.luxun.utils.Utils.*;

import java.util.Map;
import java.util.Properties;

import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.luxun.log.ILog;

/**
 * Configuration for the memory mapped queue server
 * 
 */
public class ServerConfig {
	
    protected final Properties props;

    private final Authentication authentication;
    public ServerConfig(Properties props) {
    	this.props = props;
        authentication = Authentication.build(getString(props, "password", null));
    }

    /** the port to listen and accept connections on (default 9092) */
    public int getPort() {
        return getInt(props, "port", 9092);
    }

//    /**
//     * hostname of broker. If not set, will pick up from the value returned
//     * from getLocalHost. If there are multiple interfaces getLocalHost may
//     * not be what you want.
//     */
//    public String getHostName() {
//        return getString(props, "hostname", null);
//    }

    /** the broker id for this server */
    public int getBrokerId() {
        return getInt(props, "brokerid");
    }   
    
    /**
     * the interpublic String get in which to measure performance
     * statistics
     */
    public int getMonitoringPeriodSecs() {
        return getIntInRange(props, "monitoring.period.secs", 600, 1, Integer.MAX_VALUE);
    }

    /** the directory in which the log data is kept */
    public String getLogDir() {
        return getString(props, "log.dir");
    }
    
    /**
     * the number of messages accumulated on a log before
     * messages are flushed to disk,
     * 
     * a negative number means not to flush explicitly
     * 
     * Note: usually, explicit flush is not needed since:
	 * 1.) underlying log will automatically flush a cached page when it is replaced out,
	 * 2.) underlying log uses memory mapped file internally, and the OS will flush the changes even your process crashes,
	 * Set this property to a positive number only if you need transactional reliability and you are aware of the cost to performance.
     * 
     */
    public int getFlushCount() {
    	return getInt(props, "log.flush.count", -1);
    }

    /** the number of hours to keep a log file before deleting it */
    public int getLogRetentionHours() {
        return getIntInRange(props, "log.retention.hours", 24 * 7, 1, Integer.MAX_VALUE);
    }

    /** the maximum total size of all log page files before deleting it */
    public long getLogRetentionSize() {
        return getLong(props, "log.retention.size", -1L);
    }

    /**
     * the number of hours to keep a log file before deleting it for some
     * specific topic
     */
    public Map<String, Integer> getLogRetentionHoursMap() {
        return getTopicRentionHours(getString(props, "topic.log.retention.hours", ""));
    }

    /**
     * the frequency in minutes that the log cleaner checks whether any log
     * is eligible for deletion
     */
    public int getLogCleanupIntervalMinutes() {
        return getIntInRange(props, "log.cleanup.interval.mins", 10, 1, Integer.MAX_VALUE);
    }

    /**
     * the maximum time in ms that a message in selected topics is kept in
     * memory before flushed to disk, e.g., topic1:10000,topic2: 20000
     * 
     * a negative number means not to flush explicitly
     * 
     * Note: usually, explicit flush is not needed since:
	 * 1.) underlying log will automatically flush a cached page when it is replaced out,
	 * 2.) underlying log uses memory mapped file internally, and the OS will flush the changes even your process crashes,
	 * Set this property to a positive number only if you need transactional reliability and you are aware of the cost to performance.
     * 
     */
    public Map<String, Integer> getFlushIntervalMap() {
        return getTopicFlushIntervals(getString(props, "topic.flush.intervals.ms", ""));
    }

    /**
     * the frequency in ms that the log flusher checks whether any log
     * needs to be flushed to disk
     * 
     * a negative number means not to flush explicitly
     * 
     * Note: usually, explicit flush is not needed since:
	 * 1.) underlying log will automatically flush a cached page when it is replaced out,
	 * 2.) underlying log uses memory mapped file internally, and the OS will flush the changes even your process crashes,
	 * Set this property to a positive number only if you need transactional reliability and you are aware of the cost to performance.
     * 
     */
    public int getFlushSchedulerThreadRate() {
        return getInt(props, "log.default.flush.scheduler.interval.ms", -1);
    }

    /**
     * the maximum time in ms that a message in any topic is kept in memory
     * before flushed to disk
     * 
     * a negative number means not to flush explicitly
     * 
     * Note: usually, explicit flush is not needed since:
	 * 1.) underlying log will automatically flush a cached page when it is replaced out,
	 * 2.) underlying log uses memory mapped file internally, and the OS will flush the changes even your process crashes,
	 * Set this property to a positive number only if you need transactional reliability and you are aware of the cost to performance.
     * 
     */
    public int getDefaultFlushIntervalMs() {
        return getInt(props, "log.default.flush.interval.ms", getFlushSchedulerThreadRate());
    }

    /**
     * get Authentication method
     * 
     * @return Authentication method
     * @see Authentication#build(String)
     */
    public Authentication getAuthentication() {
        return authentication;
    }

    /** 
     * maximum size of message that the server can receive, default see {@link ILog#DEFAULT_MESSAGE_SIZE}
     * 
     * @return maximum size of message
     */
    public int getMaxMessageSize() {
        return getIntInRange(props,"max.message.size", ILog.DEFAULT_MESSAGE_SIZE, 1, ILog.MAX_MESSAGE_SIZE );
    }
    
    /**
     * size per page of back log file
     * 
     * @return size per page
     */
    public int getLogPageSize() {
    	return getIntInRange(props, "log.backfile.page.size", BigArrayImpl.DEFAULT_DATA_PAGE_SIZE, BigArrayImpl.MINIMUM_DATA_PAGE_SIZE, Integer.MAX_VALUE);
    }
    
}
