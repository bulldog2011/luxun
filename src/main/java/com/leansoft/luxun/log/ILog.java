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

package com.leansoft.luxun.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.Lock;

import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.FanOutQueueImplEx.BatchReadResult;


/**
 * log interface
 * <p>
 * A log describes a topic.
 * </p>
 * 
 */
public interface ILog extends Closeable {
	
	public static int MAX_MESSAGE_SIZE = BigArrayImpl.MINIMUM_DATA_PAGE_SIZE >> 4;
	
	public static int DEFAULT_MESSAGE_SIZE = 1024 * 1024;

    
	/**
     * read data from log by index
     * 
	 * @param index item index
	 * @return binary data
	 * @throws IOException exception thrown during the operation
	 */
    byte[] read(long index) throws IOException;
    
    
    /**
     * read data from log by fanout id
     * 
     * @param fanoutId fanout identifier
     * @return binary data
     * @throws IOException exception thrown during the operation
     */
    byte[] read(String fanoutId) throws IOException;
    
    /**
     * read a list of data up to a maxFetchSize from log by fanout id
     * 
     * @param fanoutId fanout identifier
     * @param maxFetchSize max size in bytes to fetch
     * @return a list of binary data with total fetched size
     * @throws IOException exception thrown during the operation
     */
    BatchReadResult batchRead(String fanoutId, int maxFetchSize) throws IOException;
    
    /**
     * get length of item from log by index
     * 
     * @param index item index
     * @return length of binary data
     * @throws IOException exception thrown during the operation
     */
    int getItemLength(long index) throws IOException;
    
    
    /**
     * get length of item from front of log(queue) by fanout id
     * 
     * @param fanoutId fanout identifier
     * @return length of binary data
     * @throws IOException exception thrown during the operation
     */
    int getItemLength(String fanoutId) throws IOException;
    
    /**
     * Get timestamp of item from log by index
     * 
     * @param index item index
     * @return timestamp when the binary data was inserted into the log
     * @throws IOException exception thrown during the operation
     */
    long getTimestamp(long index) throws IOException;
    
    
    /**
     * append data to log
     * 
     * @param item binary data
     * @return appended index
     * @throws IOException exception thrown during the operation
     */
    long append(byte[] item) throws IOException;;
    
    
    /**
     * get front index of the log(queue), this is the first appended index
     * 
     * @return front index
     */
    long getFrontIndex();
    
    /**
     * get front index of the fanout log(queue)
     * 
     * @param fanoutId fanout identifier
     * @return front index
     */
    long getFrontIndex(String fanoutId) throws IOException;
    
    /**
     * get rear index of the log(queue), this is the next to be appended index
     * 
     * @return
     */
    long getRearIndex();
    
    
    /**
     * total num of items in the log(queue)
     * 
     * @return total number of items
     */
    long getSize();
    
    /**
     * total number of items in the fanout log(queue)
     * 
     * @param fantouId fanout identifier
     * @return total number of items
     */
    long getSize(String fantoutId) throws IOException;
    
    /**
     * check if the log is empty or not
     * 
     * @return true if empty, false otherwise
     */
    boolean isEmpty();
    
    /**
     * check if the fandout log(queue) is empty or not
     * 
     * @param fanoutId fanout identifier
     * @return true if empty, false otherwise
     */
    boolean isEmpty(String fanoutId) throws IOException;
    
    /**
     * Get an index closest to the specific timestamp when the corresponding item was appended.
     * 
     * @param timestamp when the corresponding item was appended
     * @return an index
     * @throws IOException exception thrown during the operation
     */
    long getClosestIndex(long timestamp) throws IOException;
	
	/**
	 * Remove all items before specific timestamp, this will advance the log front index and delete back page files
	 * accordingly.
	 * 
	 * @param timestamp a timestamp
	 * @throws IOException exception thrown if there was any IO error during the removal operation
	 */
	void removeBefore(long timestamp) throws IOException;
	
	/**
	 * Force to persist newly appended data,
	 * 
	 * normally, you don't need to flush explicitly since:
	 * 1.) underlying BigArray will automatically flush a cached page when it is replaced out,
	 * 2.) underlying BigArray uses memory mapped file internally, and the OS will flush the changes even your process crashes,
	 * 
	 * call this periodically only if you need transactional reliability and you are aware of the cost to performance.
	 */
	void flush();
	
	/**
	 * Limit the back file size of this log, truncate back files if necessary
	 * 
	 * Note, this is a best effort call, exact size limit can't be guaranteed
	 * 
	 * @param sizeLmit
	 * @throws IOException exception thrown if there was any IO error during the operation
	 */
	void limitBackFileSize(long sizeLmit) throws IOException;
	
	/**
	 * Current total size of the back files of this log
	 * 
	 * @return total back file size
	 * @throws IOException exception thrown if there was any IO error during the operation
	 */
	long getBackFileSize() throws IOException;
	
	/**
	 * Get fanout queue front write lock
	 * 
	 * @param fanoutId fanout identifier
	 * @return a lock
	 * @throws IOException exception thrown if there was any IO error during the operation
	 */
	Lock getQueueFrontWriteLock(String fanoutId) throws IOException;
	
	/**
	 * Get inner array read lock
	 * 
	 * @return a read lock
	 */
	Lock getInnerArrayReadLock();
	
	/**
	 * just for testing
	 * 
	 * @return number of back data files
	 */
	int getNumberOfBackFiles();
}
