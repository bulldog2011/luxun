package com.leansoft.luxun.log;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leansoft.bigqueue.FanOutQueueImplEx;
import com.leansoft.bigqueue.FanOutQueueImplEx.BatchReadResult;
import com.leansoft.bigqueue.IFanOutQueueEx;
import com.leansoft.luxun.common.annotations.NotThreadSafe;
import com.leansoft.luxun.common.exception.MessageSizeTooLargeException;
import com.leansoft.luxun.mx.BrokerTopicStat;
import com.leansoft.luxun.mx.LogFlushStats;
import com.leansoft.luxun.mx.LogStats;
import com.leansoft.luxun.utils.Closer;
import com.leansoft.luxun.utils.Utils;


/**
 * a log is a message set backed by a persistent fanout queue
 *
 */
public class Log implements ILog {
	
	private final Logger logger = LoggerFactory.getLogger(Log.class);
	
	public final File baseDir;
	
	public final String topic;
	
	final int maxItemCountBeforeFlush;
	
	final boolean needRecovery;
	
    private final AtomicBoolean flushLock = new AtomicBoolean(false);
	
	private final AtomicInteger unflushed = new AtomicInteger(0);
	
    private final AtomicLong lastflushedTime = new AtomicLong(0L);
	
    // a log is internally backed by an append only fanout queue
	private IFanOutQueueEx foQueue;
	
    private final LogStats logStats = new LogStats(this);
	
	private final int maxMessageSize;
	
	public final File realLogDir;
	
	public Log(File baseDir, String topic, 
			int maxItemCountBeforeFlush,
			boolean needRecovery,
			int pageSize,
			int maxMessageSize) throws IOException {
		this.baseDir = baseDir;
		this.maxItemCountBeforeFlush = maxItemCountBeforeFlush;
		this.needRecovery = needRecovery;
		this.maxMessageSize = maxMessageSize;
		this.topic = topic;

		String logDir = this.baseDir.getAbsolutePath();
		if (!logDir.endsWith(File.separator)) {
			logDir += File.separator;
		}
		logDir +=  topic;
		realLogDir = new File(logDir);
		
		this.logStats.setMbeanName("luxun:type=luxun.logs." + topic);
		Utils.registerMBean(logStats);
		
		this.foQueue = new FanOutQueueImplEx(baseDir.getAbsolutePath(), topic, pageSize);
	}
	
    /**
     * delete all log page files in this topic <br/>
     * The log directory will be removed also.
     * @throws IOException 
     */
	public void delete() {
		this.close();
		Utils.deleteDirectory(realLogDir);
		logger.info("Deleted " + this);
	}
	
	
	@Override
	public void close() {
		Closer.closeQuietly(this.foQueue, logger);
		this.foQueue = null;
		Utils.unregisterMBean(this.logStats);
	}
	
    public long getLastFlushedTime() {
        return lastflushedTime.get();
    }
    
    @Override
    public String toString() {
        return "Log [dir=" + this.realLogDir + ", lastflushedTime=" + //
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(lastflushedTime.get())) + "]";
    }

	@Override
	public byte[] read(long index) throws IOException {
		return this.foQueue.get(index);
	}

	@Override
	public long append(byte[] item) throws IOException {
		// validate message size
		if (item.length > maxMessageSize) {
			throw new MessageSizeTooLargeException("payload size of " + item.length + " larger than " + maxMessageSize);
		}
		
		long index = this.foQueue.enqueue(item);
		
		unflushed.incrementAndGet();
		if (maxItemCountBeforeFlush > 0) {
			this.maybeFlush();
		}
		
		BrokerTopicStat.getBrokerTopicStat(topic).recordMessagesIn(1);
		BrokerTopicStat.getBrokerAllTopicStat().recordMessagesIn(1);
		this.logStats.incrementAppendedMessageCount();
		
		return index;
	}

	/**
	 * total number of messages in this topic(some old messages may have been deleted)
	 */
	@Override
	public long getSize() {
		return this.foQueue.size();
	}

	@Override
	public long getClosestIndex(long timestamp) throws IOException {
		return this.foQueue.findClosestIndex(timestamp);
	}

	@Override
	public void removeBefore(long timestamp) throws IOException {
		this.foQueue.removeBefore(timestamp);
	}
	
	private void maybeFlush() {
		if (unflushed.get() >= maxItemCountBeforeFlush) {
			flush();
		}
	}

	@Override
	public void flush() {
		if (unflushed.get() == 0) return; // nothing to flush
		
		if (flushLock.compareAndSet(false, true)) {
			try {
				long startTime = System.currentTimeMillis();
				this.foQueue.flush();
				long elapsedTime = System.currentTimeMillis() - startTime;
				LogFlushStats.recordFlushRequest(elapsedTime);
				unflushed.set(0);
				lastflushedTime.set(System.currentTimeMillis());
			} finally {
				flushLock.set(false);
			}
		}
	}

	@Override
	public void limitBackFileSize(long sizeLimit) throws IOException {
		this.foQueue.limitBackFileSize(sizeLimit);
	}

	@Override
	public boolean isEmpty() {
		return this.foQueue.isEmpty();
	}
	
	@Override
	public long getFrontIndex() {
		return this.foQueue.getFrontIndex();
	}

	@Override
	public long getRearIndex() {
		return this.foQueue.getRearIndex();
	}

	@Override
	public long getBackFileSize() throws IOException {
		return this.foQueue.getBackFileSize();
	}

	@Override
	public int getItemLength(long index) throws IOException {
		return this.foQueue.getLength(index);
	}

	@Override
	public long getTimestamp(long index) throws IOException {
		return this.foQueue.getTimestamp(index);
	}

	@Override
	public byte[] read(String fanoutId) throws IOException {
		return this.foQueue.dequeue(fanoutId);
	}

	@Override
	public int getItemLength(String fanoutId) throws IOException {
		return this.foQueue.peekLength(fanoutId);
	}

	@Override
	public long getFrontIndex(String fanoutId) throws IOException {
		return this.foQueue.getFrontIndex(fanoutId);
	}

	@Override
	public long getSize(String fanoutId) throws IOException {
		return this.foQueue.size(fanoutId);
	}

	@Override
	public boolean isEmpty(String fanoutId) throws IOException {
		return this.foQueue.isEmpty(fanoutId);
	}

	@Override
	public Lock getQueueFrontWriteLock(String fanoutId) throws IOException {
		return this.foQueue.getQueueFrontWriteLock(fanoutId);
	}

	@Override
	public Lock getInnerArrayReadLock() {
		return this.foQueue.getInnerArrayReadLock();
	}

	@Override
	public int getNumberOfBackFiles() {
		return this.foQueue.getNumberOfBackFiles();
	}

	@Override
	@NotThreadSafe
	public BatchReadResult batchRead(String fanoutId, int maxFetchSize)
			throws IOException {
		return this.foQueue.batchDequeue(fanoutId, maxFetchSize);
	}
}
