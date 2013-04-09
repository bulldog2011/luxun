package com.leansoft.luxun.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.leansoft.luxun.api.generated.Constants;
import com.leansoft.luxun.api.generated.ConsumeRequest;
import com.leansoft.luxun.api.generated.ConsumeResponse;
import com.leansoft.luxun.api.generated.DeleteTopicRequest;
import com.leansoft.luxun.api.generated.DeleteTopicResponse;
import com.leansoft.luxun.api.generated.ErrorCode;
import com.leansoft.luxun.api.generated.FindClosestIndexByTimeRequest;
import com.leansoft.luxun.api.generated.FindClosestIndexByTimeResponse;
import com.leansoft.luxun.api.generated.GetSizeRequest;
import com.leansoft.luxun.api.generated.GetSizeResponse;
import com.leansoft.luxun.api.generated.ProduceRequest;
import com.leansoft.luxun.api.generated.ProduceResponse;
import com.leansoft.luxun.api.generated.QueueService;
import com.leansoft.luxun.api.generated.Result;
import com.leansoft.luxun.api.generated.ResultCode;
import com.leansoft.luxun.common.exception.ErrorMapper;
import com.leansoft.luxun.common.exception.InvalidTopicException;
import com.leansoft.luxun.mx.BrokerTopicStat;
import com.leansoft.luxun.mx.ThriftServerStats;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.Closer;
import com.leansoft.luxun.utils.Pool;
import com.leansoft.luxun.utils.Scheduler;
import com.leansoft.luxun.utils.TopicNameValidator;
import com.leansoft.luxun.utils.Utils;

/**
 * 
 * Central proxy for queue service
 * 
 * @author bulldog
 *
 */
public class LogManager implements Closeable, QueueService.Iface  {
	
	final ServerConfig config;
	
	private final Scheduler scheduler;
	
	final long logCleanupIntervalMs;
	
	final long logCleanupDefaultAgeMs;
	
	final boolean needRecovery;
	
	private final Logger logger = Logger.getLogger(LogManager.class);
	
    ///////////////////////////////////////////////////////////////////////
    
    final File logDir;
    
    final int flushCount;
    
    private final Object logCreationLock = new Object();
    
    final Random random = new Random();
    
    private final Pool<String, Log> logs = new Pool<String, Log>();
	
    private final Scheduler logFlusherScheduler = new Scheduler(1, "luxun-logflusher", false);
    
    final Map<String, Integer> logFlushIntervalMap;
    
    final Map<String, Long> logRetentionMSMap;
    
    final long logRetentionSize;
    
    private final int maxMessageSize;
    private final int logPageSize;
    
    private ThriftServerStats stats;
    
    public LogManager(ServerConfig config,
    		          Scheduler scheduler,
    		          long logCleanupIntervalMs,
    		          long logCleanupDefaultAgeMs,
    		          boolean needRecovery,
    		          ThriftServerStats stats) {
    	this.config = config;
    	this.maxMessageSize = config.getMaxMessageSize();
    	this.logPageSize = config.getLogPageSize();
    	this.scheduler = scheduler;
    	this.logCleanupIntervalMs = logCleanupIntervalMs;
    	this.logCleanupDefaultAgeMs = logCleanupDefaultAgeMs;
    	this.needRecovery = needRecovery;
    	this.logDir = Utils.getCanonicalFile(new File(config.getLogDir()));
    	this.flushCount = config.getFlushCount();
    	this.logFlushIntervalMap = config.getFlushIntervalMap();
    	this.logRetentionSize = config.getLogRetentionSize();
    	this.logRetentionMSMap = getLogRetentionMSMap(config.getLogRetentionHoursMap());
    	this.stats = stats;
    }
    
    public void load() throws IOException {
    	if (!logDir.exists()) {
    		logger.info("No log directory found, creating '" + logDir.getAbsolutePath() + "'");
    		logDir.mkdirs();
    	}
    	if (!logDir.isDirectory() || !logDir.canRead()) {
            throw new IllegalArgumentException(logDir.getAbsolutePath() + " is not a readable log directory.");
    	}
    	File[] subDirs = logDir.listFiles();
    	if (subDirs != null) {
    		for(File dir : subDirs) {
    			if (!dir.isDirectory()) {
    				logger.warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "' --should it be there?");
    			} else {
    				logger.info("Loading log from " + dir.getAbsolutePath());
    				final String topicName = dir.getName();
    				Log log = new Log(logDir, topicName, flushCount, needRecovery, logPageSize, maxMessageSize);
    				
    				logs.putIfNotExists(topicName, log);
    			}
    		}
    	}
    	
    	/**
    	 * Schedule the cleanup task to delete old logs or to maintain log size
    	 */
    	if (this.scheduler != null) {
    		logger.info("starting log cleaner every " + logCleanupIntervalMs + " ms.");
    		this.scheduler.scheduleWithRate(new Runnable() {

				@Override
				public void run() {
					try {
						cleanupLogs();
					} catch (IOException e) {
						logger.error("cleanup log failed.", e);
					}
				}
    			
    		}, 60 * 1000, logCleanupIntervalMs);
    	}
    }
    
    private Map<String, Long> getLogRetentionMSMap(Map<String, Integer> logRetentionHourMap) {
        Map<String, Long> ret = new HashMap<String, Long>();
        for (Map.Entry<String, Integer> e : logRetentionHourMap.entrySet()) {
            ret.put(e.getKey(), e.getValue() * 60 * 60 * 1000L);
        }
        return ret;
    }
    
    void cleanupLogs() throws IOException {
    	logger.trace("Beginning log cleanup...");
    	Iterator<Log> iter = getLogIterator();
    	long startMs = System.currentTimeMillis();
    	while(iter.hasNext()) {
    		Log log = iter.next();
    		this.cleanupExpiredLogPageFiles(log);
    		this.cleanupLogPageFilesToMaintainSize(log);
    	}
        logger.trace("Log cleanup completed  in " + (System.currentTimeMillis() - startMs) / 1000 + " seconds.");
    }
    
    /**
     * Runs through the log removing back page files until the size of log back files is at most logRetentionSize bytes in size.
     * 
     * @param log
     * @throws IOException
     */
    private void cleanupLogPageFilesToMaintainSize(final Log log) throws IOException {
    	if (logRetentionSize < 0) return; // do nothing
    	log.limitBackFileSize(logRetentionSize);
    }
    
    /**
     * Runs through the log removing back page files older than a certain age
     * 
     * @param log
     * @throws IOException
     */
    private void cleanupExpiredLogPageFiles(final Log log) throws IOException {
    	final long startMs = System.currentTimeMillis();
    	String topic = log.topic;
        Long logCleanupThresholdMS = logRetentionMSMap.get(topic);
        if (logCleanupThresholdMS == null) {
            logCleanupThresholdMS = this.logCleanupDefaultAgeMs;
        }
        final long expiredThreshold = logCleanupThresholdMS.longValue();
        log.removeBefore(startMs - expiredThreshold); 
    }

	@Override
	public void close() throws IOException {
		logFlusherScheduler.shutdown();
		Iterator<Log> iter = getLogIterator();
		while(iter.hasNext()) {
			Closer.closeQuietly(iter.next(), logger);
		}
	}
	
	/**
	 * Start log flusher scheduler
	 */
	public void startup() {
		if (config.getFlushSchedulerThreadRate() > 0) {
	        logger.info("Starting log flusher every " + config.getFlushSchedulerThreadRate() + " ms with the following overrides " + logFlushIntervalMap);
	        logFlusherScheduler.scheduleWithRate(new Runnable() {
	
				@Override
				public void run() {
					flushAllLogs(false);
				}
	        	
	        }, config.getFlushSchedulerThreadRate(), config.getFlushSchedulerThreadRate());
		}
	}
	
	/**
	 * flush all message to disk
	 * 
	 * @param force flush anyway(ignore flush interval)
	 */
	public void flushAllLogs(final boolean force) {
		Iterator<Log> iter = getLogIterator();
		while(iter.hasNext()) {
			Log log = iter.next();
			try {
				boolean needFlush = force;
				if (!needFlush) {
					long timeSinceLastFlush = System.currentTimeMillis() - log.getLastFlushedTime();
					Integer logFlushInterval = logFlushIntervalMap.get(log.topic);
					if (logFlushInterval == null) {
						logFlushInterval = config.getDefaultFlushIntervalMs();
					}
					
					if (logFlushInterval > 0) { // negative number means not to flush explicitly
						final String flushLogFormat = "[%s] flush interval %d, last flushed %d, need flush? %s";
						needFlush = timeSinceLastFlush >= logFlushInterval.intValue();
	                    logger.trace(String.format(flushLogFormat, log.topic, logFlushInterval,
	                            log.getLastFlushedTime(), needFlush));
					}
				}
				if (needFlush) {
					log.flush();
				}
			} catch (Exception e) {
				logger.error("Error flushing topic " + log.topic, e);
			}
		}
	}
	
	private Iterator<Log> getLogIterator() {
		return logs.values().iterator();
	}
	
	Log getLog(String topic) {
		if (topic == null || topic.length() <= 0) {
			throw new InvalidTopicException("topic name can't be empty");
		}
		return logs.get(topic);
	}
	
	/**
	 * Create the log if it does not exist or return back existing log
	 * 
	 * @param topic
	 * @return a log
	 * @throws IOException 
	 */
	ILog getOrCreateLog(String topic) throws IOException {
		Log log = this.getLog(topic);
		if (log == null) {
			log = createLog(topic);
			Log found = logs.putIfNotExists(topic, log);
			if (found != null) {
				Closer.closeQuietly(log, logger);
				log = found;
			} else {
                logger.info(String.format("Created log for %s on broker %d", topic, config.getBrokerId()));
			}
		}
        return log;
	}
	
	private Log createLog(String topic) throws IOException {
		TopicNameValidator.validate(topic);
		synchronized(logCreationLock) {
			return new Log(logDir, topic, flushCount, false, logPageSize, maxMessageSize);
		}
	}

	@Override
	public ProduceResponse produce(ProduceRequest produceRequest)
			throws TException {
		
		long start = System.nanoTime();
		
		ProduceResponse response = new ProduceResponse();
		Result result = new Result();
		response.setResult(result);
		
		try {
			String topic = produceRequest.getTopic();
			final ILog log = this.getOrCreateLog(topic);
			long messageSize = produceRequest.getItem().length;
			long index = log.append(produceRequest.getItem());
			result.setResultCode(ResultCode.SUCCESS);
			response.setIndex(index);
			
			BrokerTopicStat.getInstance(topic).recordBytesIn(messageSize);
			BrokerTopicStat.getBrokerAllTopicStat().recordBytesIn(messageSize);
		} catch (Exception e) {
			handleException(e, result, produceRequest);
			BrokerTopicStat.getInstance(produceRequest.getTopic()).recordFailedProduceRequest();
			BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
		}
		
		stats.recordRequest(ProduceRequest.class, System.nanoTime() - start);
		
		return response;
	}

	@Override
	public ConsumeResponse consume(ConsumeRequest consumeRequest)
			throws TException {
		
		long start = System.nanoTime();
		
		ConsumeResponse response = new ConsumeResponse();
		Result result = new Result();
		response.setResult(result);
		
		try {
			String topic = consumeRequest.getTopic();
			String fanoutId = consumeRequest.getFanoutId();
			long startIndex = consumeRequest.getStartIndex();
			int maxFetchSize = consumeRequest.getMaxFetchSize();
			
			ILog log = this.getLog(topic);
			if (log != null) {
				Lock innerArrayReadLock = log.getInnerArrayReadLock();
				try {
					innerArrayReadLock.lock();
					
					boolean isEmpty = log.isEmpty();
					if (isEmpty) {
						result.setResultCode(ResultCode.FAILURE);
						result.setErrorCode(ErrorCode.TOPIC_IS_EMPTY);
						result.setErrorMessage(String.format("topic %s is empty on broker %d", topic, config.getBrokerId()));
					} else {
						if (!Utils.isStringEmpty(fanoutId)) { // fanout queue

							Lock queueFrontWriteLock = log.getQueueFrontWriteLock(fanoutId);
							
							try {
								queueFrontWriteLock.lock();
								
								boolean isFanoutQueueEmpty = log.isEmpty(fanoutId);
								if (isFanoutQueueEmpty) {
									result.setResultCode(ResultCode.TRY_LATER);
									result.setErrorCode(ErrorCode.ALL_MESSAGE_CONSUMED);
									result.setErrorMessage("all messages have been consumed, please try later");
								} else {
									int totalFetchedSize = 0;
									if (maxFetchSize > 0) { // batch fetch
										while(!log.isEmpty(fanoutId)) {
											
											int length = log.getItemLength(fanoutId);
											if (totalFetchedSize + length > maxFetchSize) {
												break;
											}
											
											byte[] item = log.read(fanoutId);
											response.addToItemList(ByteBuffer.wrap(item));
											totalFetchedSize += length;
										} 
									} else { // fetch one item
										byte[] item = log.read(fanoutId);
										response.addToItemList(ByteBuffer.wrap(item));
										totalFetchedSize += item.length;
									}
									BrokerTopicStat.getInstance(topic).recordBytesOut(totalFetchedSize);
									BrokerTopicStat.getBrokerAllTopicStat().recordBytesOut(totalFetchedSize);
									result.setResultCode(ResultCode.SUCCESS);
								}
							} finally {
								queueFrontWriteLock.unlock();
							}
						} else { // client managed index
							if (startIndex == log.getRearIndex()) {
								result.setResultCode(ResultCode.TRY_LATER);
								result.setErrorCode(ErrorCode.ALL_MESSAGE_CONSUMED);
								result.setErrorMessage("all messages have been consumed, please try later");
							} else {
								long index = startIndex;
								int totalFetchedSize = 0;
								
								if (maxFetchSize > 0) { // batch fetch
									while(index != log.getRearIndex()) {
										
										int length = log.getItemLength(index);
										if (totalFetchedSize + length > maxFetchSize) {
											break;
										}
										
										byte[] item = log.read(index);
										response.addToItemList(ByteBuffer.wrap(item));
										response.setLastConsumedIndex(index);
										if (index == Long.MAX_VALUE) {
											index = 0;
										} else {
											index++;
										}
										totalFetchedSize += length;
									}
								} else {
									byte[] item = log.read(index);
									response.addToItemList(ByteBuffer.wrap(item));
									response.setLastConsumedIndex(index);
									totalFetchedSize += item.length;
								}
								BrokerTopicStat.getInstance(topic).recordBytesOut(totalFetchedSize);
								BrokerTopicStat.getBrokerAllTopicStat().recordBytesOut(totalFetchedSize);
								result.setResultCode(ResultCode.SUCCESS);
							}
						}
					
					}
				
				} finally {
					innerArrayReadLock.unlock();
				}
				
			} else {
				result.setResultCode(ResultCode.FAILURE);
				result.setErrorCode(ErrorCode.TOPIC_NOT_EXIST);
				result.setErrorMessage(String.format("topic %s does not exist on broker %d", topic, config.getBrokerId()));
			}
		} catch (Exception e) {
			handleException(e, result, consumeRequest);
			BrokerTopicStat.getInstance(consumeRequest.getTopic()).recordFailedConsumeRequest();
			BrokerTopicStat.getBrokerAllTopicStat().recordFailedConsumeRequest();
		}
		
		stats.recordRequest(ConsumeRequest.class, System.nanoTime() - start);
		
		return response;
	}

	@Override
	public FindClosestIndexByTimeResponse findClosestIndexByTime(
			FindClosestIndexByTimeRequest findClosestIndexByTimeRequest) throws TException {
		FindClosestIndexByTimeResponse response = new FindClosestIndexByTimeResponse();
		Result result = new Result();
		response.setResult(result);
		
		try {
			String topic = findClosestIndexByTimeRequest.getTopic();
			long timestamp = findClosestIndexByTimeRequest.getTimestamp();
			
			ILog log = this.getLog(topic);
			if (log != null) {
				
				Lock innerArrayReadLock = log.getInnerArrayReadLock();
				try {
					innerArrayReadLock.lock();
				
					if (timestamp == Constants.EARLIEST_TIME) {
						long index = log.getFrontIndex(); // queue front index is the earliest index
						response.setIndex(index);
						if (!log.isEmpty()) {
							long ts = log.getTimestamp(index);
							response.setTimestampOfIndex(ts);
						}
					    result.setResultCode(ResultCode.SUCCESS);
					} else if (timestamp == Constants.LATEST_TIME) {
						long index = log.getRearIndex(); // queue rear index is the latest index
						response.setIndex(index);
						if (!log.isEmpty()) {
							index--;
							if (index < 0) {
								index = Long.MAX_VALUE;
							}
							long ts = log.getTimestamp(index);
							response.setTimestampOfIndex(ts);
						}
						result.setResultCode(ResultCode.SUCCESS);
					} else {
						boolean isEmpty = log.isEmpty();
						if (isEmpty) {
							result.setResultCode(ResultCode.FAILURE);
							result.setErrorCode(ErrorCode.TOPIC_IS_EMPTY);
							result.setErrorMessage(String.format("topic %s is empty on broker %d", topic, config.getBrokerId()));
						} else {
							long index = log.getClosestIndex(timestamp);
							long ts = log.getTimestamp(index);
							response.setIndex(index);
							response.setTimestampOfIndex(ts);
						    result.setResultCode(ResultCode.SUCCESS);
						}
					}
				
				} finally {
					innerArrayReadLock.unlock();
				}
			} else {
				result.setResultCode(ResultCode.FAILURE);
				result.setErrorCode(ErrorCode.TOPIC_NOT_EXIST);
				result.setErrorMessage(String.format("topic %s does not exist on broker %d", topic, config.getBrokerId()));
			}
			
		} catch (Exception e) {
			handleException(e, result, findClosestIndexByTimeRequest);
		}
		
		return response;
		
	}
	
	/**
	 * delete topic which is never used
	 * 
	 * <p>
	 * This will delete all log files and remove node data from zookeeper.
	 * </p>
	 * 
	 * @param topic name of topic
	 * @param password 
	 * @return ture if succeed, false if auth fail
	 */
	private boolean deleteLog(String topic, String password) {
		if (!config.getAuthentication().auth(password)) {
			return false;
		}
		synchronized(logCreationLock) {
			Log log = logs.remove(topic);
			if (log != null) {
				log.delete();
			}
		}
		return true;
	}

	@Override
	public DeleteTopicResponse deleteTopic(DeleteTopicRequest deleteTopicRequest)
			throws TException {
		DeleteTopicResponse response = new DeleteTopicResponse();
		Result result = new Result();
		response.setResult(result);
		
		try {
			
			String topic = deleteTopicRequest.getTopic();
			String password = deleteTopicRequest.getPassword();
			boolean success = this.deleteLog(topic, password);
			if (!success) {
				result.setResultCode(ResultCode.FAILURE);
				result.setErrorCode(ErrorCode.AUTHENTICATION_FAILURE);
				result.setErrorMessage("authentication failed");
			} else {
				result.setResultCode(ResultCode.SUCCESS);
			}
		} catch (Exception e) {
			handleException(e, result, deleteTopicRequest);
		}
		
		return response;
	}

	@Override
	public GetSizeResponse getSize(GetSizeRequest getSizeRequest)
			throws TException {
		GetSizeResponse response = new GetSizeResponse();
		Result result = new Result();
		response.setResult(result);
		
		try {
			String topic = getSizeRequest.getTopic();
			String fanoutId = getSizeRequest.getFanoutId();
			
			ILog log = this.getLog(topic);
			if (log != null) {
				Lock innerArrayReadLock = log.getInnerArrayReadLock();
				try {
					innerArrayReadLock.lock();
				
					long size = -1;
					if (Utils.isStringEmpty(fanoutId) ) {
						size = log.getSize();
					} else {
						size = log.getSize(fanoutId);
					}
					response.setSize(size);
				    result.setResultCode(ResultCode.SUCCESS);
			    
				} finally {
					innerArrayReadLock.unlock();
				}
			} else {
				result.setResultCode(ResultCode.FAILURE);
				result.setErrorCode(ErrorCode.TOPIC_NOT_EXIST);
				result.setErrorMessage(String.format("topic %s does not exist on broker %d", topic, config.getBrokerId()));
			}
		} catch (Exception e) {
			handleException(e, result, getSizeRequest);
		}
		
		return response;
	}

	
	// common exception handling
	private void handleException(Exception e, Result result, Object requestObject) {
        logger.error("error when processing request " + requestObject, e);
		
        result.setResultCode(ResultCode.FAILURE);
		
        ErrorCode errorCode = ErrorMapper.toErrorCode(e);
		result.setErrorCode(errorCode);
		
		result.setErrorMessage(e.getMessage());
	}
}
