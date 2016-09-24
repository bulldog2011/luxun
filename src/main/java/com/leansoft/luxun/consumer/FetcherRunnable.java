package com.leansoft.luxun.consumer;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TException;

import com.leansoft.luxun.api.generated.ConsumeRequest;
import com.leansoft.luxun.api.generated.ConsumeResponse;
import com.leansoft.luxun.api.generated.ErrorCode;
import com.leansoft.luxun.api.generated.Result;
import com.leansoft.luxun.api.generated.ResultCode;
import com.leansoft.luxun.common.annotations.ClientSide;
import com.leansoft.luxun.common.exception.ErrorMapper;
import com.leansoft.luxun.message.MessageList;

/**
 * 
 * @author bulldog
 *
 */
@ClientSide
public class FetcherRunnable extends Thread {
	
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final SimpleConsumer simpleConsumer;

    private volatile boolean stopped = false;

    private final ConsumerConfig config;

    private final TopicInfo topicInfo;

    private final Logger logger = LoggerFactory.getLogger(FetcherRunnable.class);

    private final static AtomicInteger threadIndex = new AtomicInteger(0);
    
    private int errorRetryCount = 0;
    
    public FetcherRunnable(String name,//
            ConsumerConfig config,
            TopicInfo topicInfo) {
        super(name + "-" + threadIndex.getAndIncrement());
        this.config = config;
        this.topicInfo = topicInfo;
        this.simpleConsumer = new SimpleConsumer(topicInfo.broker.host, topicInfo.broker.port, config.getSocketTimeoutMs(), config.getConnectTimeoutMs());
    }
    
    @Override
    public void run() {
        logger.info(String.format("%s consume at %s:%d with %s", getName(), topicInfo.broker.host, topicInfo.broker.port, topicInfo.topic));
        
        try {
        	final long maxFetchBackoffMs = config.getMaxFetchBackoffMs();
        	long fetchBackoffMs = config.getFetchBackoffMs();
        	while (!stopped) {
        		if (fetchOnce() <= 0) { // read zero message
        			if (logger.isDebugEnabled()) {
        				logger.debug("backing off " + fetchBackoffMs + " ms");
        			}
        			Thread.sleep(fetchBackoffMs);
        			if (fetchBackoffMs < maxFetchBackoffMs) {
        				fetchBackoffMs += fetchBackoffMs / 10;
        			}
        		} else {
        			fetchBackoffMs = config.getFetchBackoffMs();
        		}
        	}
        } catch (Exception e) {
        	if (stopped) {
        		logger.info("FetchRunnable " + this + " interrupted");
        	} else {
        		logger.info("error in FetcherRunnable", e);
        	}
        }
        
        logger.info("stopping fetcher " + getName() + " to broker " + topicInfo.broker);
        simpleConsumer.close();
        shutdownComplete();
    }
    
    private int fetchOnce() throws TException, InterruptedException {
		ConsumeRequest consumeRequest = new ConsumeRequest();
		consumeRequest.setTopic(topicInfo.topic);
		consumeRequest.setFanoutId(config.getGroupId());
		consumeRequest.setMaxFetchSize(config.getFetchSize());
	    
		ConsumeResponse consumeResponse = simpleConsumer.consume(consumeRequest);
			
    	int read = processResponse(consumeResponse);
    	
    	if (read >= 0) {
    		this.errorRetryCount = 0; // reset
    	}
    	
    	return read;
    }
    
    private int processResponse(ConsumeResponse consumeResponse) throws InterruptedException, TException {
    	
    	Result result = consumeResponse.getResult();
    	if (result.getResultCode() == ResultCode.SUCCESS) {
    		int size = 0;
    		
    		for(ByteBuffer buffer : consumeResponse.getItemList()) {
    			MessageList messageList = MessageList.fromThriftBuffer(buffer);
    			size += topicInfo.enqueue(messageList);
    		}
    		
    		return size;
    	} else if (result.getResultCode() == ResultCode.TRY_LATER) {
    		return 0;
    	} else {
    		
    		if (result.getErrorCode() == ErrorCode.TOPIC_IS_EMPTY || 
    			result.getErrorCode() == ErrorCode.ALL_MESSAGE_CONSUMED ||
    			result.getErrorCode() == ErrorCode.TOPIC_NOT_EXIST) {
    			
    			return 0; // just wait a moment to let producers produce some messages
    		}
    		
    		errorRetryCount++;
    		
    		if (errorRetryCount > config.getConsumerNumRetires()) {
    			RuntimeException re =  ErrorMapper.toException(result.getErrorCode(), result.getErrorMessage());
        		logger.error("error to consume message", re);
        		throw re;
    		} else {
    			return -1; // error retry;
    		}
    	}
    }
    
    public void shutdown() throws InterruptedException {
        logger.debug("shutdown the fetcher " + getName());
        stopped = true;
        shutdownLatch.await(5,TimeUnit.SECONDS);
    }
    
    private void shutdownComplete() {
        this.shutdownLatch.countDown();
    }
	
}
