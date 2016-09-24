package com.leansoft.luxun.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leansoft.luxun.broker.Broker;
import com.leansoft.luxun.broker.BrokerInfo;
import com.leansoft.luxun.broker.ConfigBrokerInfo;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.serializer.Decoder;
import com.leansoft.luxun.serializer.DefaultDecoder;

/**
 * Consumer factory builds stream for message consuming
 * 
 * @author bulldog
 *
 */
public class StreamFactory implements IStreamFactory {
	
    public static final FetchedDataChunk SHUTDOWN_COMMAND = new FetchedDataChunk(null, null);
    
    public final static int CLOSE_TIMEOUT_IN_SECONDS = 10; // seconds
    
    private final Logger logger = LoggerFactory.getLogger(StreamFactory.class);
    
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    
    final private ConsumerConfig config;
    
    private Fetcher fetcher;
    
    final boolean enableFetcher;
    
    final BrokerInfo brokerInfo;
    
    final List<BlockingQueue<FetchedDataChunk>> queues;
    
    /**
     * Create a consumer, at minimum, groupid(aka consumer group name) and broker.list must be specified in the config
     * 
     * @param config consumer config
     */
    public StreamFactory(ConsumerConfig config) {
    	this(config, true);
    }
    
    /**
     * Create a consumer, at minimum, groupid(aka consumer group name) and broker list must be specified in the config
     * 
     * @param config consumer config
     * @param enableFetcher just for testing
     */
    public StreamFactory(ConsumerConfig config, boolean enableFetcher) {
    	this.config = config;
    	this.enableFetcher = enableFetcher;
    	this.brokerInfo = new ConfigBrokerInfo(this.config.getBrokerList());
        if (enableFetcher) {
            this.fetcher = new Fetcher(config);
        }
        this.queues = new ArrayList<BlockingQueue<FetchedDataChunk>>();
    }
    

	@Override
	public <T> Map<String, List<MessageStream<T>>> createMessageStreams(
			Map<String, Integer> topicThreadNumMap, Decoder<T> decoder) {
		return this.buildStreamMap(topicThreadNumMap, decoder);
	}
	

	@Override
	public <T> Map<String, List<MessageStream<Message>>> createMessageStreams(
			Map<String, Integer> topicThreadNumMap) {
		return this.buildStreamMap(topicThreadNumMap, new DefaultDecoder());
	}
	
	
	private <T> Map<String, List<MessageStream<T>>> buildStreamMap(Map<String, Integer> topicThreadNumMap, Decoder<T> decoder) {
        if (topicThreadNumMap == null) {
            throw new IllegalArgumentException("topicThreadNumMap is null");
        }
        if (decoder == null) {
            throw new IllegalArgumentException("decoder is null");
        }
        
        this.queues.clear();
        
        List<TopicInfo> topicInfos = new ArrayList<TopicInfo>();
        
        final Map<String, List<MessageStream<T>>> streamsMap = new HashMap<String, List<MessageStream<T>>>();
        for(String topic : topicThreadNumMap.keySet()) {
        	Integer threadNum = topicThreadNumMap.get(topic);
            LinkedBlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>(
                    config.getMaxQueuedChunks());
            queues.add(queue);
            List<MessageStream<T>> streams = new ArrayList<MessageStream<T>>();
            for(int i = 0; i < threadNum; i++) {
            	streams.add(new MessageStream<T>(topic, queue, config.getConsumerTimeoutMs(), decoder));
            }
            streamsMap.put(topic, streams);
            
            for(Integer brokerId : this.brokerInfo.getBrokerIdList()) {
            	Broker broker = this.brokerInfo.getBrokerInfo(brokerId);
            	TopicInfo topicInfo = new TopicInfo(topic, broker, queue);
            	topicInfos.add(topicInfo);
            }
            
        }
        
        fetcher.startConnections(topicInfos);
        
        return streamsMap;
	}

	@Override
	public void close() throws IOException {
		if (this.isShuttingDown.compareAndSet(false, true)) {
            logger.info("Consumer shutting down");
            try {
            	if (fetcher != null) {
            		fetcher.stopConnectionsToAllBrokers();
            	}
            	
            	this.sendShutdownToAllQueues();
            	
        		int timeWaited = 0;
        		int checkInterval = 1000;
        		while(!this.isAllQueuesEmpty()) {
        			logger.info("Wating for queues to become empty, time waited : " + timeWaited / 1000 + " s.");
        			if (timeWaited > CLOSE_TIMEOUT_IN_SECONDS * 1000) {
        				String errorMessage = "fail to wait for queues to become timeout within timeout " + CLOSE_TIMEOUT_IN_SECONDS + " seconds";
        				logger.error(errorMessage);
        				throw new RuntimeException(errorMessage);
        			}
        			try {
        				Thread.sleep(checkInterval);
        				timeWaited += checkInterval;
        			} catch (InterruptedException e) {
        				// ignore
        			}
        		}
                logger.info("Comsumer shutdown completed");
            } catch (Exception e) {
                logger.error("error during consumer shutdown", e);
            }
		}
	}
	
	private boolean isAllQueuesEmpty() {
        for (BlockingQueue<FetchedDataChunk> queue : queues) {
        	if (!queue.isEmpty()) {
        		if (queue.size() == 1 && queue.peek() != SHUTDOWN_COMMAND) {
        			return false;
        		} else if (queue.size() > 1) {
        			return false;
        		}
        	}
        }
        return true;
	}
	
    private void sendShutdownToAllQueues() {
        for (BlockingQueue<FetchedDataChunk> queue : queues) {
            try {
                queue.put(SHUTDOWN_COMMAND);
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(),e);
            }
        }
    }

}
