package com.leansoft.luxun.consumer;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leansoft.luxun.common.annotations.ClientSide;

/**
 * 
 * @author bulldog
 *
 */
@ClientSide
public class Fetcher {

    private final ConsumerConfig config;
    
    private final Logger logger = LoggerFactory.getLogger(Fetcher.class);

    private volatile List<FetcherRunnable> fetcherThreads = new ArrayList<FetcherRunnable>(0);
    
    public Fetcher(ConsumerConfig config) {
        this.config = config;
    }
    
    public void stopConnectionsToAllBrokers() {
        // shutdown the old fetcher threads, if any
        List<FetcherRunnable> threads = this.fetcherThreads;
        for (FetcherRunnable fetcherThread : threads) {
            try {
                fetcherThread.shutdown();
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(),e);
            }
        }
        this.fetcherThreads = new ArrayList<FetcherRunnable>(0);
    }
    
    public <T> void startConnections(Iterable<TopicInfo> topicInfos){
        if(topicInfos == null) {
            return;
        }
        
        final List<FetcherRunnable> fetcherThreads = new ArrayList<FetcherRunnable>();
        for(TopicInfo topicInfo : topicInfos) {
            FetcherRunnable fetcherThread = new FetcherRunnable(
            		"FetchRunnable",
                    config,
                    topicInfo);
            fetcherThreads.add(fetcherThread);
            fetcherThread.start();
        }
        //
        this.fetcherThreads = fetcherThreads;
    }
}
