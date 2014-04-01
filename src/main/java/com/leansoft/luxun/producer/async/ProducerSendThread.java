package com.leansoft.luxun.producer.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.leansoft.luxun.common.exception.IllegalQueueStateException;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.serializer.Encoder;

public class ProducerSendThread<T> extends Thread {

    final String threadName;

    final BlockingQueue<QueueItem<T>> queue;

    final Encoder<T> serializer;

    final SyncProducer underlyingProducer;

    final EventHandler<T> eventHandler;

    final CallbackHandler<T> callbackHandler;

    final long queueTime;

    final int batchSize;

    private final Logger logger = Logger.getLogger(ProducerSendThread.class);

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    
    private final Object shutdownCommand;
    
    public ProducerSendThread(String threadName, //
            BlockingQueue<QueueItem<T>> queue, //
            Encoder<T> serializer, //
            SyncProducer underlyingProducer,//
            EventHandler<T> eventHandler, //
            CallbackHandler<T> callbackHandler, //
            long queueTime, //
            int batchSize,
            Object shutdownCommand) {
        super();
        this.threadName = threadName;
        this.queue = queue;
        this.serializer = serializer;
        this.underlyingProducer = underlyingProducer;
        this.eventHandler = eventHandler;
        this.callbackHandler = callbackHandler;
        this.queueTime = queueTime;
        this.batchSize = batchSize;
        this.shutdownCommand = shutdownCommand;
    }
    
    public void run() {
    	try {
            List<QueueItem<T>> remainingEvents = processEvents();
            //handle remaining events
            if (remainingEvents.size() > 0) {
                logger.debug(String.format("Dispatching last batch of %d events to the event handler", remainingEvents.size()));
                tryToHandle(remainingEvents);
            }
    	} catch (Exception e) {
            logger.error("Error in sending events: ", e);
        } finally {
            shutdownLatch.countDown();
        }
    }
    
    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
    }

    public void shutdown() {
        eventHandler.close();
        logger.info("Shutdown thread complete");
    }
    
    private List<QueueItem<T>> processEvents() {
    	long lastSend = System.currentTimeMillis();
    	final List<QueueItem<T>> events = new ArrayList<QueueItem<T>>();
    	boolean full = false;
    	while(true) {
    		try {
    			QueueItem<T> item = queue.poll(Math.max(0, (lastSend + queueTime) - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    			
    			long elapsed = System.currentTimeMillis() - lastSend;
    			boolean expired = item == null;
    			if (item != null) {
    				
        			if (item.data == shutdownCommand) {
        				logger.info("Producer sending thread got a shutdown command, exit...");
        				break;
        			}
    				
					if (callbackHandler != null) {
						List<QueueItem<T>> items = callbackHandler.afterDequeuingExistingData(item);
						if (items != null) {
							events.addAll(items);
						} else {
							events.add(item);
						}
					} else {
						events.add(item);
					}
    				full = events.size() >= batchSize;
    			}
    			if (full || expired) {
                    if (logger.isDebugEnabled()) {
                        if (expired) {
                            logger.debug(elapsed + " ms elapsed. Queue time reached. Sending..");
                        } else {
                            logger.debug(String.format("Batch(%d) full. Sending..", batchSize));
                        }
                    }
                    tryToHandle(events);
                    lastSend = System.currentTimeMillis();
                    events.clear();
    			}
    		} catch (InterruptedException e) {
    			logger.warn(e.getMessage(), e);
    		}
    	}
        if (queue.size() > 0) {
            throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, " + queue.size() + " remaining items in the queue");
        }
        if (this.callbackHandler != null) {
            List<QueueItem<T>> items = this.callbackHandler.lastBatchBeforeClose();
            if (items != null) {
                events.addAll(items);
            }
        }
        return events;
    }
    
    private void tryToHandle(List<QueueItem<T>> events) {
        if (logger.isDebugEnabled()) {
            logger.debug("handling " + events.size() + " events");
        }
        if (events.size() > 0) {
            try {
                this.eventHandler.handle(events, underlyingProducer, serializer);
            } catch (RuntimeException e) {
                logger.error("Error in handling batch of " + events.size() + " events", e);
            }
        }
    }
}
