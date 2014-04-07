package com.leansoft.luxun.producer.async;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.leansoft.luxun.common.exception.AsyncProducerInterruptedException;
import com.leansoft.luxun.common.exception.QueueClosedException;
import com.leansoft.luxun.common.exception.QueueFullException;
import com.leansoft.luxun.mx.AsyncProducerQueueSizeStats;
import com.leansoft.luxun.mx.AsyncProducerStats;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.serializer.Encoder;
import com.leansoft.luxun.utils.Utils;

/**
 * 
 * @author bulldog
 *
 */
public class AsyncProducer<T> implements Closeable {
	
    private final Logger logger = Logger.getLogger(AsyncProducer.class);
    private static final Random random = new Random();
    private static final String ProducerQueueSizeMBeanName = "luxun.producer.Producer:type=AsyncProducerQueueSizeStats";
    /////////////////////////////////////////////////////////////////////
    final AsyncProducerConfig config;

    final SyncProducer producer;

    final Encoder<T> serializer;

    final EventHandler<T> eventHandler;
    final Properties eventHandlerProperties;
    final CallbackHandler<T> callbackHandler;
    final Properties callbackHandlerProperties;
    /////////////////////////////////////////////////////////////////////
    final AtomicBoolean closed = new AtomicBoolean(false);
    final LinkedBlockingQueue<QueueItem<T>> queue;
    final int asyncProducerID = AsyncProducer.random.nextInt();
    /////////////////////////////////////////////////////////////////////
    final ProducerSendThread<T> sendThread;
    private final int enqueueTimeoutMs;
    
    private final Object shutdownData = new Object();

    public AsyncProducer(AsyncProducerConfig config, //
            SyncProducer producer, //
            Encoder<T> serializer, //
            EventHandler<T> eventHandler,//
            Properties eventHandlerProperties, //
            CallbackHandler<T> callbackHandler, //
            Properties callbackHandlerProperties) {
    	super();
    	this.config = config;
    	this.producer = producer;
    	this.serializer = serializer;
    	this.eventHandler = eventHandler;
    	this.eventHandlerProperties = eventHandlerProperties;
    	this.callbackHandler = callbackHandler;
    	this.callbackHandlerProperties = callbackHandlerProperties;
    	this.enqueueTimeoutMs = config.getEnqueueTimeoutMs();
    	
    	this.queue = new LinkedBlockingQueue<QueueItem<T>>(config.getQueueSize());
    	
    	if (eventHandler != null) {
    		eventHandler.init(eventHandlerProperties);
    	}
    	if (callbackHandler != null) {
    		callbackHandler.init(callbackHandlerProperties);
    	}
		this.sendThread = new ProducerSendThread<T>("ProducerSendThread-" + asyncProducerID,
				queue,
				serializer,
				producer,
				eventHandler != null ? eventHandler : 
					new DefaultEventHandler<T>(new ProducerConfig(config.getProperties()), callbackHandler),
				callbackHandler,
				config.getQueueTime(),
				config.getBatchSize(),
				shutdownData);
		this.sendThread.setDaemon(false);
		AsyncProducerQueueSizeStats<T> stats = new AsyncProducerQueueSizeStats<T>(queue);
		stats.setMbeanName(ProducerQueueSizeMBeanName + "-" + asyncProducerID);
		Utils.registerMBean(stats);
	}
    
    @SuppressWarnings("unchecked")
    public AsyncProducer(AsyncProducerConfig config) {
        this(config//
                , new SyncProducer(config)//
                , (Encoder<T>)Utils.getObject(config.getSerializerClass())//
                , (EventHandler<T>)Utils.getObject(config.getEventHandler())//
                , config.getEventHandlerProperties()//
                , (CallbackHandler<T>)Utils.getObject(config.getCbkHandler())//
                , config.getCbkHandlerProperties());
    }
    
    public void start() {
    	sendThread.start();
    }
    
    public void send(String topic, T event) {
    	AsyncProducerStats.recordEvent();
    	if (closed.get()) {
            throw new QueueClosedException("Attempt to add event to a closed queue.");
    	}
    	QueueItem<T> data = new QueueItem<T>(event, topic);
        if (this.callbackHandler != null) {
            QueueItem<T> items = this.callbackHandler.beforeEnqueue(data);
            if (items != null) {
                data = items;
            }
        }
    	boolean added = false;
    	try {
    		if (enqueueTimeoutMs == 0) {
    			added = queue.offer(data);
    		} else if (enqueueTimeoutMs < 0) {
    			queue.put(data);
    			added = true;
    		} else {
    			added = queue.offer(data, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
    		}
    	} catch (InterruptedException e) {
    		throw new AsyncProducerInterruptedException(e);
    	}
    	if (this.callbackHandler != null) {
    		this.callbackHandler.afterEnqueue(data, added);
    	}
    	if (!added) {
    		AsyncProducerStats.recordDroppedEvents();
    		throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + event);
    	}
    	
    }

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void close() throws IOException {
		if (this.callbackHandler != null) {
			callbackHandler.close();
		}
		closed.set(true);
		try {
			queue.put(new QueueItem(shutdownData, null));
		} catch (InterruptedException e) {
    		throw new AsyncProducerInterruptedException(e);
		}
		sendThread.shutdown();
		sendThread.awaitShutdown();
		producer.close();
		logger.info("Closed AsyncProducer");
	}

}
