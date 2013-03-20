package com.leansoft.luxun.producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.leansoft.luxun.broker.Broker;
import com.leansoft.luxun.common.annotations.ClientSide;
import com.leansoft.luxun.common.exception.InvalidConfigException;
import com.leansoft.luxun.common.exception.UnavailableProducerException;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.producer.async.AsyncProducer;
import com.leansoft.luxun.producer.async.AsyncProducerConfig;
import com.leansoft.luxun.producer.async.CallbackHandler;
import com.leansoft.luxun.producer.async.DefaultEventHandler;
import com.leansoft.luxun.producer.async.EventHandler;
import com.leansoft.luxun.serializer.Encoder;
import com.leansoft.luxun.utils.Utils;

/**
 * 
 * @author bulldog
 *
 * @param <V>
 */

@ClientSide
public class ProducerPool<V> implements Closeable {
	
    private final ProducerConfig config;

    private final Encoder<V> serializer;

    private final ConcurrentMap<Integer, SyncProducer> syncProducers;

    private final ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers;

    private final EventHandler<V> eventHandler;

    private final CallbackHandler<V> callbackHandler;

    private boolean sync = true;
    
    private final Set<String> compressedTopics;

    private final Logger logger = Logger.getLogger(ProducerPool.class);

    public ProducerPool(ProducerConfig config,//
            Encoder<V> serializer, //
            ConcurrentMap<Integer, SyncProducer> syncProducers,//
            ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers, //
            EventHandler<V> inputEventHandler, //
            CallbackHandler<V> callbackHandler) {
        super();
        this.config = config;
        this.serializer = serializer;
        this.syncProducers = syncProducers;
        this.asyncProducers = asyncProducers;
        this.eventHandler = inputEventHandler != null ? inputEventHandler : new DefaultEventHandler<V>(config, callbackHandler);
        this.callbackHandler = callbackHandler;
        if (serializer == null) {
            throw new InvalidConfigException("serializer passed in is null!");
        }
        this.sync = !"async".equalsIgnoreCase(config.getProducerType());
        this.compressedTopics = new HashSet<String>(config.getCompressedTopics());
    }
    
    public ProducerPool(ProducerConfig config, Encoder<V> serializer,//
            EventHandler<V> eventHandler,//
            CallbackHandler<V> callbackHandler) {
        this(config,//
                serializer,//
                new ConcurrentHashMap<Integer, SyncProducer>(),//
                new ConcurrentHashMap<Integer, AsyncProducer<V>>(),//
                eventHandler,//
                callbackHandler);
    }

    @SuppressWarnings("unchecked")
    public ProducerPool(ProducerConfig config, Encoder<V> serializer) {
        this(config,//
                serializer,//
                new ConcurrentHashMap<Integer, SyncProducer>(),//
                new ConcurrentHashMap<Integer, AsyncProducer<V>>(),//
                (EventHandler<V>) Utils.getObject(config.getEventHandler()),//
                (CallbackHandler<V>) Utils.getObject(config.getCbkHandler()));
    }
    
    /**
     * add a new producer, either synchronous or asynchronous, connecting
     * to the specified broker
     * 
     * @param broker broker to producer
     */
    public void addProducerForBroker(Broker broker) {
        Properties props = new Properties();
        props.put("host", broker.host);
        props.put("port", "" + broker.port);
        props.putAll(config.getProperties());
        if (sync) {
            SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
            logger.info("Creating sync producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port);
            syncProducers.put(broker.id, producer);
        } else {
            AsyncProducer<V> producer = new AsyncProducer<V>(new AsyncProducerConfig(props),//
                    new SyncProducer(new SyncProducerConfig(props)),//
                    serializer,//
                    eventHandler,//
                    config.getEventHandlerProperties(),//
                    this.callbackHandler, //
                    config.getCbkHandlerProperties());
            producer.start();
            logger.info("Creating async producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port);
            asyncProducers.put(broker.id, producer);
        }
    }
    
    /**
     * selects either a synchronous or an asynchronous producer, for the
     * specified broker id and calls the send API on the selected producer
     * to publish the data to the specified broker
     * 
     * @param ppd the producer pool request object
     */
    public void send(ProducerPoolData<V> ppd) {
        if (logger.isDebugEnabled()) {
            logger.debug("send message: " + ppd);
        }
        
        if (sync) {
			MessageList messageList = convert(ppd);
			SyncProducer producer = syncProducers.get(ppd.broker.id);
            if (producer == null) {
                throw new UnavailableProducerException("Producer pool has not been initialized correctly. " + "Sync Producer for broker "
                        + ppd.broker + " does not exist in the pool");
            }
            producer.send(ppd.topic, messageList);
        } else {
        	AsyncProducer<V> asyncProducer = asyncProducers.get(ppd.broker.id);
            if (asyncProducer == null) {
                throw new UnavailableProducerException("Producer pool has not been initialized correctly. " + "Async Producer for broker "
                        + ppd.broker + " does not exist in the pool");
            }
        	for(V v : ppd.data) {
        		asyncProducer.send(ppd.topic, v);
        	}
        }
    }
    
    public void send(List<ProducerPoolData<V>> poolData) {
    	for(ProducerPoolData<V> ppd : poolData) {
    		send(ppd);
    	}
    }
    
    // Convert to Luxun message list format
    private MessageList convert(ProducerPoolData<V> ppd) {
    	
    	CompressionCodec codec = config.getCompressionCodec();
    	if (codec != CompressionCodec.NO_COMPRESSION && !compressedTopics.isEmpty() && !compressedTopics.contains(ppd.topic)) {
    		codec = CompressionCodec.NO_COMPRESSION;
    	}
    	MessageList messageList = new MessageList(codec);
    	
    	for(V v : ppd.data) {
    		Message message  = serializer.toMessage(v);
    		messageList.add(message);
    	}
    	
    	return messageList;
    }

    /**
     * Closes all the producers in the pool
     */
	@Override
	public void close() throws IOException {
        logger.info("Closing all producers");
        if (sync) {
            for (SyncProducer p : syncProducers.values()) {
                p.close();
            }
        } else {
            for (AsyncProducer<V> p : asyncProducers.values()) {
                p.close();
            }
        }
	}
	
    /**
     * This constructs and returns the value object for the producer pool
     * 
     * @param topic the topic to which the data should be published
     * @param broker the broker
     * @param data the data to be published
     */
    public ProducerPoolData<V> buildProducerPoolData(String topic, Broker broker, List<V> data) {
        return new ProducerPoolData<V>(topic, broker, data);
    }

}
