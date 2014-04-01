package com.leansoft.luxun.producer.async;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.leansoft.luxun.api.generated.ProduceRequest;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.serializer.Encoder;

public class DefaultEventHandler<T> implements EventHandler<T> {

    private final CallbackHandler<T> callbackHandler;

    private final Set<String> compressedTopics;

    private final CompressionCodec codec;

    private final Logger logger = Logger.getLogger(DefaultEventHandler.class);

    private final int numRetries;
    
    public DefaultEventHandler(ProducerConfig producerConfig, CallbackHandler<T> callbackHandler) {
        this.callbackHandler = callbackHandler;
        this.compressedTopics = new HashSet<String>(producerConfig.getCompressedTopics());
        this.codec = producerConfig.getCompressionCodec();
        this.numRetries = producerConfig.getNumRetries();
    }

	@Override
	public void init(Properties properties) {
		
	}
	
	private void send(List<ProduceRequest> produceRequests, SyncProducer syncProducer) {
		if (produceRequests.isEmpty()) {
			return;
		}
		final int maxAttempts = 1 + numRetries;
		for(int i = 0; i < maxAttempts; i++) {
			try {
				syncProducer.multiSend(produceRequests);
				break;
			} catch (RuntimeException e) {
                logger.warn("error sending message, attempts times: " + i, e);
                if (i == maxAttempts - 1) {
                    throw e;
                }
			}
		}
	}
	
	private List<ProduceRequest> collate(List<QueueItem<T>> events, Encoder<T> encoder) {
		final Map<String, MessageList> topicData = new HashMap<String, MessageList>();
		for(QueueItem<T> event : events) {
			MessageList messageList = topicData.get(event.topic);
			if (messageList == null) {
				CompressionCodec useCodec = codec;
				if (codec != CompressionCodec.NO_COMPRESSION && !compressedTopics.isEmpty() && !compressedTopics.contains(event.topic)) {
					useCodec = CompressionCodec.NO_COMPRESSION;
				}
				messageList = new  MessageList(useCodec);
				topicData.put(event.topic, messageList);
			}
			Message message = encoder.toMessage(event.data);
			messageList.add(message);
		}
		final List<ProduceRequest> requests = new ArrayList<ProduceRequest>();
		for(String topic : topicData.keySet()) {
			MessageList messageList = topicData.get(topic);
			ProduceRequest produceRequest = new ProduceRequest();
			produceRequest.setTopic(topic);
			ByteBuffer buffer = messageList.toThriftBuffer();
			produceRequest.setItem(buffer);
			requests.add(produceRequest);
		}
		return requests;
	}

	@Override
	public void handle(List<QueueItem<T>> events, SyncProducer producer,
			Encoder<T> encoder) {
		List<QueueItem<T>> processedEvents = events;
		if (this.callbackHandler != null) {
		    List<QueueItem<T>> items = this.callbackHandler.beforeSendingData(events);
		    if (items != null) {
    			processedEvents = items;
		    }
		}
		List<ProduceRequest> collatedRequests = collate(processedEvents, encoder);
		send(collatedRequests, producer);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
