package com.leansoft.luxun.producer;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.easymock.EasyMock;
import org.junit.Test;

import static com.leansoft.luxun.utils.CollectionEqualsMatcher.colEq;

import com.leansoft.luxun.api.generated.ProduceRequest;
import com.leansoft.luxun.common.exception.QueueClosedException;
import com.leansoft.luxun.common.exception.QueueFullException;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.producer.SyncProducerConfig;
import com.leansoft.luxun.producer.async.AsyncProducer;
import com.leansoft.luxun.producer.async.AsyncProducerConfig;
import com.leansoft.luxun.serializer.StringEncoder;
import com.leansoft.luxun.utils.TestUtils;

public class AsyncProducerTest {
	
	private String messageContent1 = "test";
	private String topic1 = "test-topic";
	private Message message1 = new Message(messageContent1.getBytes());
	
	private String messageContent2 = "test1";
	private String topic2 = "test1$topic";
	private Message message2 = new Message(messageContent2.getBytes());
	
	
	@Test
	public void testProducerQueueSize() throws Exception {
		SyncProducer basicProducer = EasyMock.createMock(SyncProducer.class);
		List<Message> messages = new ArrayList<Message>();
		messages.add(message1);
		List<ProduceRequest> produceRequests = new ArrayList<ProduceRequest>();
		produceRequests.add(new ProduceRequest(this.getMessageListOfSize(messages, 10).toThriftBuffer(), topic1));
		basicProducer.multiSend((List<ProduceRequest>) colEq(produceRequests));
		EasyMock.expectLastCall();
		basicProducer.close();
		EasyMock.expectLastCall();
		EasyMock.replay(basicProducer);
		
		Properties props = new Properties();
	    props.put("host", "127.0.0.1");
	    props.put("port", "9092");
	    props.put("queue.size", "10");
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("broker.list", TestUtils.brokerList);
	    AsyncProducerConfig config = new AsyncProducerConfig(props);
	    
	    AsyncProducer<String> producer = new AsyncProducer<String>(config, basicProducer, new StringEncoder(), null, null, null, null);
	    
	    try {
	    	for(int i = 0; i < 11; i++) {
	    		producer.send(topic1, messageContent1);
	    	}
	    	fail("Queue should be full");
	    } catch (QueueFullException qfe) {
	    	System.out.println("Queue is full");
	    }
	    
	    producer.start();
	    producer.close();
	    Thread.sleep(2000);
	    EasyMock.verify(basicProducer);
	}
	
	@Test
	public void testAddAfterQueueClosed() throws Exception {
		SyncProducer basicProducer = EasyMock.createMock(SyncProducer.class);
		List<Message> messages = new ArrayList<Message>();
		messages.add(message1);
		List<ProduceRequest> produceRequests = new ArrayList<ProduceRequest>();
		produceRequests.add(new ProduceRequest(this.getMessageListOfSize(messages, 10).toThriftBuffer(), topic1));
		basicProducer.multiSend((List<ProduceRequest>) colEq(produceRequests));
		EasyMock.expectLastCall();
		basicProducer.close();
		EasyMock.expectLastCall();
		EasyMock.replay(basicProducer);
		
		Properties props = new Properties();
	    props.put("host", "127.0.0.1");
	    props.put("port", "9092");
	    props.put("queue.size", "10");
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("broker.list", TestUtils.brokerList);
	    AsyncProducerConfig config = new AsyncProducerConfig(props);
	    
	    AsyncProducer<String> producer = new AsyncProducer<String>(config, basicProducer, new StringEncoder(), null, null, null, null);
	    
	    producer.start();
    	for(int i = 0; i < 10; i++) {
    		producer.send(topic1, messageContent1);
    	}
    	producer.close();
    	
    	try {
    		producer.send(topic1, messageContent1);
    	    fail("Queue should be closed");
    	} catch (QueueClosedException e) {
    		// expected
    	}
    	EasyMock.verify(basicProducer);
	}
	
	@Test
	public void testBatchSize() throws Exception {
		SyncProducer basicProducer = EasyMock.createMock(SyncProducer.class);
		List<Message> messages = new ArrayList<Message>();
		messages.add(message1);
		List<ProduceRequest> produceRequests = new ArrayList<ProduceRequest>();
		produceRequests.add(new ProduceRequest(this.getMessageListOfSize(messages, 5).toThriftBuffer(), topic1));
		basicProducer.multiSend((List<ProduceRequest>) colEq(produceRequests));
		EasyMock.expectLastCall().times(2);
		produceRequests = new ArrayList<ProduceRequest>();
		produceRequests.add(new ProduceRequest(this.getMessageListOfSize(messages, 1).toThriftBuffer(), topic1));
		basicProducer.multiSend((List<ProduceRequest>) colEq(produceRequests));
		EasyMock.expectLastCall();
		basicProducer.close();
		EasyMock.expectLastCall();
		EasyMock.replay(basicProducer);
		
		Properties props = new Properties();
	    props.put("host", "127.0.0.1");
	    props.put("port", "9092");
	    props.put("queue.size", "10");
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("batch.size", "5");
	    props.put("broker.list", TestUtils.brokerList);
	    AsyncProducerConfig config = new AsyncProducerConfig(props);
	    
	    AsyncProducer<String> producer = new AsyncProducer<String>(config, basicProducer, new StringEncoder(), null, null, null, null);
	    
	    producer.start();
    	for(int i = 0; i < 10; i++) {
    		producer.send(topic1, messageContent1);
    	}
    	
        Thread.sleep(100);
	    try {
	    	producer.send(topic1, messageContent1);
	    } catch (QueueFullException qfe) {
	    	fail("Queue should not be full");
	    }
    	
    	producer.close();
    	EasyMock.verify(basicProducer);
	}
	
	@Test
	public void testQueueTimeExpired() throws Exception {
		SyncProducer basicProducer = EasyMock.createMock(SyncProducer.class);
		List<Message> messages = new ArrayList<Message>();
		messages.add(message1);
		List<ProduceRequest> produceRequests = new ArrayList<ProduceRequest>();
		produceRequests.add(new ProduceRequest(this.getMessageListOfSize(messages, 3).toThriftBuffer(), topic1));
		basicProducer.multiSend((List<ProduceRequest>) colEq(produceRequests));
		EasyMock.expectLastCall();
		basicProducer.close();
		EasyMock.expectLastCall();
		EasyMock.replay(basicProducer);
		
		Properties props = new Properties();
	    props.put("host", "127.0.0.1");
	    props.put("port", "9092");
	    props.put("queue.size", "10");
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("queue.time", "200");
	    props.put("broker.list", TestUtils.brokerList);
	    AsyncProducerConfig config = new AsyncProducerConfig(props);
	    
	    AsyncProducer<String> producer = new AsyncProducer<String>(config, basicProducer, new StringEncoder(), null, null, null, null);
	    
	    producer.start();
    	for(int i = 0; i < 3; i++) {
    		producer.send(topic1, messageContent1);
    	}
    	
    	Thread.sleep(300);
    	producer.close();
    	EasyMock.verify(basicProducer);
	}
	
	@Test
	public void testSenderThreadShutdown() throws Exception {
		Properties syncProducerProps = new Properties();
	    syncProducerProps.put("host", "127.0.0.1");
	    syncProducerProps.put("port", "9092");
	    syncProducerProps.put("buffer.size", "1000");
	    syncProducerProps.put("connect.timeout.ms", "1000");
	    syncProducerProps.put("reconnect.interval", "1000");
	    
	    SyncProducer basicProducer = new MockProducer(new SyncProducerConfig(syncProducerProps));
	    
	    Properties asyncProducerProps = new Properties();
	    asyncProducerProps.put("host", "127.0.0.1");
	    asyncProducerProps.put("port", "9092");
	    asyncProducerProps.put("queue.size", "10");
	    asyncProducerProps.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    asyncProducerProps.put("queue.time", "100");
	    asyncProducerProps.put("broker.list", TestUtils.brokerList);
	    
	    AsyncProducerConfig config = new AsyncProducerConfig(asyncProducerProps);
	    AsyncProducer<String> producer = new AsyncProducer<String>(config, basicProducer, new StringEncoder(), null, null, null, null);
	    
	    producer.start();
		producer.send(topic1, messageContent1);
	    producer.close();
	}
	
    @Test
    public void testCollateEvents() throws Exception {
		SyncProducer basicProducer = EasyMock.createMock(SyncProducer.class);
		List<Message> messages = new ArrayList<Message>();
		messages.add(message1);
		List<ProduceRequest> produceRequests = new ArrayList<ProduceRequest>();
		produceRequests.add(new ProduceRequest(this.getMessageListOfSize(messages, 5).toThriftBuffer(), topic1));
		messages.clear();
		messages.add(message2);
		produceRequests.add(new ProduceRequest(this.getMessageListOfSize(messages, 5).toThriftBuffer(), topic2));
		basicProducer.multiSend((List<ProduceRequest>) colEq(produceRequests));
		EasyMock.expectLastCall();
		basicProducer.close();
		EasyMock.expectLastCall();
		EasyMock.replay(basicProducer);
		
		Properties props = new Properties();
	    props.put("host", "127.0.0.1");
	    props.put("port", "9092");
	    props.put("queue.size", "50");
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("batch.size", "10");
	    props.put("broker.list", TestUtils.brokerList);
	    AsyncProducerConfig config = new AsyncProducerConfig(props);
	    
	    AsyncProducer<String> producer = new AsyncProducer<String>(config, basicProducer, new StringEncoder(), null, null, null, null);
	    
	    producer.start();
	    for(int i = 0; i < 5; i++) {
	    	producer.send(messageContent1 + "-topic", messageContent1);
	    	producer.send(messageContent2 + "$topic", messageContent2);
	    }
	    
	    producer.close();
	    EasyMock.verify(basicProducer);
    }
	
	private MessageList getMessageListOfSize(List<Message> messages, int count) {
		MessageList messageList = new MessageList();
		for(Message message : messages) {
			for(int i = 0; i < count; i++) {
				messageList.add(message);
			}
		}
		return messageList;
	}
	
	static class MockProducer extends SyncProducer {

		public MockProducer(SyncProducerConfig config) {
			super(config);
		}
		
		
		@Override
		public void send(String topic, MessageList messageList) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
		@Override
		public void multiSend(List<ProduceRequest> requests) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
	}

}
