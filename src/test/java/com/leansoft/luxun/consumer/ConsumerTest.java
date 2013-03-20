package com.leansoft.luxun.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.leansoft.luxun.LuxunServerTestHarness;
import com.leansoft.luxun.common.exception.ConsumerTimeoutException;
import com.leansoft.luxun.consumer.ConsumerConfig;
import com.leansoft.luxun.consumer.IStreamFactory;
import com.leansoft.luxun.consumer.MessageStream;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.serializer.StringDecoder;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.ImmutableMap;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

public class ConsumerTest extends LuxunServerTestHarness {
	
	static final Logger logger = Logger.getLogger(ConsumerTest.class);
	
	int numNodes = 2;
	String topic = "topic1";
	String group = "group1";
	String consumer0 = "consumer0";
	String consumer1 = "consumer1";
	String consumer2 = "consumer2";
	String consumer3 = "consumer3";
	int nMessages = 2;
	
	String brokerList;
	
	
	@Override
	public void setUp() throws Exception {
		configs = new ArrayList<ServerConfig>();
		List<Properties> propsList = TestUtils.createBrokerConfigs(numNodes);
		for(Properties props : propsList) {
			configs.add(new ServerConfig(props));
			if (brokerList == null) {
				brokerList = "";
			} else {
				brokerList += ",";
			}
			brokerList += props.getProperty("brokerid") + ":127.0.0.1:" + props.getProperty("port");
		}
		
		super.setUp();
	}
	
	@Test
	public void testBasic() throws Exception {
		Properties props = TestUtils.createConsumerProperties(brokerList, group, consumer0, 200);
		ConsumerConfig consumerConfig0 = new ConsumerConfig(props);
		
		// test consumer timeout logic
		@SuppressWarnings("resource")
		IStreamFactory streamFactory0 = new StreamFactory(consumerConfig0);
		Map<String, List<MessageStream<Message>>> topicMessageStreams0 = 
				streamFactory0.createMessageStreams(ImmutableMap.of(topic, numNodes));
		
		// no messages to consume, we should hit timeout;
		// also the iterator should support re-entrant, so loop it twice
		for(int i = 0; i < 2; i++) {
			try {
				this.getMessages(nMessages, topicMessageStreams0);
				fail("should get an exception");
			} catch (ConsumerTimeoutException e) {
				// this is ok
			} catch (Exception e) {
				throw e;
			}
		}
		
		streamFactory0.close();
		
		// send some messages to each broker
		List<Message> sentMessages1 = sendMessages(nMessages, "batch1");
		// create a consumer
		ConsumerConfig consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(brokerList, group, consumer1));
		IStreamFactory streamFactory1 = new StreamFactory(consumerConfig1);
		Map<String, List<MessageStream<Message>>> topicMessageStreams1 = 
				streamFactory1.createMessageStreams(ImmutableMap.of(topic, numNodes));
		List<Message> receivedMessages1 = getMessages(nMessages, topicMessageStreams1);
		assertTrue(isMessageListEqual(sentMessages1, receivedMessages1));
		
		streamFactory1.close();
	}
	
	
	@Test
	public void testCompression() throws Exception {		
		// send some messages to each broker
		List<Message> sentMessages1 = sendMessages(nMessages, "batch1", CompressionCodec.GZIP);
		// create a consumer
		ConsumerConfig consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(brokerList, group, consumer1));
		IStreamFactory streamFactory1 = new StreamFactory(consumerConfig1);
		Map<String, List<MessageStream<Message>>> topicMessageStreams1 = 
				streamFactory1.createMessageStreams(ImmutableMap.of(topic, numNodes));
		List<Message> receivedMessages1 = getMessages(nMessages, topicMessageStreams1);
		assertTrue(isMessageListEqual(sentMessages1, receivedMessages1));
		
		streamFactory1.close();
	}
	
	@Test
	public void testConsumerDecoder() throws IOException {
		List<Message> sentMessages = sendMessages(nMessages, "batch1");
		Properties props = TestUtils.createConsumerProperties(brokerList, group, consumer1);
		ConsumerConfig consumerConfig = new ConsumerConfig(props);		
		
		// create a consumer
		IStreamFactory streamFactory = new StreamFactory(consumerConfig);
		Map<String, List<MessageStream<String>>> topicMessageStreams = 
				streamFactory.createMessageStreams(ImmutableMap.of(topic, numNodes), new StringDecoder());
		
		List<Message> receivedMessages = new ArrayList<Message>();
		for(String topic : topicMessageStreams.keySet()) {
			List<MessageStream<String>> messageStreams = topicMessageStreams.get(topic);
			for(MessageStream<String> messageStream : messageStreams) {
				Iterator<String> iterator = messageStream.iterator();
				for(int i = 0; i < nMessages; i++) {
					assertTrue(iterator.hasNext());
					String message = iterator.next();
					receivedMessages.add(new Message(message.getBytes()));
					logger.debug("received message : " + message);
				}
			}
		}
		
		assertTrue(isMessageListEqual(sentMessages, receivedMessages));
		streamFactory.close();
	}
	
	private List<Message> sendMessages(ServerConfig conf, int messagesPerNode, String header, CompressionCodec compression) {
		List<Message> messages = new ArrayList<Message>();
		SyncProducer producer = TestUtils.createProducer("127.0.0.1", conf.getPort());
		MessageList messageList = new MessageList();
    	for(int i = 0; i < messagesPerNode; i++) {
    		Message message = new Message((header + conf.getBrokerId()  + "-" + i).getBytes());
    		messageList.add(message);
    		messages.add(message);
    	}
    	producer.send(topic, messageList);
		producer.close();
		return messages;
	}
	
	private boolean isMessageListEqual(List<Message> sourceMessageList, List<Message> targetMessageList) {
		if (sourceMessageList.size() != targetMessageList.size()) {
			return false;
		}
		Set<Message> messageSet = new HashSet<Message>();
		for(Message msg : sourceMessageList) {
			messageSet.add(msg);
		}
		for(Message msg : targetMessageList) {
			messageSet.remove(msg);
		}
		
		return messageSet.size() == 0;
	}
	
	private List<Message> sendMessages(int messagesPerNode, String header) {
		return this.sendMessages(messagesPerNode, header, CompressionCodec.NO_COMPRESSION);
	}
	
	private List<Message> sendMessages(int messagesPerNode, String header, CompressionCodec compression) {
		List<Message> messages = new ArrayList<Message>();
		for(ServerConfig conf : configs) {
			messages.addAll(this.sendMessages(conf, messagesPerNode, header, compression));
		}
		return messages;
	}
	
	private List<Message> getMessages(int nMessagesPerThread, Map<String, List<MessageStream<Message>>> topicMessageStreams) {
		List<Message> messages = new ArrayList<Message>();
		for(String topic : topicMessageStreams.keySet()) {
			List<MessageStream<Message>> messageStreams = topicMessageStreams.get(topic);
			for(MessageStream<Message> messageStream : messageStreams) {
				Iterator<Message> iterator = messageStream.iterator();
				for(int i = 0; i < nMessagesPerThread; i++) {
					assertTrue(iterator.hasNext());
					Message message = iterator.next();
					messages.add(message);
					logger.debug("received message : " + Utils.toString(message.getBufferDuplicate(), "UTF-8"));
				}
			}
		}
		return messages;
	}

}
