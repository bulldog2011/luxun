package com.leansoft.luxun.quickstart;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.leansoft.luxun.consumer.SimpleConsumer;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.producer.Producer;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.ProducerData;
import com.leansoft.luxun.serializer.StringEncoder;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

public class SimpleDemo {
	
	private int brokerId1 = 0;
	private int brokerId2 = 1;
	private int port1 = 9092;
	private int port2 = 9093;
	private LuxunServer server1 = null;
	private LuxunServer server2 = null;
	private String brokerList = brokerId1 + ":localhost:" + port1 + "," + brokerId2 + ":localhost:" + port2;
	private String broker1 = brokerId1 + ":localhost:" + port1;
	
	private SimpleConsumer simpleConsumer1 = null;
	private SimpleConsumer simpleConsumer2 = null;
	
	@Before
	public void setup() {
		// set up 2 brokers
		Properties props1 = new Properties();
	    props1.put("brokerid", String.valueOf(brokerId1));
	    props1.put("port", String.valueOf(port1));
	    props1.put("log.dir", TestUtils.createTempDir().getAbsolutePath());
		ServerConfig config1 = new ServerConfig(props1);
		server1 = new LuxunServer(config1);
		server1.startup();
		
		Properties props2 = new Properties();
	    props2.put("brokerid", String.valueOf(brokerId2));
	    props2.put("port", String.valueOf(port2));
	    props2.put("log.dir", TestUtils.createTempDir().getAbsolutePath());
		ServerConfig config2 = new ServerConfig(props2);
		server2 = new LuxunServer(config2);
		server2.startup();
		
		// set up two simple consumers
		// create a consumer 1 to connect to the Luxun server running on localhost, port 9092, socket timeout of 60 secs
		simpleConsumer1 = new SimpleConsumer("localhost", port1, 60000);
		// create a consumer 2 to connect to the Luxun server running on localhost, port 9093, socket timeout of 60 secs
		simpleConsumer2 = new SimpleConsumer("localhost", port2, 60000);
	}
	
	
	@Test
	public void sendSingleMessage() throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", broker1);
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		ProducerData<String, String> data = new ProducerData<String, String>("test-topic", "test-message");
		producer.send(data);
		
		producer.close(); // finish with the producer
		
		// consume by index
		List<MessageList> listOfMessageList = simpleConsumer1.consume("test-topic", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		MessageList messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		Message message = messageList.get(0);
		assertEquals("test-message", new String(message.getBytes()));
		
		// consume by fanout id
		String fanoutId = "demo";
		listOfMessageList = simpleConsumer1.consume("test-topic", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message", new String(message.getBytes()));
	}
	
	@Test
	public void sendMessageWithGZIPCompression() throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", broker1);
		props.put("compression.codec", "1");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		ProducerData<String, String> data = new ProducerData<String, String>("test-topic", "test-message");
		producer.send(data);
		
		producer.close(); // finish with the producer
		
		// consume by index
		List<MessageList> listOfMessageList = simpleConsumer1.consume("test-topic", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		MessageList messageList = listOfMessageList.get(0);
		assertEquals(CompressionCodec.GZIP, messageList.getCompressionCodec());
		assertTrue(messageList.size() == 1);
		Message message = messageList.get(0);
		assertEquals("test-message", new String(message.getBytes()));
		
		// consume by fanout id
		String fanoutId = "demo";
		listOfMessageList = simpleConsumer1.consume("test-topic", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertEquals(CompressionCodec.GZIP, messageList.getCompressionCodec());
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message", new String(message.getBytes()));
	}
	
	@Test
	public void sendMessageWithAsyncProducer() throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("producer.type", "async");
		props.put("broker.list", broker1);
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		ProducerData<String, String> data = new ProducerData<String, String>("test-topic", "test-message");
		producer.send(data);
		
		producer.close(); // finish with the producer
		
		// consume by index
		List<MessageList> listOfMessageList = simpleConsumer1.consume("test-topic", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		MessageList messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		Message message = messageList.get(0);
		assertEquals("test-message", new String(message.getBytes()));
		
		// consume by fanout id
		String fanoutId = "demo";
		listOfMessageList = simpleConsumer1.consume("test-topic", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message", new String(message.getBytes()));
	}
	
	@Test
	public void sendMultipleMessages() throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", broker1);
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		List<String> messages = new ArrayList<String>();
		messages.add("test-message1");
		messages.add("test-message2");
		messages.add("test-message3");
		ProducerData<String, String> data = new ProducerData<String, String>("test-topic", messages);
		producer.send(data);
		
		producer.close(); // finish with the producer
		
		// consume by index
		List<MessageList> listOfMessageList = simpleConsumer1.consume("test-topic", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		MessageList messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 3);
		for(int i = 1; i <= 3; i++) {
			Message message = messageList.get(i - 1);
			assertEquals("test-message" + i, new String(message.getBytes()));
		}
		
		// consume by fanout id
		String fanoutId = "demo";
		listOfMessageList = simpleConsumer1.consume("test-topic", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 3);
		for(int i = 1; i <= 3; i++) {
			Message message = messageList.get(i - 1);
			assertEquals("test-message" + i, new String(message.getBytes()));
		}
	}
	
	@Test
	public void sendMessagesToDifferentTopics() throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", broker1);
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		ProducerData<String, String> data1 = new ProducerData<String, String>("test-topic1", "test-message1");
		producer.send(data1);
		
		ProducerData<String, String> data2 = new ProducerData<String, String>("test-topic2", "test-message2");
		producer.send(data2);
		
		producer.close(); // finish with the producer
		
		// consume by index
		List<MessageList> listOfMessageList = simpleConsumer1.consume("test-topic1", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		MessageList messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		Message message = messageList.get(0);
		assertEquals("test-message1", new String(message.getBytes()));
		
		listOfMessageList = simpleConsumer1.consume("test-topic2", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message2", new String(message.getBytes()));
		
		// consume by fanoutId
		String fanoutId = "demo";
		listOfMessageList = simpleConsumer1.consume("test-topic1", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message1", new String(message.getBytes()));
		
		listOfMessageList = simpleConsumer1.consume("test-topic2", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message2", new String(message.getBytes()));
	}
	
	@Test
	public void sendMessagesWithCustomPartitioner() throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", brokerList);
		props.put("partitioner.class", CustomPartitioner.class.getName());
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		// will be sent to broker 1 since (the length of key % num of brokers) = 0
		ProducerData<String, String> data1 = new ProducerData<String, String>("test-topic1", "key1", "test-message1");
		producer.send(data1);
		
		// will be went to broker 2 since (the length of key % num of brokers) = 1
		ProducerData<String, String> data2 = new ProducerData<String, String>("test-topic2", "key11", "test-message2");
		producer.send(data2);
		
		producer.close(); // finish with the producer
		
		// consume by index
		List<MessageList> listOfMessageList = simpleConsumer1.consume("test-topic1", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		MessageList messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		Message message = messageList.get(0);
		assertEquals("test-message1", new String(message.getBytes()));
		
		listOfMessageList = simpleConsumer2.consume("test-topic2", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message2", new String(message.getBytes()));
		
		// consume by fanoutId
		String fanoutId = "demo";
		listOfMessageList = simpleConsumer1.consume("test-topic1", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message1", new String(message.getBytes()));
		
		listOfMessageList = simpleConsumer2.consume("test-topic2", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals("test-message2", new String(message.getBytes()));
	}
	
	@Test
	public void sendMessageWithCustomEncoder() throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", LogEventEncoder.class.getName());
		props.put("broker.list", broker1);
		
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, LogEvent> producer = new Producer<String, LogEvent>(config);
		
		LogEvent logEvent = new LogEvent();
		logEvent.createdTime = System.currentTimeMillis();
		logEvent.hostId = "127.0.0.1";
		logEvent.logLevel = LogLevel.INFO;
		logEvent.message = "a test log message";
		
		ProducerData<String, LogEvent> data = new ProducerData<String, LogEvent>("log-topic", logEvent);
		producer.send(data);
		
		producer.close(); // finish with the producer
		
		// consume by index
		LogEventDecoder decoder = new LogEventDecoder();
		List<MessageList> listOfMessageList = simpleConsumer1.consume("log-topic", 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		MessageList messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		Message message = messageList.get(0);
		assertEquals(logEvent, decoder.toEvent(message));
		
		// consume by fanout id
		String fanoutId = "demo";
		listOfMessageList = simpleConsumer1.consume("log-topic", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 1);
		messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		message = messageList.get(0);
		assertEquals(logEvent, decoder.toEvent(message));
	}
	
	@Test
	public void consumeMessageWithDifferentFanoutId() throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", broker1);
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		for(int i = 0; i < 100; i++) {
			ProducerData<String, String> data = new ProducerData<String, String>("test-topic", "test-message" + i);
			producer.send(data);
		}
		
		producer.close(); // finish with the producer
		
		// consume by different fanout id independently
		String fanoutId = "group-a";
		List<MessageList> listOfMessageList = simpleConsumer1.consume("test-topic", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 100);
		for(int i = 0; i < 100; i++) {
			MessageList messageList = listOfMessageList.get(i);
			assertTrue(messageList.size() == 1);
			Message message = messageList.get(0);
			assertEquals("test-message" + i, new String(message.getBytes()));
		}
		
		fanoutId = "group-b";
		listOfMessageList = simpleConsumer1.consume("test-topic", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 100);
		for(int i = 0; i < 100; i++) {
			MessageList messageList = listOfMessageList.get(i);
			assertTrue(messageList.size() == 1);
			Message message = messageList.get(0);
			assertEquals("test-message" + i, new String(message.getBytes()));
		}
		
		fanoutId = "group-c";
		listOfMessageList = simpleConsumer1.consume("test-topic", fanoutId, 10000);
		assertTrue(listOfMessageList.size() == 100);
		for(int i = 0; i < 100; i++) {
			MessageList messageList = listOfMessageList.get(i);
			assertTrue(messageList.size() == 1);
			Message message = messageList.get(0);
			assertEquals("test-message" + i, new String(message.getBytes()));
		}
	}
	
	@After
	public void cleanup() throws Exception {
		server1.close();
		server2.close();
		
		simpleConsumer1.close();
		simpleConsumer2.close();
		
		Utils.deleteDirectory(new File(server1.config.getLogDir()));
		Utils.deleteDirectory(new File(server2.config.getLogDir()));
		Thread.sleep(500);
	}

}
