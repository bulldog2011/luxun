package com.leansoft.luxun.integration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.thrift.TException;

import com.leansoft.luxun.api.generated.ConsumeRequest;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.StringProducer;
import com.leansoft.luxun.producer.StringProducerData;
import com.leansoft.luxun.serializer.Decoder;
import com.leansoft.luxun.serializer.StringDecoder;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

/**
 * End to end tests of the primitive apis against a local server
 * 
 * @author bulldog
 *
 */
public class PrimitiveApiTest extends ProducerConsumerTestHarness {
	
	Properties props;
	ServerConfig config;
	List<ServerConfig> configs;
	List<LuxunServer> servers;
	

	@Override
	public void setUp() throws Exception {
		port = TestUtils.choosePort();
		props = TestUtils.createBrokerConfig(0, port);
		config = new ServerConfig(props);
		configs = new ArrayList<ServerConfig>();
		configs.add(config);
		servers = new ArrayList<LuxunServer>();
		servers.add(TestUtils.createServer(config));
		super.setUp();
	}
	
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		
		for(LuxunServer server : servers) {
			server.close();
		}
		
		for(ServerConfig config : configs) {
			Utils.deleteDirectory(new File(config.getLogDir()));
		}
	}
	
	public void testDefaultEncoderProduceAndFetchByIndex() throws TException {
		String topic = "test-topic";
		Properties props = new Properties();
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("broker.list", "0:localhost:" + port);
	    ProducerConfig config = new ProducerConfig(props);
	    
	    StringProducer stringProducer1 = new StringProducer(config);
	    stringProducer1.send(new StringProducerData(topic, "test-message"));
	    
	    List<MessageList> listOfMessageList = consumer.consume(topic, 0, 10000);
	    assertTrue(listOfMessageList.size() == 1);
	    assertTrue(listOfMessageList.get(0).iterator().hasNext());
	    
	    Message message = listOfMessageList.get(0).get(0);
	    Decoder<String> stringDecoder = new StringDecoder();
	    String fetchedStringMessage = stringDecoder.toEvent(message);
	    assertEquals("test-message", fetchedStringMessage); 
	}
	
	public void testDefaultEncoderProduceAndFetchByFanoutId() throws TException {
		String topic = "test-topic";
		Properties props = new Properties();
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("broker.list", "0:localhost:" + port);
	    ProducerConfig config = new ProducerConfig(props);
	    
	    StringProducer stringProducer1 = new StringProducer(config);
	    stringProducer1.send(new StringProducerData(topic, "test-message"));
	    
	    List<MessageList> listOfMessageList = consumer.consume(topic, "fan0", 10000);
	    assertTrue(listOfMessageList.size() == 1);
	    assertTrue(listOfMessageList.get(0).iterator().hasNext());
	    
	    Message message = listOfMessageList.get(0).get(0);
	    Decoder<String> stringDecoder = new StringDecoder();
	    String fetchedStringMessage = stringDecoder.toEvent(message);
	    assertEquals("test-message", fetchedStringMessage); 
	}
	
	public void testEmptyMessageProduceAndFetchByIndex() throws TException {
		String topic = "test-topic";
	    MessageList messageList = new MessageList();
	    producer.send(topic, messageList);
	    
	    
	    List<MessageList> listOfMessageList = consumer.consume(topic, 0, 10000);
	    assertTrue(listOfMessageList.size() == 1);
	    assertTrue(listOfMessageList.get(0).isEmpty());
	    assertTrue(listOfMessageList.get(0).getCompressionCodec() == CompressionCodec.NO_COMPRESSION);
	}
	
	public void testEmptyMessageProduceAndFetchByFanoutId() throws TException {
		String topic = "test-topic";
	    MessageList messageList = new MessageList();
	    producer.send(topic, messageList);
	    
	    
	    List<MessageList> listOfMessageList = consumer.consume(topic, "fan001", 10000);
	    assertTrue(listOfMessageList.size() == 1);
	    assertTrue(listOfMessageList.get(0).isEmpty());
	    assertTrue(listOfMessageList.get(0).getCompressionCodec() == CompressionCodec.NO_COMPRESSION);
	}
	
	
	
	public void testDefaultEncoderProduceAndFetchByIndexWithCompression() throws TException {
		String topic = "test-topic";
		Properties props = new Properties();
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("broker.list", "0:localhost:" + port);
	    props.put("compression.codec", "1"); // GZIP
	    ProducerConfig config = new ProducerConfig(props);
	    
	    StringProducer stringProducer1 = new StringProducer(config);
	    stringProducer1.send(new StringProducerData(topic, "test-message"));
	    
	    List<MessageList> listOfMessageList = consumer.consume(topic, 0, 10000);
	    assertTrue(listOfMessageList.size() == 1);
	    assertTrue(listOfMessageList.get(0).iterator().hasNext());
	    
	    Message message = listOfMessageList.get(0).get(0);
	    Decoder<String> stringDecoder = new StringDecoder();
	    String fetchedStringMessage = stringDecoder.toEvent(message);
	    assertEquals("test-message", fetchedStringMessage);
	    
	}
	
	public void testDefaultEncoderProduceAndFetchByFanoutIdWithCompression() throws TException {
		String topic = "test-topic";
		Properties props = new Properties();
	    props.put("serializer.class", "com.leansoft.luxun.serializer.StringEncoder");
	    props.put("broker.list", "0:localhost:" + port);
	    props.put("compression.codec", "1"); // GZIP
	    ProducerConfig config = new ProducerConfig(props);
	    
	    StringProducer stringProducer1 = new StringProducer(config);
	    stringProducer1.send(new StringProducerData(topic, "test-message"));
	    
	    List<MessageList> listOfMessageList = consumer.consume(topic, "fan002", 10000);
	    assertTrue(listOfMessageList.size() == 1);
	    assertTrue(listOfMessageList.get(0).iterator().hasNext());
	    
	    Message message = listOfMessageList.get(0).get(0);
	    Decoder<String> stringDecoder = new StringDecoder();
	    String fetchedStringMessage = stringDecoder.toEvent(message);
	    assertEquals("test-message", fetchedStringMessage);
	    
	}
	
	public void testEmptyMessageProduceAndFetchByIndexWithCompression() throws TException {
		String topic = "test-topic";
	    MessageList messageList = new MessageList(CompressionCodec.GZIP);
	    producer.send(topic, messageList);
	    
	    
	    List<MessageList> listOfMessageList = consumer.consume(topic, 0, 10000);
	    assertTrue(listOfMessageList.size() == 1);
	    assertTrue(listOfMessageList.get(0).isEmpty());
	    assertTrue(listOfMessageList.get(0).getCompressionCodec() == CompressionCodec.GZIP);
	}
	
	public void testEmptyMessageProduceAndFetchByFanoutIdWithCompression() throws TException {
		String topic = "test-topic";
	    MessageList messageList = new MessageList(CompressionCodec.GZIP);
	    producer.send(topic, messageList);
	    
	    
	    List<MessageList> listOfMessageList = consumer.consume(topic, "fan9", 10000);
	    assertTrue(listOfMessageList.size() == 1);
	    assertTrue(listOfMessageList.get(0).isEmpty());
	    assertTrue(listOfMessageList.get(0).getCompressionCodec() == CompressionCodec.GZIP);
	}

}
