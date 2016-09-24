package com.leansoft.luxun.producer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.easymock.EasyMock;

import com.leansoft.luxun.broker.Broker;
import com.leansoft.luxun.common.exception.InvalidConfigException;
import com.leansoft.luxun.common.exception.InvalidPartitionException;
import com.leansoft.luxun.common.exception.UnavailableProducerException;
import com.leansoft.luxun.consumer.SimpleConsumer;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.producer.Producer;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.ProducerData;
import com.leansoft.luxun.producer.ProducerPool;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.producer.SyncProducerConfig;
import com.leansoft.luxun.producer.async.AsyncProducer;
import com.leansoft.luxun.serializer.Encoder;
import com.leansoft.luxun.serializer.StringEncoder;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

import junit.framework.TestCase;

/**
 * 
 * @author bulldog
 * 
 *
 */
public class ProducerTest extends TestCase {
	
	private String topic = "test-topic";
	private int brokerId1 = 0;
	private int brokerId2 = 1;
	private int port1 = 9092;
	private int port2 = 9093;
	private LuxunServer server1 = null;
	private LuxunServer server2 = null;
	private SyncProducer producer1 = null;
	private SyncProducer producer2 = null;
	private SimpleConsumer consumer1 = null;
	private SimpleConsumer consumer2 = null;
	private String brokerList = brokerId1 + ":127.0.0.1:" + port1 + "," + brokerId2 + ":127.0.0.1:" + port2;
	
	
	@Override
	public void setUp() throws Exception {
		// set up 2 brokers
		Properties props1 = TestUtils.createBrokerConfig(brokerId1, port1);
		ServerConfig config1 = new ServerConfig(props1);
		server1 = TestUtils.createServer(config1);
		
		Properties props2 = TestUtils.createBrokerConfig(brokerId2, port2);
		ServerConfig config2 = new ServerConfig(props2);
		server2 = TestUtils.createServer(config2);
		
	    Properties props = new Properties();
	    props.put("host", "127.0.0.1");
	    props.put("port", String.valueOf(port1));

	    producer1 = new SyncProducer(new SyncProducerConfig(props));
	    MessageList messageList = new MessageList();
	    messageList.add(new Message("test".getBytes()));
	    producer1.send("test-topic", messageList);
	    
	    props.put("port", String.valueOf(port2));
	    producer2 = new SyncProducer(new SyncProducerConfig(props));
	    messageList = new MessageList();
	    messageList.add(new Message("test".getBytes()));
	    producer2.send("test-topic", messageList);
	    
	    consumer1 = new SimpleConsumer("127.0.0.l", port1, 1000000);
	    consumer2 = new SimpleConsumer("127.0.0.1", port2, 1000000);
	    
	    Thread.sleep(500);
	}
	
	public void tearDown() throws Exception {
		server1.close();
		server2.close();
		
		Utils.deleteDirectory(new File(server1.config.getLogDir()));
		Utils.deleteDirectory(new File(server2.config.getLogDir()));
		Thread.sleep(500);
	}
	
	
	public void testSend() {
		Properties props = new Properties();
		props.put("partitioner.class", StaticPartitioner.class.getName());
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", brokerList);
		
		ProducerConfig config = new ProducerConfig(props);
		IPartitioner<String> partitioner = new StaticPartitioner();
		Encoder<String> serializer = new StringEncoder();
		
		// 2 sync producers
		ConcurrentHashMap<Integer, SyncProducer> syncProducers = new ConcurrentHashMap<Integer, SyncProducer>();
		SyncProducer syncProducer1 = EasyMock.createMock(SyncProducer.class);
		SyncProducer syncProducer2 = EasyMock.createMock(SyncProducer.class);
		// it should send to second broker
		MessageList messageList = new MessageList();
		messageList.add(new Message("test1".getBytes()));
		syncProducer2.send(topic, messageList);
		EasyMock.expectLastCall();
		syncProducer1.close();
		EasyMock.expectLastCall();
		syncProducer2.close();
		EasyMock.expectLastCall();
		EasyMock.replay(syncProducer1);
		EasyMock.replay(syncProducer2);
		
		syncProducers.put(brokerId1, syncProducer1);
		syncProducers.put(brokerId2, syncProducer2);
		
		ProducerPool<String> producerPool = new ProducerPool<String>(config, serializer, syncProducers, new ConcurrentHashMap<Integer, AsyncProducer<String>>(), null, null);
		Producer<String, String> producer = new Producer<String, String>(config, partitioner, producerPool, false, null);
		producer.send(new ProducerData<String, String>(topic, "test1", "test1"));
		producer.close();
		
		EasyMock.verify(syncProducer1);
		EasyMock.verify(syncProducer2);
	}
	
	
	public void testSendSingleMessage() {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", brokerList);
		
		ProducerConfig config = new ProducerConfig(props);
		IPartitioner<String> partitioner = new StaticPartitioner();
		Encoder<String> serializer = new StringEncoder();
		
		ConcurrentHashMap<Integer, SyncProducer> syncProducers = new ConcurrentHashMap<Integer, SyncProducer>();
		SyncProducer syncProducer1 = EasyMock.createMock(SyncProducer.class);
		MessageList messageList = new MessageList();
		messageList.add(new Message("test".getBytes()));
		syncProducer1.send(topic, messageList);
		EasyMock.expectLastCall();
		syncProducer1.close();
		EasyMock.expectLastCall();
		EasyMock.replay(syncProducer1);
		
		syncProducers.put(brokerId1, syncProducer1);
		
		ProducerPool<String> producerPool = new ProducerPool<String>(config, serializer, syncProducers, new ConcurrentHashMap<Integer, AsyncProducer<String>>(), null, null);
		Producer<String, String> producer = new Producer<String, String>(config, partitioner, producerPool, false, null);
		producer.send(new ProducerData<String, String>(topic, "test", "test"));
		producer.close();
		
		EasyMock.verify(syncProducer1);		
	}
	
	public void testInvalidPartition() {
		Properties props = new Properties();
		props.put("partitioner.class", NegativePartitioner.class.getName());
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", brokerList);
		
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> richProducer = new Producer<String, String>(config);
		
		try {
			richProducer.send(new ProducerData<String, String>(topic, "test", "test"));
			fail("Should fail with InvalidPartitionException");
		} catch (InvalidPartitionException e) {
		}
	}
	
	public void testSyncProducerPool() throws IOException {
		// 2 sync producers
		ConcurrentHashMap<Integer, SyncProducer> syncProducers = new ConcurrentHashMap<Integer, SyncProducer>();
		SyncProducer syncProducer1 = EasyMock.createMock(SyncProducer.class);
		SyncProducer syncProducer2 = EasyMock.createMock(SyncProducer.class);
		MessageList messageList = new MessageList();
		messageList.add(new Message("test1".getBytes()));
		syncProducer1.send(topic, messageList);
		EasyMock.expectLastCall();
		syncProducer1.close();
		EasyMock.expectLastCall();
		syncProducer2.close();
		EasyMock.expectLastCall();
		EasyMock.replay(syncProducer1);
		EasyMock.replay(syncProducer2);
		
		
		syncProducers.put(brokerId1, syncProducer1);
		syncProducers.put(brokerId2, syncProducer2);
		
		// default for producer.type is "sync"
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", brokerList);
		ProducerPool<String> producerPool = new ProducerPool<String>(new ProducerConfig(props), new StringEncoder(), syncProducers, new ConcurrentHashMap<Integer, AsyncProducer<String>>(), null, null);
		List<String> data = new ArrayList<String>();
		data.add("test1");
		producerPool.send(producerPool.buildProducerPoolData(topic, new Broker(0, "local", "127.0.0.1", 9092), data));
		producerPool.close();
		
		EasyMock.verify(syncProducer1);
		EasyMock.verify(syncProducer2);
	}	
	
	@SuppressWarnings("unchecked")
	public void testAsyncProducerPool() throws IOException {
		// 2 sync producers
		ConcurrentHashMap<Integer, AsyncProducer<String>> asyncProducers = new ConcurrentHashMap<Integer, AsyncProducer<String>>();
		AsyncProducer<String> asyncProducer1 = EasyMock.createMock(AsyncProducer.class);
		AsyncProducer<String> asyncProducer2 = EasyMock.createMock(AsyncProducer.class);
		asyncProducer1.send(topic, "test1");
		EasyMock.expectLastCall();
		asyncProducer1.close();
		EasyMock.expectLastCall();
		asyncProducer2.close();
		EasyMock.expectLastCall();
		EasyMock.replay(asyncProducer1);
		EasyMock.replay(asyncProducer2);
		
		asyncProducers.put(brokerId1, asyncProducer1);
		asyncProducers.put(brokerId2, asyncProducer2);
		
	    // change producer.type to "async"
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", brokerList);
	    props.put("producer.type", "async");
		ProducerPool<String> producerPool = new ProducerPool<String>(new ProducerConfig(props), new StringEncoder(), new ConcurrentHashMap<Integer, SyncProducer>(), asyncProducers , null, null);
		List<String> data = new ArrayList<String>();
		data.add("test1");
		producerPool.send(producerPool.buildProducerPoolData(topic, new Broker(0, "local", "127.0.0.1", 9092), data));
		producerPool.close();
		
		EasyMock.verify(asyncProducer1);
		EasyMock.verify(asyncProducer2);
	}
	
	public void testSyncUnavailableProducerException() throws IOException {
		ConcurrentHashMap<Integer, SyncProducer> syncProducers = new ConcurrentHashMap<Integer, SyncProducer>();
		SyncProducer syncProducer1 = EasyMock.createMock(SyncProducer.class);
		SyncProducer syncProducer2 = EasyMock.createMock(SyncProducer.class);
		syncProducer2.close();
	    EasyMock.expectLastCall();
	    EasyMock.replay(syncProducer1);
	    EasyMock.replay(syncProducer2);
	    
		syncProducers.put(brokerId2, syncProducer2);
		
		// default for producer.type is "sync"
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", brokerList);
		ProducerPool<String> producerPool = new ProducerPool<String>(new ProducerConfig(props), new StringEncoder(), syncProducers, new ConcurrentHashMap<Integer, AsyncProducer<String>>(), null, null);
		List<String> data = new ArrayList<String>();
		data.add("test1");
		try {
			producerPool.send(producerPool.buildProducerPoolData(topic, new Broker(brokerId1, brokerId1 + "", "127.0.0.1", port1), data));
			fail("Should fail with UnavailableProducerException");
		} catch (UnavailableProducerException upe) {
			// expected
		}
		producerPool.close();
		
		EasyMock.verify(syncProducer1);
		EasyMock.verify(syncProducer2);
	}

	@SuppressWarnings("unchecked")
	public void testAsyncUnavailableProducerException() throws IOException {
		ConcurrentHashMap<Integer, AsyncProducer<String>> asyncProducers = new ConcurrentHashMap<Integer, AsyncProducer<String>>();
		AsyncProducer<String> asyncProducer1 = EasyMock.createMock(AsyncProducer.class);
		AsyncProducer<String> asyncProducer2 = EasyMock.createMock(AsyncProducer.class);
		asyncProducer2.close();
	    EasyMock.expectLastCall();
	    EasyMock.replay(asyncProducer1);
	    EasyMock.replay(asyncProducer2);
	    
		asyncProducers.put(brokerId2, asyncProducer2);
		
		// default for producer.type is "sync"
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("broker.list", TestUtils.brokerList);
	    props.put("producer.type", "async");
		ProducerPool<String> producerPool = new ProducerPool<String>(new ProducerConfig(props), new StringEncoder(), new ConcurrentHashMap<Integer, SyncProducer>(), asyncProducers , null, null);
		List<String> data = new ArrayList<String>();
		data.add("test1");
		try {
			producerPool.send(producerPool.buildProducerPoolData(topic, new Broker(brokerId1, brokerId1 + "", "127.0.0.1", port1), data));
			fail("Should fail with UnavailableProducerException");
		} catch (UnavailableProducerException upe) {
			// expected
		}
		producerPool.close();
		
		EasyMock.verify(asyncProducer1);
		EasyMock.verify(asyncProducer2);
	}
	

	public void testMissingBrokerList() {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
	    props.put("producer.type", "async");
	    
	    try {
	    	@SuppressWarnings("unused")
			ProducerConfig config = new ProducerConfig(props);
	        fail("should fail with InvalidConfigException due to missing broker.list");
	    } catch (InvalidConfigException ice) {
	    	// expected
	    }
	}
	
	
	public void testConfigBrokerInfo() throws IOException {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
	    props.put("producer.type", "async");
	    props.put("broker.list", brokerId1 + ":" + "127.0.0.1" + ":" + port1);
		
		ProducerConfig config = new ProducerConfig(props);
		IPartitioner<String> partitioner = new StaticPartitioner();
		Encoder<String> serializer = new StringEncoder();
		
		// async producer
		ConcurrentHashMap<Integer, AsyncProducer<String>> asyncProducers = new ConcurrentHashMap<Integer, AsyncProducer<String>>();
		@SuppressWarnings("unchecked")
		AsyncProducer<String> asyncProducer1 = EasyMock.createMock(AsyncProducer.class);	
		asyncProducer1.send(topic, "test1");
		EasyMock.expectLastCall();
		asyncProducer1.close();
		EasyMock.expectLastCall();
		EasyMock.replay(asyncProducer1);
		
		asyncProducers.put(brokerId1, asyncProducer1);
		
		ProducerPool<String> producerPool = new ProducerPool<String>(config, serializer, new ConcurrentHashMap<Integer, SyncProducer>(), asyncProducers, null, null);
		Producer<String, String> producer = new Producer<String, String>(config, partitioner, producerPool, false, null);
		producer.send(new ProducerData<String, String>(topic, "test1"));
		producer.close();
		
	    EasyMock.verify(asyncProducer1);		
	}
	
	public void testSendToNewTopic() {
		Properties props = new Properties();
		props.put("serializer.class", StringEncoder.class.getName());
	    props.put("broker.list", this.brokerList);
		props.put("partitioner.class", StaticPartitioner.class.getName());
		
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> producer = new Producer<String, String>(config);
		try {
			producer.send(new ProducerData<String, String>("new-topic", "key", "test2"));
		    Thread.sleep(100);
			producer.send(new ProducerData<String, String>("new-topic", "key1", "test1"));
		    Thread.sleep(100);
			
		    // cross check if brokers got the messages
			List<MessageList> listOfMessageList1 = consumer1.consume("new-topic", 0, 10000);
			assertTrue(listOfMessageList1.size() == 1);
			assertEquals("test1", listOfMessageList1.get(0).get(0).toString());
			List<MessageList> listOfMessageList2 = consumer2.consume("new-topic", 0, 10000);
			assertTrue(listOfMessageList2.size() == 1);
			assertEquals("test2", listOfMessageList2.get(0).get(0).toString());
		} catch (Exception e) {
			fail("Not expected");
		}
		producer.close();
	}
	
	
	public static class NegativePartitioner implements IPartitioner<String> {

		@Override
		public int partition(String key, int numBrokers) {
			return -1;
		}
		
	}
	
	public static class StaticPartitioner implements IPartitioner<String> {

		@Override
		public int partition(String key, int numBrokers) {
			return (key.length() % numBrokers);
		}
		
	}
	
	public static class HashPartitioner implements IPartitioner<String> {

		@Override
		public int partition(String key, int numBrokers) {
			return (key.hashCode() % numBrokers);
		}
		
	}
}
