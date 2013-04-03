package com.leansoft.luxun.quickstart;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.leansoft.luxun.consumer.SimpleConsumer;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.producer.Producer;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.ProducerData;
import com.leansoft.luxun.serializer.StringEncoder;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

public class ProducerDemo {
	
	private String topic = "test-topic";
	private int brokerId1 = 0;
	private int brokerId2 = 1;
	private int port1 = 9092;
	private int port2 = 9093;
	private LuxunServer server1 = null;
	private LuxunServer server2 = null;
	//private String brokerList = brokerId1 + ":localhost:" + port1 + "," + brokerId2 + ":localhost:" + port2;
	private String broker1 = brokerId1 + ":localhost:" + port1;
	
	private SimpleConsumer simpleConsumer1 = null;
	private SimpleConsumer simpleConsumer2 = null;
	
	@Before
	public void setup() {
		// set up 2 brokers
		Properties props1 = TestUtils.createBrokerConfig(brokerId1, port1);
		ServerConfig config1 = new ServerConfig(props1);
		server1 = TestUtils.createServer(config1);
		
		Properties props2 = TestUtils.createBrokerConfig(brokerId2, port2);
		ServerConfig config2 = new ServerConfig(props2);
		server2 = TestUtils.createServer(config2);
		
		// and two simple consumers
		simpleConsumer1 = new SimpleConsumer("localhost", port1, 60000);
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
		List<MessageList> listOfMessageList = simpleConsumer1.consume(topic, 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		MessageList messageList = listOfMessageList.get(0);
		assertTrue(messageList.size() == 1);
		Message message = messageList.get(0);
		assertEquals("test-message", new String(message.getBytes()));
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
