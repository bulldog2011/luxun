package com.leansoft.luxun.server;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.leansoft.luxun.consumer.SimpleConsumer;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.producer.SyncProducerConfig;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

public class ServerShutdownTest {
	
	int port;
	
	@Before
	public void setUp() throws IOException {
		port = TestUtils.choosePort();
	}
	
	@Test
	public void testCleanShutdown() throws Exception {
		Properties props = TestUtils.createBrokerConfig(0, port);
		ServerConfig config = new ServerConfig(props);
		
		String host = "localhost";
		String topic = "test";
		
		MessageList sent1 = new MessageList();
		sent1.add(new Message("hello".getBytes()));
		sent1.add(new Message("there".getBytes()));
		
		MessageList sent2 = new MessageList();
		sent2.add(new Message("more".getBytes()));
		sent2.add(new Message("messages".getBytes()));
		
		{
			SyncProducer producer = new SyncProducer(this.getProducerConfig(host, port, 100000, 10000));
			SimpleConsumer consumer = new SimpleConsumer(host, port, 1000000);
			
			LuxunServer server = new LuxunServer(config);
			server.startup();
			
			// send some messages
			producer.send(topic, sent1);
			
			Thread.sleep(200);
			// do a clean shutdown
			server.close();
			
			File cleanShutDownFile = new File(new File(config.getLogDir()), server.CLEAN_SHUTDOWN_FILE);
			assertTrue(cleanShutDownFile.exists());
			producer.close();
		}
		
		{
			SyncProducer producer = new SyncProducer(this.getProducerConfig(host, port, 100000, 10000));
			SimpleConsumer consumer = new SimpleConsumer(host, port, 1000000);
			
			LuxunServer server = new LuxunServer(config);
			server.startup();
			
			List<MessageList> listOfMessageList = consumer.consume(topic, 0, 10000);
			assertTrue(listOfMessageList.size() == 1);
			assertEquals(sent1, listOfMessageList.get(0));
			
			// send some more messages
			producer.send(topic, sent2);
			
			Thread.sleep(200);
			listOfMessageList = consumer.consume(topic, 0, 10000);
			assertTrue(listOfMessageList.size() == 2);
			assertEquals(sent1, listOfMessageList.get(0));
			assertEquals(sent2, listOfMessageList.get(1));
			
			server.close();
			Utils.deleteDirectory(new File(server.config.getLogDir()));
			producer.close();
		}
	}
	
	private SyncProducerConfig getProducerConfig(String host, int port, int connectTimeout, int reconnectInterval) {
		Properties props = new Properties();
	    props.put("host", host);
	    props.put("port", String.valueOf(port));
	    props.put("connect.timeout.ms", String.valueOf(connectTimeout));
	    props.put("reconnect.interval", String.valueOf(reconnectInterval));
	    return new SyncProducerConfig(props);
	}

}
