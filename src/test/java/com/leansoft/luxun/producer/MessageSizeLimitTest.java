package com.leansoft.luxun.producer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.leansoft.luxun.common.exception.MessageSizeTooLargeException;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.producer.SyncProducerConfig;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;

public class MessageSizeLimitTest {

	private LuxunServer server;
	private int port;
	
	@Before
	public void setup() throws IOException {
		port = TestUtils.choosePort();
		Properties props = TestUtils.createBrokerConfig(0, port);
		ServerConfig config = new ServerConfig(props);
		server = TestUtils.createServer(config);
	}
	
	@After
	public void tearDown() throws IOException {
		server.close();
	}
	
	@Test
	public void testSingleMessageSizeTooLarge() {
	    Properties props = new Properties();
	    props.put("host", "localhost");
	    props.put("port", String.valueOf(port));
	    props.put("connect.timeout.ms", "300");
	    props.put("reconnect.interval", "500");
	    props.put("max.message.size", "100");
	    
	    SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
	    byte[] bytes = new byte[101];
	    try {
	    	MessageList messageList = new MessageList();
	    	messageList.add(new Message(bytes));
	    	producer.send("test", messageList);
	    	fail("should throw MessageSizeTooLargeException");
	    } catch (MessageSizeTooLargeException e) {
	    	// expected
	    }
	    
	    producer.close();
	}
	
	@Test
	public void testCompressedMessageSizeTooLarge() {
	    Properties props = new Properties();
	    props.put("host", "localhost");
	    props.put("port", String.valueOf(port));
	    props.put("connect.timeout.ms", "300");
	    props.put("reconnect.interval", "500");
	    props.put("max.message.size", "100");
	    
	    SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
    	MessageList messageList = new MessageList(CompressionCodec.GZIP);
    	for(int i = 0; i < 19; i++) {
    	    byte[] bytes = new byte[20];
    	    for(int j = 0; j < 20; j++) {
    	    	bytes[i] = (byte)i;
    	    }
    		messageList.add(new Message(bytes));
    	}
        /** After compression, the compressed message has size 106 **/
	    try {
	    	producer.send("test", messageList);
	    	fail("should throw MessageSizeTooLargeException");
	    } catch (MessageSizeTooLargeException e) {
	    	// expected
	    }
	    
	    producer.close();
	}
	
}
