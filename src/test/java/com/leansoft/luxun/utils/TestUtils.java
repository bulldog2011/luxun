package com.leansoft.luxun.utils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.producer.SyncProducerConfig;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;

public class TestUtils {
	
	private static Random random = new Random();
	
	public static String brokerList = "0:127.0.0.1:9092";  
	
	
	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

	public static String randomString(int len ) 
	{
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( random.nextInt(AB.length()) ) );
	   return sb.toString();
	}
	
	public static void sleepQuietly(long duration) {
		try {
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			// ignore
		}
	}
	
	/**
	 * Create a producer for the given host and port
	 * 
	 */
	public static SyncProducer createProducer(String host, int port) {
		Properties props = new Properties();
	    props.put("host", host);
	    props.put("port", String.valueOf(port));
	    props.put("connect.timeout.ms", "100000");
	    props.put("reconnect.interval", "10000");
	    return new SyncProducer(new SyncProducerConfig(props));
	}
	
	/**
	 * Create a luxun server instance with appropriate test settings
	 * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
	 * @param config The configuration of the server
	 */
	public static LuxunServer createServer(ServerConfig serverConfig) {
		LuxunServer server = new LuxunServer(serverConfig);
		server.startup();
		return server;
	}
	
	public static File createTempDir() {
		String ioDir = System.getProperty("java.io.tmpdir");
		File f = new File(ioDir, "luxun-" + random.nextInt(1000000));
		f.mkdirs();
		f.deleteOnExit();
		return f;
	}
	
	/**
	 * Choose an available port
	 * @throws IOException 
	 */
	public static int choosePort() throws IOException {
		return choosePorts(1).get(0);
	}
	
	public static List<Integer> choosePorts(int count) throws IOException {
		List<Integer> ports = new ArrayList<Integer>();
		for(int i = 0; i < count; i++) {
			ServerSocket socket = new ServerSocket(0);
			ports.add(socket.getLocalPort());
			socket.close();
		}
		return ports;
	}
	
	/**
	 * Create a list of configs brokers
	 */
	public static List<Properties> createBrokerConfigs(int numConfigs) throws IOException {
		List<Properties> configs = new ArrayList<Properties>();
		List<Integer> ports = choosePorts(numConfigs);
		for(int i = 0; i < numConfigs; i++) {
			Properties props = createBrokerConfig(i, ports.get(i));
			configs.add(props);
		}
		return configs;
	}
	
	/**
	 * Create a test config for the given node id
	 */
	public static Properties createBrokerConfig(int nodeId, int port) {
		Properties props = new Properties();
	    props.put("brokerid", String.valueOf(nodeId));
	    props.put("port", String.valueOf(port));
	    props.put("log.dir", TestUtils.createTempDir().getAbsolutePath());
	    return props;
	}
	
	/**
	 * Create a test config for a consumer with default no consumer timeout
	 */
	public static Properties createConsumerProperties(String brokerList, String groupId, String consumerId) {
	    return createConsumerProperties(brokerList, groupId, consumerId, -1);
	}
	
	/**
	 * Create a test config for a consumer
	 */
	public static Properties createConsumerProperties(String brokerList, String groupId, String consumerId, long consumerTimeout) {
		Properties props = new Properties();
	    props.put("broker.list", brokerList);
	    props.put("groupid", groupId);
	    props.put("consumerid", consumerId);
	    props.put("consumer.timeout.ms", String.valueOf(consumerTimeout));

	    return props;
	}
	
	/**
	 * Wrap a message in buffer
	 * 
	 * @param msg The bytes of the message
	 */
	public static MessageList buildSigleMessageList(byte[] msg) {
		MessageList messageList = new MessageList();
		messageList.add(new Message(msg));
    	return messageList;
	}
}
