package com.leansoft.luxun.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.leansoft.luxun.LuxunServerTestHarness;
import com.leansoft.luxun.broker.Broker;
import com.leansoft.luxun.consumer.ConsumerConfig;
import com.leansoft.luxun.consumer.FetchedDataChunk;
import com.leansoft.luxun.consumer.Fetcher;
import com.leansoft.luxun.consumer.StreamFactory;
import com.leansoft.luxun.consumer.TopicInfo;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;

public class FetcherTest extends LuxunServerTestHarness {
	
	int numNodes = 2;
	Map<Integer, List<MessageList>> messages;
	String topic = "topic";
	FetchedDataChunk shutdown = StreamFactory.SHUTDOWN_COMMAND;
	BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>();
	List<TopicInfo> topicInfos;
	Fetcher fetcher = null;
	
	
	@Override
	public void setUp() throws Exception {
		configs = new ArrayList<ServerConfig>();
		List<Properties> propsList = TestUtils.createBrokerConfigs(numNodes);
		for(Properties props : propsList) {
			configs.add(new ServerConfig(props));
		}
		
		super.setUp();
		
		messages = new HashMap<Integer, List<MessageList>>();
		
		topicInfos = new ArrayList<TopicInfo>();
		for(ServerConfig config : configs) {
			TopicInfo ptInfo = new TopicInfo(
					topic,
					new Broker(config.getBrokerId(), String.valueOf(config.getBrokerId()), "localhost", config.getPort()),
					queue
			);
			topicInfos.add(ptInfo);
		}
		
		fetcher = new Fetcher(new ConsumerConfig(TestUtils.createConsumerProperties("dummy.borker.list", "dummy.groupid", "")));
		fetcher.stopConnectionsToAllBrokers();
		fetcher.startConnections(topicInfos);
	}
	
	@Override
	public void tearDown() throws Exception {
		fetcher.stopConnectionsToAllBrokers();
		super.tearDown();
	}
	
	public void testFetcher() throws InterruptedException {
		int perNode = 2;
		int count = sendMessages(perNode);
		fetch(count);
		Thread.sleep(1000);
		assertQueueEmpty();
		count = sendMessages(perNode);
		fetch(count);
		Thread.sleep(1000);
		assertQueueEmpty();
	}
	
	private void assertQueueEmpty() {
		assertEquals(0, queue.size());
	}
	
	public int sendMessages(int messagesPerNode) {
		int count = 0;
		for(ServerConfig conf : configs) {
			SyncProducer producer = TestUtils.createProducer("localhost", conf.getPort());
			MessageList messageList = new MessageList();
	    	for(int i = 0; i < messagesPerNode; i++) {
	    		byte[] message = String.valueOf(conf.getBrokerId() * 5  + i).getBytes();
	    		messageList.add(new Message(message));
	    	}
	    	if (!messages.containsKey(conf.getBrokerId())) {
	    		messages.put(conf.getBrokerId(), new ArrayList<MessageList>());
	    	}
	    	messages.get(conf.getBrokerId()).add(messageList);
	    	producer.send(topic, messageList);
	    	producer.close();
	    	count += messageList.size();
		}
		return count;
	}
	
	public void fetch(int expected) throws InterruptedException {
		int count = 0;
		while(true) {
			FetchedDataChunk chunk = queue.poll(2L, TimeUnit.SECONDS);
			assertNotNull("Timed out waiting for data chunk " + (count + 1), chunk);
			count += chunk.messageList.size();
			if (count == expected) return;
		}
	}

}
