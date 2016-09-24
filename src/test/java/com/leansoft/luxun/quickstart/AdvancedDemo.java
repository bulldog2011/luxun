package com.leansoft.luxun.quickstart;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.leansoft.luxun.common.exception.ConsumerTimeoutException;
import com.leansoft.luxun.consumer.ConsumerConfig;
import com.leansoft.luxun.consumer.IStreamFactory;
import com.leansoft.luxun.consumer.MessageStream;
import com.leansoft.luxun.consumer.StreamFactory;
import com.leansoft.luxun.producer.IStringProducer;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.ProducerData;
import com.leansoft.luxun.serializer.StringDecoder;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.ImmutableMap;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

public class AdvancedDemo {
	
	// enable or disable async producing
	private static final boolean async = true;
	
	// 2 topics
	private static final String TOPIC_STAR = "star";
	private static final String TOPIC_MOON = "moon";
	
	// number of items to produce per topic
	private static int topicStarTotalItemCount = 100000;
	private static int topicMoonTotalItemCount = 200000;
	
	// number of producer threads
	private static int topicStarProducerNum = 2;
	private static int topicMoonProducerNum = 4;
	private static final AtomicInteger topicStarProducedItemCount = new AtomicInteger(0);
	private static final AtomicInteger topicMoonProducedItemCount = new AtomicInteger(0);
	// remember items produced for later validation
	private static final List<String> topicStarProducedItems = Collections.synchronizedList(new ArrayList<String>());
	private static final List<String> topicMoonProducedItems = Collections.synchronizedList(new ArrayList<String>());
	
	// number of consumer threads per topic/group
	private static int topicStarGroupAConsumerNum = 2;
	private static int topicStarGroupBConsumerNum = 4;
	private static int topicMoonGroupAConsumerNum = 1;
	private static int topicMoonGroupBConsumerNum = 2;
	// remember items consumed for later validation
	private static final List<String> topicStarGroupAConsumedItems = Collections.synchronizedList(new ArrayList<String>());
	private static final List<String> topicStarGroupBConsumedItems = Collections.synchronizedList(new ArrayList<String>());
	private static final List<String> topicMoonGroupAConsumedItems = Collections.synchronizedList(new ArrayList<String>());
	private static final List<String> topicMoonGroupBConsumedItems = Collections.synchronizedList(new ArrayList<String>());
	
	private int brokerId1 = 0;
	private int brokerId2 = 1;
	private int port1 = 9092;
	private int port2 = 9093;
	private LuxunServer server1 = null;
	private LuxunServer server2 = null;
	private String brokerList = brokerId1 + ":127.0.0.1:" + port1 + "," + brokerId2 + ":127.0.0.1:" + port2;
	
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
	}
	
	
	// Thread produces to topic star
    static class StarProducer extends Thread {
		private final CountDownLatch latch;
		private final IStringProducer stringProducer;
		
		public StarProducer(CountDownLatch latch, IStringProducer stringProducer) {
			this.latch = latch;
			this.stringProducer = stringProducer;
		}
		
		public void run() {
			latch.countDown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// ignore
			}
			
			while(true) {
				int count = topicStarProducedItemCount.incrementAndGet();
				if(count > topicStarTotalItemCount) break;
				String item = TOPIC_STAR + '-' + count;
				topicStarProducedItems.add(item);
				stringProducer.send(new ProducerData<String, String>(TOPIC_STAR, item));
			}
		}	
    }
    
    // Thread produces to topic moon
    static class MoonProducer extends Thread {
		private final CountDownLatch latch;
		private final IStringProducer stringProducer;
		
		public MoonProducer(CountDownLatch latch, IStringProducer stringProducer) {
			this.latch = latch;
			this.stringProducer = stringProducer;
		}
		
		public void run() {
			latch.countDown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// ignore
			}
			
			while(true) {
				int count = topicMoonProducedItemCount.incrementAndGet();
				if(count > topicMoonTotalItemCount) break;
				String item = TOPIC_MOON + '-' + count;
				topicMoonProducedItems.add(item);
				stringProducer.send(new ProducerData<String, String>(TOPIC_MOON, item));
			}
		}	
    }
    
	static class Consumer extends Thread {
		private final CountDownLatch latch;
		private final MessageStream<String> stream;
		List<String> consumedItems;
		
		public Consumer(CountDownLatch latch,
				MessageStream<String> stream, List<String> consumedItems) {
			this.latch = latch;
			this.stream = stream;
			this.consumedItems = consumedItems;
		}
		
		public void run() {
				latch.countDown();
				try {
					latch.await();
				} catch (InterruptedException e) {
					// ignore
				}
				
				try {
					for(String item : stream) {
						this.consumedItems.add(item);
					}
				} catch (ConsumerTimeoutException cte) {
					// expected
				}
		}
	}
	
	@Test
	public void demo() throws IOException {
		//prepare
		CountDownLatch allLatch = new CountDownLatch(topicStarProducerNum  + topicMoonProducerNum  + 
				topicStarGroupAConsumerNum + topicStarGroupBConsumerNum + topicMoonGroupAConsumerNum + topicMoonGroupBConsumerNum);
		IStringProducer[] topicStarProducers = new IStringProducer[topicStarProducerNum];
		IStringProducer[] topicMoonProducers = new IStringProducer[topicMoonProducerNum];
		
		List<Thread> producers = new ArrayList<Thread>();
		List<Thread> consumers = new ArrayList<Thread>();
		
		//start topic star producers
		for(int i = 0; i < topicStarProducerNum; i++) {
			Properties props = new Properties();
			props.put("broker.list", this.brokerList);
			if (async) {
			    props.put("producer.type", "async");
			    props.put("queue.enqueueTimeout.ms", "-1");
			    props.put("queue.time", "500");
			}
			ProducerConfig config = new ProducerConfig(props);
			IStringProducer stringProducer = new com.leansoft.luxun.producer.StringProducer(config);
			topicStarProducers[i] = stringProducer;
			StarProducer p = new StarProducer(allLatch, stringProducer);
			producers.add(p);
			p.start();
		}
		
		//start topic moon producers
		for(int i = 0; i < topicMoonProducerNum; i++) {
			Properties props = new Properties();
			props.put("broker.list", this.brokerList);
			if (async) {
			    props.put("producer.type", "async");
			    props.put("queue.enqueueTimeout.ms", "-1");
			    props.put("queue.time", "500");
			}
			ProducerConfig config = new ProducerConfig(props);
			IStringProducer stringProducer = new com.leansoft.luxun.producer.StringProducer(config);
			topicMoonProducers[i] = stringProducer;
			MoonProducer p = new MoonProducer(allLatch, stringProducer);
			producers.add(p);
			p.start();
		}
		
		
		// start group A consumers
		Properties props = new Properties();
	    props.put("broker.list", brokerList);
	    props.put("groupid", "group-a");
	    props.put("consumerid", "consume-1");
	    props.put("consumer.timeout.ms", String.valueOf(5000)); // let timeout if no message consumed in 5 seconds.
	    
		ConsumerConfig groupAConsumerConfig = new ConsumerConfig(props);
		
		IStreamFactory groupAStreamFactory = new StreamFactory(groupAConsumerConfig);
		Map<String, List<MessageStream<String>>> groupAStreams = groupAStreamFactory.createMessageStreams(
				ImmutableMap.of(TOPIC_STAR, topicStarGroupAConsumerNum, TOPIC_MOON, topicMoonGroupAConsumerNum), new StringDecoder());
		
		
		// delegate stream consuming to consumer threads
		List<MessageStream<String>> topicFishGroupAStreams = groupAStreams.get(TOPIC_STAR);
		for(MessageStream<String> stream : topicFishGroupAStreams) {
			Consumer c = new Consumer(allLatch, stream, topicStarGroupAConsumedItems);
			consumers.add(c);
			c.start();
		}
		List<MessageStream<String>> topicMoonGroupAStreams = groupAStreams.get(TOPIC_MOON);
		for(MessageStream<String> stream : topicMoonGroupAStreams) {
			Consumer c = new Consumer(allLatch, stream, topicMoonGroupAConsumedItems);
			consumers.add(c);
			c.start();
		}
		
		// start group B consumers
	    props.put("groupid", "group-b");
	    props.put("consumerid", "consume-2");
	    
		ConsumerConfig groupBConsumerConfig = new ConsumerConfig(props);
		
		IStreamFactory groupBStreamFactory = new StreamFactory(groupBConsumerConfig);
		Map<String, List<MessageStream<String>>> groupBStreams = groupBStreamFactory.createMessageStreams(
				ImmutableMap.of(TOPIC_STAR, topicStarGroupBConsumerNum, TOPIC_MOON, topicMoonGroupBConsumerNum), new StringDecoder());
		
		// delegate stream consuming to consumer threads
		List<MessageStream<String>> topicStartGroupBStreams = groupBStreams.get(TOPIC_STAR);
		for(MessageStream<String> stream : topicStartGroupBStreams) {
			Consumer c = new Consumer(allLatch, stream, topicStarGroupBConsumedItems);
			consumers.add(c);
			c.start();
		}
		List<MessageStream<String>> topicMoonGroupBStreams = groupBStreams.get(TOPIC_MOON);
		for(MessageStream<String> stream : topicMoonGroupBStreams) {
			Consumer c = new Consumer(allLatch, stream, topicMoonGroupBConsumedItems);
			consumers.add(c);
			c.start();
		}
		
		// wait threads to finish
		for(Thread thread: producers) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
		for(Thread thread: consumers) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
		// validation
		assertTrue(topicStarProducedItems.size() == topicStarTotalItemCount);
		assertTrue(topicMoonProducedItems.size() == topicMoonTotalItemCount);
		
		assertTrue(this.assertListEquals(topicStarProducedItems, topicStarGroupAConsumedItems));
		assertTrue(this.assertListEquals(topicStarProducedItems, topicStarGroupBConsumedItems));
		assertTrue(this.assertListEquals(topicMoonProducedItems, topicMoonGroupAConsumedItems));
		assertTrue(this.assertListEquals(topicMoonProducedItems, topicMoonGroupBConsumedItems));
		
		// close
		for(int i = 0; i < topicStarProducerNum; i++) {
			topicStarProducers[i].close();
		}
		for(int i = 0; i < topicMoonProducerNum; i++) {
			topicMoonProducers[i].close();
		}
		
		groupAStreamFactory.close();
		groupBStreamFactory.close();
	}
	
	boolean assertListEquals(List<String> source, List<String> target) {
		if (source.size() != target.size()) return false;
		Collections.sort(source);
		Collections.sort(target);
		
		for(int i = 0; i < source.size(); i++) {
			if (!source.get(i).equals(target.get(i))) {
				return false;
			}
		}
		
		return true;
	}
	
	@After
	public void cleanup() throws Exception {
		server1.close();
		server2.close();
		
		Utils.deleteDirectory(new File(server1.config.getLogDir()));
		Utils.deleteDirectory(new File(server2.config.getLogDir()));
		Thread.sleep(500);
	}
}
