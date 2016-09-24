package com.leansoft.luxun.load;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.leansoft.luxun.common.exception.ConsumerTimeoutException;
import com.leansoft.luxun.consumer.ConsumerConfig;
import com.leansoft.luxun.consumer.IStreamFactory;
import com.leansoft.luxun.consumer.MessageStream;
import com.leansoft.luxun.consumer.StreamFactory;
import com.leansoft.luxun.producer.IProducer;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.ProducerData;
import com.leansoft.luxun.serializer.StringDecoder;
import com.leansoft.luxun.serializer.StringEncoder;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.ImmutableMap;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

public class StreamLoadTest {
	
	private int port1 = 9092;
	private int brokerId1 = 0;
	private int port2 = 9093;
	private int brokerId2 = 1;
	private LuxunServer server1 = null;
	private LuxunServer server2 = null;
	private String brokerList = brokerId1 + ":127.0.0.1:" + port1 + "," + brokerId2 + ":127.0.0.1:" + port2;
	
	@Before
	public void setup() {
		Properties props1 = TestUtils.createBrokerConfig(brokerId1, port1);
		ServerConfig config1 = new ServerConfig(props1);
		server1 = TestUtils.createServer(config1);
		Properties props2 = TestUtils.createBrokerConfig(brokerId2, port2);
		ServerConfig config2 = new ServerConfig(props2);
		server2 = TestUtils.createServer(config2);
	}
	
	@After
	public void clean() throws Exception {
		server1.close();
		server2.close();
		
		Utils.deleteDirectory(new File(server1.config.getLogDir()));
		Utils.deleteDirectory(new File(server2.config.getLogDir()));
		Thread.sleep(500);
	}
	
	// configurable parameters
	//////////////////////////////////////////////////////////////////
	private static int loop = 3;
	private static int topicFishTotalItemCount = 100000;
	private static int topicBirdTotalItemCount = 200000;
	private static int topicFishProducerNum = 2;
	private static int topicBirdProducerNum = 4;
	private static int topicFishConsumerGroupANum = 2;
	private static int topicFishConsumerGroupBNum = 4;
	private static int topicBirdConsumerGroupANum = 1;
	private static int topicBirdConsumerGroupBNum = 2;
	private static int messageLength = 1024;
	//////////////////////////////////////////////////////////////////
	
	private static enum Status {
		ERROR,
		SUCCESS
	}
	
	private static class Result {
		Status status;
	}
	
	private static final String TOPIC_FISH = "fish";
	private static final String TOPIC_BIRD = "bird";
	
	private static final AtomicInteger topicFishProducingItemCount = new AtomicInteger(0);
	private static final AtomicInteger topicBirdProducingItemCount = new AtomicInteger(0);
    private static final Map<String, AtomicInteger> topicFishItemMap = new ConcurrentHashMap<String,AtomicInteger>();
    private static final Map<String, AtomicInteger> topicBirdItemMap = new ConcurrentHashMap<String,AtomicInteger>();
    
    private static class TopicFishProducerThread extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final IProducer<String, String> stringProducer;
		
		public TopicFishProducerThread(CountDownLatch latch, Queue<Result> resultQueue, IProducer<String, String> stringProducer) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			this.stringProducer = stringProducer;
		}
		
		public void run() {
			Result result = new Result();
			String rndString = TestUtils.randomString(messageLength);
			try {
				latch.countDown();
				latch.await();
				
				while(true) {
					int count = topicFishProducingItemCount.incrementAndGet();
					if(count > topicFishTotalItemCount) break;
					String item = rndString + '-' + count;
					topicFishItemMap.put(item, new AtomicInteger(0));
					stringProducer.send(new ProducerData<String, String>(TOPIC_FISH, item));
				}
				result.status = Status.SUCCESS;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}	
    }
    
    private static class TopicBirdProducerThread extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final IProducer<String, String> stringProducer;
		
		public TopicBirdProducerThread(CountDownLatch latch, Queue<Result> resultQueue, IProducer<String, String> stringProducer) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			this.stringProducer = stringProducer;
		}
		
		public void run() {
			Result result = new Result();
			String rndString = TestUtils.randomString(messageLength);
			try {
				latch.countDown();
				latch.await();
				
				while(true) {
					int count = topicBirdProducingItemCount.incrementAndGet();
					if(count > topicBirdTotalItemCount) break;
					String item = rndString + '-' + count;
					topicBirdItemMap.put(item, new AtomicInteger(0));
					stringProducer.send(new ProducerData<String, String>(TOPIC_BIRD, item));
				}
				result.status = Status.SUCCESS;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}	
    }
    
	private static class TopicFishConsumerThread extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final MessageStream<String> stream;
		private final AtomicInteger itemCount;
		
		public TopicFishConsumerThread(CountDownLatch latch, Queue<Result> resultQueue,
				MessageStream<String> stream, AtomicInteger itemCount) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			this.stream = stream;
			this.itemCount = itemCount;
		}
		
		public void run() {
			Result result = new Result();
			try {
				latch.countDown();
				latch.await();
				
				try {
					while (itemCount.get() < topicFishTotalItemCount) {
						String item = stream.iterator().next();
						AtomicInteger counter = topicFishItemMap.get(item);
						assertNotNull(counter);
						counter.incrementAndGet();
						itemCount.incrementAndGet();					
					}
				
				} catch (ConsumerTimeoutException cte) {
					// expected
				}
				result.status = Status.SUCCESS;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}
	}
	
	private static class TopicBirdConsumerThread extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final MessageStream<String> stream;
		private final AtomicInteger itemCount;
		
		
		public TopicBirdConsumerThread(CountDownLatch latch, Queue<Result> resultQueue,
				MessageStream<String> stream, AtomicInteger itemCount) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			this.stream = stream;
			this.itemCount = itemCount;
		}
		
		public void run() {
			Result result = new Result();
			try {
				latch.countDown();
				latch.await();
				
				try {
					while (itemCount.get() < topicBirdTotalItemCount) {
						String item = stream.iterator().next();
						AtomicInteger counter = topicBirdItemMap.get(item);
						assertNotNull(counter);
						counter.incrementAndGet();
						itemCount.incrementAndGet();					
					}
				} catch (ConsumerTimeoutException cte) {
					// expected
				}
				result.status = Status.SUCCESS;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void doRunMixed(boolean async) throws Exception {
		//prepare
		CountDownLatch allLatch = new CountDownLatch(topicFishProducerNum  + topicBirdProducerNum  + 
				topicFishConsumerGroupANum + topicBirdConsumerGroupANum + topicFishConsumerGroupBNum + topicBirdConsumerGroupBNum);
		IProducer<String, String>[] topicFishProducers = new IProducer[topicFishProducerNum];
		IProducer<String, String>[] topicBirdProducers = new IProducer[topicBirdProducerNum];
		BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();
		
		//run testing
		for(int i = 0; i < topicFishProducerNum; i++) {
			Properties props = new Properties();
			props.put("serializer.class", StringEncoder.class.getName());
			props.put("broker.list", this.brokerList);
			if (async) {
			    props.put("producer.type", "async");
			    props.put("queue.enqueueTimeout.ms", "-1");
			    props.put("queue.time", "500");
			}
			ProducerConfig config = new ProducerConfig(props);
			IProducer<String, String> stringProducer = new com.leansoft.luxun.producer.Producer<String, String>(config);
			topicFishProducers[i] = stringProducer;
			TopicFishProducerThread p = new TopicFishProducerThread(allLatch, producerResults, stringProducer);
			p.start();
		}
		
		for(int i = 0; i < topicBirdProducerNum; i++) {
			Properties props = new Properties();
			props.put("serializer.class", StringEncoder.class.getName());
			props.put("broker.list", this.brokerList);
			if (async) {
			    props.put("producer.type", "async");
			    props.put("queue.enqueueTimeout.ms", "-1");
			    props.put("queue.time", "500");
			}
			ProducerConfig config = new ProducerConfig(props);
			IProducer<String, String> stringProducer = new com.leansoft.luxun.producer.Producer<String, String>(config);
			topicBirdProducers[i] = stringProducer;
			TopicBirdProducerThread p = new TopicBirdProducerThread(allLatch, producerResults, stringProducer);
			p.start();
		}

		AtomicInteger topicFishConsumerGroupAItemCount = new AtomicInteger(0);
		AtomicInteger topicBirdConsumerGroupAItemCount = new AtomicInteger(0);
		
		Properties props = TestUtils.createConsumerProperties(brokerList, "group-a", "consumer-a", 5000);
		ConsumerConfig groupAConsumerConfig = new ConsumerConfig(props);
		
		IStreamFactory groupAStreamFactory = new StreamFactory(groupAConsumerConfig);
		Map<String, List<MessageStream<String>>> groupAStreams = groupAStreamFactory.createMessageStreams(
				ImmutableMap.of(TOPIC_FISH, topicFishConsumerGroupANum, TOPIC_BIRD, topicBirdConsumerGroupANum), new StringDecoder());
		
		
		List<MessageStream<String>> topicFishGroupAStreams = groupAStreams.get(TOPIC_FISH);
		for(MessageStream<String> stream : topicFishGroupAStreams) {
			TopicFishConsumerThread c = new TopicFishConsumerThread(allLatch, consumerResults, stream, topicFishConsumerGroupAItemCount);
			c.start();
		}
		List<MessageStream<String>> topicBirdGroupAStreams = groupAStreams.get(TOPIC_BIRD);
		for(MessageStream<String> stream : topicBirdGroupAStreams) {
			TopicBirdConsumerThread c = new TopicBirdConsumerThread(allLatch, consumerResults, stream, topicBirdConsumerGroupAItemCount);
			c.start();
		}
		
		AtomicInteger topicFishConsumerGroupBItemCount = new AtomicInteger(0);
		AtomicInteger topicBirdConsumerGroupBItemCount = new AtomicInteger(0);
		
		props = TestUtils.createConsumerProperties(brokerList, "group-b", "consumer-b", 5000);
		ConsumerConfig groupBConsumerConfig = new ConsumerConfig(props);
		
		IStreamFactory groupBStreamFactory = new StreamFactory(groupBConsumerConfig);
		Map<String, List<MessageStream<String>>> groupBStreams = groupBStreamFactory.createMessageStreams(
				ImmutableMap.of(TOPIC_FISH, topicFishConsumerGroupBNum, TOPIC_BIRD, topicBirdConsumerGroupBNum), new StringDecoder());
		
		
		List<MessageStream<String>> topicFishGroupBStreams = groupBStreams.get(TOPIC_FISH);
		for(MessageStream<String> stream : topicFishGroupBStreams) {
			TopicFishConsumerThread c = new TopicFishConsumerThread(allLatch, consumerResults, stream, topicFishConsumerGroupBItemCount);
			c.start();
		}
		List<MessageStream<String>> topicBirdGroupBStreams = groupBStreams.get(TOPIC_BIRD);
		for(MessageStream<String> stream : topicBirdGroupBStreams) {
			TopicBirdConsumerThread c = new TopicBirdConsumerThread(allLatch, consumerResults, stream, topicBirdConsumerGroupBItemCount);
			c.start();
		}
		
		//verify
		for(int i = 0; i < topicFishProducerNum + topicBirdProducerNum; i++) {
			Result result = producerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		for(int i = 0; i < topicFishConsumerGroupANum + topicBirdConsumerGroupANum + topicFishConsumerGroupBNum + topicBirdConsumerGroupBNum; i++) {
			Result result = consumerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		assertTrue(topicFishItemMap.size() == topicFishTotalItemCount);
		for(AtomicInteger counter : topicFishItemMap.values()) {
			assertTrue(counter.get() == 2);
		}
		
		assertTrue(topicBirdItemMap.size() == topicBirdTotalItemCount);
		for(AtomicInteger counter : topicBirdItemMap.values()) {
			assertTrue(counter.get() == 2);
		}
		
		// closing
		for(int i = 0; i < topicFishProducerNum; i++) {
			topicFishProducers[i].close();
		}
		for(int i = 0; i < topicBirdProducerNum; i++) {
			topicBirdProducers[i].close();
		}
		
		groupAStreamFactory.close();
		groupBStreamFactory.close();
	}
	
	@Test
	public void runTest() throws Exception {
		
		System.out.println("Load test begin ...");
		
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunMixed sync producer] round " + (i + 1) + " of " + loop);
			this.doRunMixed(false);
			
			// reset
			topicFishProducingItemCount.set(0);
			topicBirdProducingItemCount.set(0);
			topicFishItemMap.clear();
			topicBirdItemMap.clear();
		}
		
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunMixed async producer] round " + (i + 1) + " of " + loop);
			this.doRunMixed(true);
			
			// reset
			topicFishProducingItemCount.set(0);
			topicBirdProducingItemCount.set(0);
			topicFishItemMap.clear();
			topicBirdItemMap.clear();
		}
		
		System.out.println("Load test finished successfully.");
	}

}
