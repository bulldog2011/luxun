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

import com.leansoft.luxun.common.exception.TopicNotExistException;
import com.leansoft.luxun.consumer.SimpleConsumer;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.producer.IProducer;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.ProducerData;
import com.leansoft.luxun.serializer.Decoder;
import com.leansoft.luxun.serializer.StringDecoder;
import com.leansoft.luxun.serializer.StringEncoder;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

public class SimpleConsumeByFanoutIdLoadTest {
	
	private int port = 9092;
	private int brokerId = 0;
	private LuxunServer server = null;
	private String brokerList = brokerId + ":localhost:" + port;
	
	@Before
	public void setup() {
		Properties props1 = TestUtils.createBrokerConfig(brokerId, port);
		ServerConfig config1 = new ServerConfig(props1);
		server = TestUtils.createServer(config1);
	}
	
	@After
	public void clean() throws Exception {
		server.close();
		
		Utils.deleteDirectory(new File(server.config.getLogDir()));
		Thread.sleep(500);
	}
	
	// configurable parameters
	//////////////////////////////////////////////////////////////////
	private static int loop = 5;
	private static int totalItemCount = 100000;
	private static int producerNum = 4;
	private static int consumerGroupANum = 2;
	private static int consumerGroupBNum = 4;
	private static int messageLength = 1024;
	//////////////////////////////////////////////////////////////////
	
	private static enum Status {
		ERROR,
		SUCCESS
	}
	
	private static class Result {
		Status status;
	}
	
	private static final AtomicInteger producingItemCount = new AtomicInteger(0);
    private static final Map<String, AtomicInteger> itemMap = new ConcurrentHashMap<String,AtomicInteger>();
    
    private static class ProducerThread extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final IProducer<String, String> stringProducer;
		private final String topic;
		
		public ProducerThread(CountDownLatch latch, Queue<Result> resultQueue, IProducer<String, String> stringProducer, String topic) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			this.stringProducer = stringProducer;
			this.topic = topic;
		}
		
		public void run() {
			Result result = new Result();
			String rndString = TestUtils.randomString(messageLength);
			try {
				latch.countDown();
				latch.await();
				
				while(true) {
					int count = producingItemCount.incrementAndGet();
					if(count > totalItemCount) break;
					String item = rndString + '-' + count;
					itemMap.put(item, new AtomicInteger(0));
					stringProducer.send(new ProducerData<String, String>(topic, item));
				}
				result.status = Status.SUCCESS;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}	
    }
    
	// sequential consumer can work concurrently with producer
	private static class SequentialConsumerThread extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final SimpleConsumer simpleConsumer;
		private final String topic;
		private final Decoder<String> stringDecoder = new StringDecoder();
		private final String fanoutId;
		private final AtomicInteger itemCount;
		
		
		public SequentialConsumerThread(CountDownLatch latch, Queue<Result> resultQueue, String fanoutId, 
				SimpleConsumer simpleConsumer, String topic, AtomicInteger itemCount) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			this.simpleConsumer = simpleConsumer;
			this.topic = topic;
			this.fanoutId = fanoutId;
			this.itemCount = itemCount;
		}
		
		public void run() {
			Result result = new Result();
			try {
				latch.countDown();
				latch.await();
				
				while (itemCount.get() < totalItemCount) {
					try {
						List<MessageList> listOfMessageList = simpleConsumer.consume(topic, fanoutId, 10000);
						if (listOfMessageList.size() == 0) {
							Thread.sleep(20); // no item to consume yet, just wait a moment
						}
						for(MessageList messageList : listOfMessageList) {
							for(Message message : messageList) {
								String item = stringDecoder.toEvent(message);
								AtomicInteger counter = itemMap.get(item);
								assertNotNull(counter);
								counter.incrementAndGet();
								itemCount.incrementAndGet();
							}
						}
					} catch (TopicNotExistException ex) {
						Thread.sleep(200);// wait the producer to register the topic in broker
					}
					
				}
				result.status = Status.SUCCESS;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}
	}
	
	public void doRunMixed(int round) throws Exception {
		//prepare
		CountDownLatch allLatch = new CountDownLatch(producerNum + consumerGroupANum + consumerGroupBNum);
		@SuppressWarnings("unchecked")
		IProducer<String, String>[] producers = new IProducer[producerNum];
		SimpleConsumer[] groupAConsumers = new SimpleConsumer[consumerGroupANum];
		SimpleConsumer[] groupBConsumers = new SimpleConsumer[consumerGroupBNum];
		BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();
		String topic = "load-test002-" + round;
		
		//run testing
		for(int i = 0; i < producerNum; i++) {
			Properties props = new Properties();
			props.put("serializer.class", StringEncoder.class.getName());
			props.put("broker.list", this.brokerList);
			ProducerConfig config = new ProducerConfig(props);
			IProducer<String, String> stringProducer = new com.leansoft.luxun.producer.Producer<String, String>(config);
			producers[i] = stringProducer;
			ProducerThread p = new ProducerThread(allLatch, producerResults, stringProducer, topic);
			p.start();
		}
		
		AtomicInteger groupAItemCount = new AtomicInteger(0);
		for(int i = 0; i < consumerGroupANum; i++) {
			SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", 9092, 60000);
			groupAConsumers[i] = simpleConsumer;
			SequentialConsumerThread c = new SequentialConsumerThread(allLatch, consumerResults, "group-a", simpleConsumer, topic, groupAItemCount);
			c.start();
		}
		
		AtomicInteger groupBItemCount = new AtomicInteger(0);
		for(int i = 0; i < consumerGroupBNum; i++) {
			SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", 9092, 60000);
			groupBConsumers[i] = simpleConsumer;
			SequentialConsumerThread c = new SequentialConsumerThread(allLatch, consumerResults, "group-b", simpleConsumer, topic, groupBItemCount);
			c.start();
		}
		
		//verify
		for(int i = 0; i < producerNum; i++) {
			Result result = producerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		for(int i = 0; i < consumerGroupANum + consumerGroupBNum; i++) {
			Result result = consumerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		assertTrue(itemMap.size() == totalItemCount);
		for(AtomicInteger counter : itemMap.values()) {
			assertTrue(counter.get() == 2);
		}
		
		// closing
		for(int i = 0; i < producerNum; i++) {
			producers[i].close();
		}
		for(int i = 0; i < consumerGroupANum; i++) {
			groupAConsumers[i].close();
		}
		for(int i = 0; i < consumerGroupBNum; i++) {
			groupBConsumers[i].close();
		}
	}
	
	@Test
	public void runTest() throws Exception {
		
		System.out.println("Load test begin ...");
		
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
			this.doRunMixed(i);
			
			// reset
			producingItemCount.set(0);
			itemMap.clear();
		}
		
		System.out.println("Load test finished successfully.");
	}

}
