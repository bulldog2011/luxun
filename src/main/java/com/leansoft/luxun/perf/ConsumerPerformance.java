package com.leansoft.luxun.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leansoft.luxun.common.exception.ConsumerTimeoutException;
import com.leansoft.luxun.consumer.StreamFactory;
import com.leansoft.luxun.consumer.ConsumerConfig;
import com.leansoft.luxun.consumer.IStreamFactory;
import com.leansoft.luxun.consumer.MessageStream;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.utils.ImmutableMap;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;

public class ConsumerPerformance {
	
	private static Logger logger = LoggerFactory.getLogger(ConsumerPerformance.class);
	
    public static void main(String[] args) {
    	try {
    		ConsumerPerfConfig config = ConsumerPerfConfig.fromArgs(args);
			logger.info("Starting consumer ...");
			
		    AtomicLong totalMessagesRead = new AtomicLong(0);
		    AtomicLong totalBytesRead = new AtomicLong(0);
		    
		    if(!config.hideHeader) {
		        if(!config.showDetailedStats)
		          System.out.println("start.time, end.time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
		        else
		          System.out.println("time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
		    }
		    
		    IStreamFactory streamFactory = new StreamFactory(config.consumerConfig);
		    
		    Map<String, List<MessageStream<Message>>> topicMessageStreams = streamFactory.createMessageStreams(ImmutableMap.of(config.topic, config.numThreads));
		    List<ConsumerPerfThread> threadList = new ArrayList<ConsumerPerfThread>();
		    for(String topic:  topicMessageStreams.keySet()) {
		    	List<MessageStream<Message>> streamList = topicMessageStreams.get(topic);
		    	int i = 0;
		    	for (MessageStream<Message> stream : streamList) {
		    		threadList.add(new ConsumerPerfThread(i, "luxun-consumer-" + topic, stream, config, totalMessagesRead, totalBytesRead));
			    	i++;
		    	}
		    }
		    
		    logger.info("Sleeping for 1000 seconds.");
		    Thread.sleep(1000);
		    logger.info("starting threads");
		    long startMs = System.currentTimeMillis();
		    for (ConsumerPerfThread thread : threadList)
		      thread.start();

		    for (ConsumerPerfThread thread : threadList)
		      thread.shutdown();
		    
		    streamFactory.close();
		    
		    long endMs = System.currentTimeMillis();
		    double elapsedSecs = (endMs - startMs - config.consumerConfig.getConsumerTimeoutMs()) / 1000.0;
		    if (!config.showDetailedStats) {
		    	double totalMBRead = (totalBytesRead.get() * 1.0) / (1024 * 1024);
		    	System.out.println(String.format("%s, %s, %d, %.4f, %.4f, %d, %.4f", config.dateFormat.format(startMs), config.dateFormat.format(endMs),
		    			config.consumerConfig.getFetchSize(), totalMBRead, totalMBRead / elapsedSecs, totalMessagesRead.get(), totalMessagesRead.get() / elapsedSecs));
		    }
		    System.exit(0);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
    	
    }
	
	static class ConsumerPerfThread extends Thread {
		
		private final int threadId;
		private final MessageStream<Message> stream;
		private final ConsumerPerfConfig config;
		private final AtomicLong totalMessagesRead;
		private final AtomicLong totalBytesRead;
		
		private CountDownLatch shutdownLatch = new CountDownLatch(1);
		
		public void shutdown() {
			try {
				this.shutdownLatch.await();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
		public ConsumerPerfThread(
				int threadId,
				String name,
				MessageStream<Message> stream,
				ConsumerPerfConfig config,
				AtomicLong totalMessagesRead,
				AtomicLong totalBytesRead
				) {
			super(name);
			this.threadId = threadId;
			this.stream = stream;
			this.config = config;
			this.totalMessagesRead = totalMessagesRead;
			this.totalBytesRead = totalBytesRead;
		}
		
		@Override
		public void run() {
			long bytesRead = 0L;
			long messagesRead = 0L;
			long startMs = System.currentTimeMillis();
			long lastReportTime = startMs;
			long lastBytesRead = 0L;
			long lastMessagesRead = 0L;
			
			try {
				for(Message message : stream) {
					messagesRead += 1;
					bytesRead += message.length();
					
					if (messagesRead % config.reportingInterval == 0) {
						if (config.showDetailedStats) {
							this.printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime, System.currentTimeMillis());
						}
						lastReportTime = System.currentTimeMillis();
						lastMessagesRead = messagesRead;
						lastBytesRead = bytesRead;
					}
				}
			} catch (ConsumerTimeoutException te) {
				logger.info("No more to consume, consumer thread begin to exit ...");
			}
			
			totalMessagesRead.addAndGet(messagesRead);
			totalBytesRead.addAndGet(bytesRead);
			if (config.showDetailedStats) {
				this.printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs, System.currentTimeMillis());
			}
			
			this.shutdownLatch.countDown();
		}
		
		private void printMessage(int id, long bytesRead, long lastBytesRead, long messagesRead, long lastMessagesRead, long startMs, long endMs) {
			long elapsedMs = endMs - startMs;
			double totalMBRead = (bytesRead * 1.0) / (1024 * 1024);
			double mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
			System.out.println(String.format("%s, %d, %d, %.4f, %.4f, %d, %.4f", config.dateFormat.format(endMs), id,
					config.consumerConfig.getFetchSize(), totalMBRead,
					1000.0 * (mbRead/elapsedMs), messagesRead, ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0));
		}
		
		
	}
	
	static class ConsumerPerfConfig extends PerfConfig {
		
		protected static ArgumentAcceptingOptionSpec<String> brokerInfoOpt = parser.accepts("brokerinfo", "REQUIRED: broker info list to consume from." +
		                       "Multiple brokers can be given to allow concurrent concuming")
		        .withRequiredArg().
		        describedAs("brokerid1:hostname1:port1,brokerid2:hostname2:port2")
		        .ofType(String.class);
		
		protected static ArgumentAcceptingOptionSpec<String> groupIdOpt = 
			parser.accepts("group", "The group to consume on.")
				               .withRequiredArg()
		                       .describedAs("gid")
		                       .defaultsTo("perf-consumer-" + new Random().nextInt(100000))
		                       .ofType(String.class);
		
		protected static ArgumentAcceptingOptionSpec<Integer> fetchSizeOpt = 
			parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
						        .withRequiredArg().
						        describedAs("size")
						        .ofType(Integer.class)
						        .defaultsTo(1024 * 1024);
		
		protected static ArgumentAcceptingOptionSpec<Integer> numThreadsOpt = 
			parser.accepts("threads", "Number of processing threads.")
						        .withRequiredArg().
						        describedAs("count").
						        ofType(Integer.class).
						        defaultsTo(10);
		
		ConsumerConfig consumerConfig;
		int numThreads;
		
		public static ConsumerPerfConfig fromArgs(String[] args) throws Exception {
	        OptionSet options = parser.parse(args);
	        checkRequiredArgs(options, topicOpt, brokerInfoOpt);
	        
	        ConsumerPerfConfig config = new ConsumerPerfConfig();
	        
	        fillCommonConfig(options, config);
	        
	        Properties props = new Properties();
	        props.put("groupid", options.valueOf(groupIdOpt));
	        props.put("fetch.size", String.valueOf(options.valueOf(fetchSizeOpt)));
	        props.put("broker.list", options.valueOf(brokerInfoOpt));
	        props.put("consumer.timeout.ms", "5000");

	        config.consumerConfig = new ConsumerConfig(props);
	        
	        config.numThreads = options.valueOf(numThreadsOpt).intValue();
	        
	        return config;
	       
		}
	}

}
