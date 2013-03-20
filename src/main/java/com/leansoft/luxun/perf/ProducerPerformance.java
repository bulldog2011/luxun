package com.leansoft.luxun.perf;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.producer.Producer;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.ProducerData;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

public class ProducerPerformance {
	
	private static final Logger logger = Logger.getLogger(ProducerPerformance.class);
	
    public static void main(String[] args) {
    	try {
			ProducerPerfConfig config = ProducerPerfConfig.fromArgs(args);
			
			if (!config.isFixSize) {
				logger.info("WARN: Throughput will be slower due to changing message size per request");
			}
			
			AtomicLong totalBytesSent = new AtomicLong(0);
			AtomicLong totalMessagesSent = new AtomicLong(0);
			ExecutorService executor = Executors.newFixedThreadPool(config.numThreads);
			CountDownLatch allDone = new CountDownLatch(config.numThreads);
			long startMs = System.currentTimeMillis();
			Random rand = new Random();
			
		    if(!config.hideHeader) {
		        if(!config.showDetailedStats)
		          System.out.println("start.time, end.time, compression, message.size, batch.size, total.data.sent.in.MB, MB.sec, " +
		            "total.data.sent.in.nMsg, nMsg.sec");
		        else
		        	System.out.println("time, compression, thread.id, message.size, batch.size, total.data.sent.in.MB, MB.sec, " +
		            "total.data.sent.in.nMsg, nMsg.sec");
		    }
		    
		    for(int i = 0; i < config.numThreads; i++) {
		    	executor.execute(new ProducerThread(i, config, totalBytesSent, totalMessagesSent, allDone, rand));
		    }
		    
		    allDone.await();
		    
			long endMs = System.currentTimeMillis();
			double elapsedSecs = (endMs - startMs) / 1000.0;
		    if(!config.showDetailedStats) {
		        double totalMBSent = (totalBytesSent.get() * 1.0)/ (1024 * 1024);
		        System.out.println(String.format("%s, %s, %d, %d, %d, %.2f, %.4f, %d, %.4f", config.dateFormat.format(startMs),
		          config.dateFormat.format(endMs), config.compressionCodec.getValue(), config.messageSize, config.batchSize,
		          totalMBSent, totalMBSent/elapsedSecs, totalMessagesSent.get(), totalMessagesSent.get()/elapsedSecs));
		      }
		      System.exit(0);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	
	
	static class ProducerThread implements Runnable {
		
		private int threadId;
		private ProducerPerfConfig config;
		private Producer<byte[]> producer;
		private Random rand;
		
		private AtomicLong totalBytesSent;
		private AtomicLong totalMessagesSent;
		private CountDownLatch allDone;
		
		public ProducerThread(int threadId,
				ProducerPerfConfig config,
				AtomicLong totalBytesSent,
				AtomicLong totalMessagesSent,
				CountDownLatch allDone,
				Random rand) {
			
			this.threadId = threadId;
			this.config = config;
			this.rand = rand;
			
			this.totalBytesSent = totalBytesSent;
			this.totalMessagesSent = totalMessagesSent;
			this.allDone = allDone;
			
			Properties props = new Properties();
			String[] brokerInfoArray = config.brokerInfo.split("=");
			props.put("broker.list", brokerInfoArray[1]);
			props.put("compression.codec", String.valueOf(config.compressionCodec.getValue()));
			if(config.isAsync) {
				props.put("producer.type", "async");
				props.put("batch.size", String.valueOf(config.batchSize));
				props.put("queue.enqueueTimeout.ms", "-1");
			}
			ProducerConfig producerConfig = new ProducerConfig(props);
			producer = new Producer<byte[]>(producerConfig);
		}

		@Override
		public void run() {
			long bytesSent = 0L;
			long lastBytesSent = 0L;
			int nSends = 0;
			int lastNSends = 0;
			byte[] data = new byte[config.messageSize];
			long reportTime = System.currentTimeMillis();
			long lastReportTime = reportTime;
			long messagesPerThread = config.numMessages / config.numThreads;
			logger.debug("Messages per thread = " + messagesPerThread);
			
			long j = 0L;
			while(j < messagesPerThread) {
				
				if (!config.isFixSize) {
					int length = rand.nextInt(config.messageSize);
					data = new byte[length];
				}
				bytesSent += data.length;
					
				ProducerData<byte[]> producerData = new ProducerData<byte[]>(config.topic, data);
				producer.send(producerData);
				nSends += 1;
				if (nSends % config.reportingInterval == 0) {
					reportTime = System.currentTimeMillis();
					double elapsed = (reportTime - lastReportTime) / 1000.0;
					double mbBytesSent = ((bytesSent - lastBytesSent) * 1.0) / (1024 * 1024);
					double numMessagesPerSec = (nSends - lastNSends) / elapsed;
					double mbPerSec = mbBytesSent / elapsed;
					String formattedReportTime = config.dateFormat.format(reportTime);
					if (config.showDetailedStats) {
						System.out.println((String.format("%s, %d, %d, %d, %d, %.2f, %.4f, %d, %.4f", formattedReportTime, config.compressionCodec.getValue(),
					              threadId, config.messageSize, config.batchSize, (bytesSent*1.0)/(1024 * 1024), mbPerSec, nSends, numMessagesPerSec)));
					}
					lastReportTime = reportTime;
					lastBytesSent = bytesSent;
					lastNSends = nSends;
				}
				
				j++;
			}
			producer.close();
			totalBytesSent.addAndGet(bytesSent);
			totalMessagesSent.addAndGet(nSends);
			allDone.countDown();
		}
		
	}
	
	
	static class ProducerPerfConfig extends PerfConfig {
		
		protected static ArgumentAcceptingOptionSpec<String> brokerInfoOpt = parser.accepts("brokerinfo", "REQUIRED: broker info list.")
															        .withRequiredArg().
															        describedAs("broker.list=brokerid1:hostname1:port1,brokerid2:hostname2:port2")
															        .ofType(String.class);

		protected static ArgumentAcceptingOptionSpec<Integer> messageSizeOpt = parser.accepts("message-size", "The size of each message.")
													         .withRequiredArg().
													         describedAs("size")
													         .ofType(Integer.class).
													         defaultsTo(100);

		protected static OptionSpecBuilder varyMessageSizeOpt = parser.accepts("vary-message-size", "If set, message size will vary up to the given maximum.");
		
		protected static OptionSpecBuilder asyncOpt = parser.accepts("async", "If set, messages are sent asynchronously.");
		
		protected static ArgumentAcceptingOptionSpec<Integer> batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch.")
													        .withRequiredArg().
													        describedAs("size").
													        ofType(Integer.class).
													        defaultsTo(200);
		
		protected static ArgumentAcceptingOptionSpec<Integer> numThreadsOpt = parser.accepts("threads", "Number of sending threads.")
													         .withRequiredArg().
													         describedAs("count").
													         ofType(Integer.class).
													         defaultsTo(10);
		
		protected static ArgumentAcceptingOptionSpec<Integer> compressionCodecOption = parser.accepts("compression-codec", "If set, messages are sent compressed")
													                  .withRequiredArg().
													                  describedAs("compression codec")
													                  .ofType(Integer.class).
													                  defaultsTo(0);
		
		String brokerInfo;
		int messageSize;
		boolean isFixSize;
		boolean isAsync;
		int batchSize;
		int numThreads;
		CompressionCodec compressionCodec;
		
		public static ProducerPerfConfig fromArgs(String[] args) throws Exception {
	        OptionSet options = parser.parse(args);
	        checkRequiredArgs(options, topicOpt, brokerInfoOpt, numMessagesOpt);
		
	        ProducerPerfConfig config = new ProducerPerfConfig();
	        fillCommonConfig(options, config);
	        
	        config.brokerInfo = options.valueOf(brokerInfoOpt);
	        config.messageSize = options.valueOf(messageSizeOpt).intValue();
	        config.isFixSize = !options.has(varyMessageSizeOpt);
	        config.isAsync = options.has(asyncOpt);
	        config.batchSize = options.valueOf(batchSizeOpt).intValue();
	        config.numThreads = options.valueOf(numThreadsOpt).intValue();
	        config.compressionCodec = CompressionCodec.findByValue(options.valueOf(compressionCodecOption).intValue());
	        
	        return config;
		}
		
	}

}
