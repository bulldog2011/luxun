package com.leansoft.luxun.perf;

import java.net.URI;
import java.util.List;

import org.apache.log4j.Logger;

import com.leansoft.luxun.api.generated.Constants;
import com.leansoft.luxun.consumer.SimpleConsumer;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

public class SimpleConsumerPerformance {
	
	private static final Logger logger = Logger.getLogger(SimpleConsumerPerformance.class);
	
    public static void main(String[] args) {
    	try {
    		
    		ConsumerPerfConfig config = ConsumerPerfConfig.fromArgs(args);
    		
    		if (!config.hideHeader) {
    			if (!config.showDetailedStats) {
    				System.out.println("start.time, end.time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
    			} else {
    				System.out.println("time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
    			}
    		}
    		
    		SimpleConsumer consumer = new SimpleConsumer(config.url.getHost(), config.url.getPort(), 30 * 1000);
    		long timeStamp = config.fromLatest ? Constants.LATEST_TIME : Constants.EARLIEST_TIME;
    		long index = consumer.findClosestIndexByTime(config.topic, timeStamp);
    		logger.info("Consumer got " + (config.fromLatest ? Constants.LATEST_INDEX_STRING : Constants.EARLIEST_INDEX_STRING) + " index : " + index);
    		
    		long startMs = System.currentTimeMillis();
    		boolean done = false;
    		long totalBytesRead = 0L;
    		long totalMessagesRead = 0L;
    		int consumedInterval = 0;
    		long lastReportTime = startMs;
    		long lastBytesRead = 0L;
    		long lastMessagesRead = 0L;
    		
    		while(!done) {
    			List<MessageList> listOfMessageList = consumer.consume(config.topic, index, config.fetchSize);
    			
    			int messagesRead = 0;
    			int bytesRead = 0;
    			
    			for(MessageList messageList : listOfMessageList) {
    				
    				for(Message message : messageList) {
    					bytesRead += message.length();
    				}
    				
    				messagesRead += messageList.size();
    			}
    			
    			if (messagesRead == 0 || totalMessagesRead > config.numMessages) {
    				done = true;
    			} else {
    				index += listOfMessageList.size();
    			}
    			
    			totalBytesRead += bytesRead;
    			totalMessagesRead += messagesRead;
    			consumedInterval += messagesRead;
    			
    			if (consumedInterval > config.reportingInterval) {
    				if (config.showDetailedStats) {
    					long reportTime = System.currentTimeMillis();
    					double elapsed = (reportTime - lastReportTime) / 1000.0;
    					double totalMBRead = ((totalBytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
    					System.out.println(String.format("%s, %d, %.4f, %.4f, %d, %.4f", config.dateFormat.format(reportTime), config.fetchSize,
    							(totalBytesRead * 1.0) / (1024 * 1024), totalMBRead / elapsed,
    							totalMessagesRead, (totalMessagesRead - lastMessagesRead) / elapsed
    							));
    				}
    				lastReportTime = System.currentTimeMillis();
    				lastBytesRead = totalBytesRead;
    				lastMessagesRead = totalMessagesRead;
    				consumedInterval = 0;
    			}
    		}
    		
    		long reportTime = System.currentTimeMillis();
    		double elapsed = (reportTime - startMs) / 1000.0;
    		
    		if (!config.showDetailedStats) {
    			double totalMBRead = (totalBytesRead * 1.0) / (1024 * 1024);
    			System.out.println(String.format("%s, %s, %d, %.4f, %.4f, %d, %.4f", config.dateFormat.format(startMs),
    					config.dateFormat.format(reportTime), config.fetchSize, totalMBRead, totalMBRead/elapsed,
    					totalMessagesRead, totalMessagesRead / elapsed
    					));
    		}
    		
    		consumer.close();
    		
    		System.exit(0);
    		
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}	
    }	
	
	static class ConsumerPerfConfig extends PerfConfig {
		
		protected static ArgumentAcceptingOptionSpec<String> urlOpt = parser.accepts("server", "REQUIRED: The hostname of the server to connect to.")
		        .withRequiredArg().
		        describedAs("luxun://hostname:port")
		        .ofType(String.class);
		
		protected static OptionSpecBuilder resetBeginningIndexOpt = parser.accepts("from-latest", "If the consumer does not already have an established " +
		   "index to consume from, start with the latest message present in the log rather than the earliest message.");
		
		
		protected static ArgumentAcceptingOptionSpec<Integer> fetchSizeOpt = parser.accepts("fetch-size", "The fetch size to use for consumption.")
		        .withRequiredArg().
		        describedAs("bytes")
		        .ofType(Integer.class)
		        .defaultsTo(1024 * 1024);
		
		URI url;
		int fetchSize;
		boolean fromLatest;
		int partition;
		
		
		public static ConsumerPerfConfig fromArgs(String[] args) throws Exception {
	        OptionSet options = parser.parse(args);
	        checkRequiredArgs(options, topicOpt, urlOpt);
	        
	        ConsumerPerfConfig config = new ConsumerPerfConfig();
	        
	        fillCommonConfig(options, config);
	        
	        config.url = new URI(options.valueOf(urlOpt));
	        config.fetchSize = options.valueOf(fetchSizeOpt).intValue();
	        config.fromLatest = options.has(resetBeginningIndexOpt);
	        
	        return config;
	       
		}
		
	}

}
