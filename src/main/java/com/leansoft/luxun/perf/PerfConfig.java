package com.leansoft.luxun.perf;

import java.io.IOException;
import java.text.SimpleDateFormat;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

public abstract class PerfConfig {
	
	protected static OptionParser parser = new OptionParser();
	
	protected static ArgumentAcceptingOptionSpec<String> topicOpt = parser.accepts("topic", "REQUIRED: The topic to produce to or consume from.")
	                                                 .withRequiredArg().describedAs("topic").ofType(String.class);
	
	protected static ArgumentAcceptingOptionSpec<Long> numMessagesOpt = parser.accepts("messages", "The number of messages to send or consume")
	                                                  .withRequiredArg().describedAs("count").ofType(Long.class).defaultsTo(Long.MAX_VALUE);
	
	protected static ArgumentAcceptingOptionSpec<Integer> reportingIntervalOpt = parser.accepts("reporting-interval", "Interval at which to print progress info.")
	                                                            .withRequiredArg().describedAs("size").ofType(Integer.class).defaultsTo(5000);
	
	
	protected static ArgumentAcceptingOptionSpec<String> dateFormatOpt = parser.accepts("date-format", "The date format to use for formatting the time field. " +
		                                                       "See java.text.SimpleDateFormat for options.")
                                                       .withRequiredArg().describedAs("date format").ofType(String.class)
                                                       .defaultsTo("yyyy-MM-dd HH:mm:ss:SSS");
	
	protected static OptionSpecBuilder showDetailedStatsOpt = parser.accepts("show-detailed-stats", "If set, stats are reported for each reporting " +
                                                    "interval as configured by reporting-interval");
	
	protected static OptionSpecBuilder hideHeaderOpt = parser.accepts("hide-header", "If set, skips printing the header for the stats ");
	
	protected static void checkRequiredArgs(OptionSet options, OptionSpec<?>... optionSepcs) throws IOException {
        for (OptionSpec<?> arg : optionSepcs) {
            if (!options.has(arg)) {
                System.err.println("Missing required argument " + arg);
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }
    }
	
	String topic;
	long numMessages;
	int reportingInterval;
	boolean showDetailedStats;
	SimpleDateFormat dateFormat;
	boolean hideHeader;
	
	protected static void fillCommonConfig(OptionSet options, PerfConfig config) throws Exception {
        config.topic = options.valueOf(topicOpt);
        config.numMessages = options.valueOf(numMessagesOpt).longValue();
        config.reportingInterval = options.valueOf(reportingIntervalOpt).intValue();
        config.showDetailedStats = options.has(showDetailedStatsOpt);
        config.dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt));
        config.hideHeader = options.has(hideHeaderOpt);
	}
	
}
