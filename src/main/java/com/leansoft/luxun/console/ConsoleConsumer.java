/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.leansoft.luxun.console;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import com.leansoft.luxun.common.exception.ConsumerTimeoutException;
import com.leansoft.luxun.consumer.ConsumerConfig;
import com.leansoft.luxun.consumer.IStreamFactory;
import com.leansoft.luxun.consumer.MessageStream;
import com.leansoft.luxun.consumer.StreamFactory;
import com.leansoft.luxun.serializer.StringDecoder;
import com.leansoft.luxun.utils.ImmutableMap;

/**
 * @author bulldog
 * 
 */
public class ConsoleConsumer {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> topicIdOpt = parser.accepts("topic", "REQUIRED: The topic id to consumer on.")//
                .withRequiredArg().describedAs("topic").ofType(String.class);
	    ArgumentAcceptingOptionSpec<String> brokerInfoOpt = parser.accepts("brokerinfo", "REQUIRED: broker info list to consume from." +
				                "Multiple brokers can be given to allow concurrent concuming")
				 .withRequiredArg().
				 describedAs("brokerid1:hostname1:port1,brokerid2:hostname2:port2")
				 .ofType(String.class);
        final ArgumentAcceptingOptionSpec<String> groupIdOpt = parser.accepts("group", "The group id to consume on.")//
                .withRequiredArg().describedAs("gid").defaultsTo("console-consumer-" + new Random().nextInt(100000)).ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> fetchSizeOpt = parser.accepts("fetch-size", "The amount of data in bytes to fetch in a single request.")//
                .withRequiredArg().describedAs("size").ofType(Integer.class).defaultsTo(1024 * 1024);
        ArgumentAcceptingOptionSpec<Integer> consumerTimeoutMsOpt = parser
                .accepts("consumer-timeout-ms", "consumer throws timeout exception after waiting this much " + "of time without incoming messages")//
                .withRequiredArg().describedAs("prop").ofType(Integer.class).defaultsTo(5000);
        ArgumentAcceptingOptionSpec<String> messageFormatterOpt = parser
                .accepts("formatter", "The name of a class to use for formatting luxun messages for display.").withRequiredArg().describedAs("class")
                .ofType(String.class).defaultsTo(NewlineMessageFormatter.class.getName());
        //

        final OptionSet options = tryParse(parser, args);
        checkRequiredArgs(parser, options, topicIdOpt, brokerInfoOpt);
        //
        Properties props = new Properties();
        props.put("groupid", options.valueOf(groupIdOpt));
        props.put("fetch.size", options.valueOf(fetchSizeOpt).toString());
        props.put("broker.list", options.valueOf(brokerInfoOpt));
        props.put("consumer.timeout.ms", options.valueOf(consumerTimeoutMsOpt).toString());
        //
        //
        final ConsumerConfig config = new ConsumerConfig(props);
        final String topic = options.valueOf(topicIdOpt);
        @SuppressWarnings("unchecked")
        final Class<MessageFormatter> messageFormatterClass = (Class<MessageFormatter>) Class.forName(options.valueOf(messageFormatterOpt));

        IStreamFactory streamFactory = new StreamFactory(config);
        MessageStream<String> stream = streamFactory.createMessageStreams(ImmutableMap.of(topic, 1), new StringDecoder()).get(topic).get(0);
        final MessageFormatter formatter = messageFormatterClass.newInstance();
        //formatter.init(props);
        //
        try {
            for (String message : stream) {
            	formatter.writeTo(message, System.out);
            }
        } catch(ConsumerTimeoutException cte) {
        	System.out.println("Consumer timeouted.");
        } finally {
            System.out.flush();
            formatter.close();
            streamFactory.close();
        }
    }

    static OptionSet tryParse(OptionParser parser, String[] args) {
        try {
            return parser.parse(args);
        } catch (OptionException e) {
            e.printStackTrace();
            return null;
        }
    }

    static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec<?>... optionSepcs) throws IOException {
        for (OptionSpec<?> arg : optionSepcs) {
            if (!options.has(arg)) {
                System.err.println("Missing required argument " + arg);
               // parser.formatHelpWith(new MyFormatter());
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }
    }
}
