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
import java.net.URI;
import java.nio.ByteBuffer;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import com.leansoft.luxun.api.generated.ConsumeRequest;
import com.leansoft.luxun.api.generated.ConsumeResponse;
import com.leansoft.luxun.api.generated.ErrorCode;
import com.leansoft.luxun.api.generated.Result;
import com.leansoft.luxun.api.generated.ResultCode;
import com.leansoft.luxun.common.exception.ErrorMapper;
import com.leansoft.luxun.consumer.SimpleConsumer;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.utils.Closer;
import com.leansoft.luxun.utils.Utils;

/**
 * @author bulldog
 * 
 */
public class SimpleConsoleConsumer {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> topicIdOpt = parser.accepts("topic", "REQUIRED: The topic id to consumer on.")//
                .withRequiredArg().describedAs("topic").ofType(String.class);
        ArgumentAcceptingOptionSpec<String> serverOpt = parser.accepts("server", "REQUIRED: The luxun server connection string.")//
                .withRequiredArg().describedAs("luxun://hostname:port").ofType(String.class);
        ArgumentAcceptingOptionSpec<Long> offsetOpt = parser.accepts("index", "The index to start consuming from.")//
                .withRequiredArg().describedAs("index").ofType(Long.class).defaultsTo(0L);

        //
        OptionSet options = parser.parse(args);
        checkRequiredArgs(parser, options, topicIdOpt, serverOpt);

        final URI server = new URI(options.valueOf(serverOpt));
        final String topic = options.valueOf(topicIdOpt);
        final long startingIndex = options.valueOf(offsetOpt).longValue();
        //
        final SimpleConsumer consumer = new SimpleConsumer(server.getHost(), server.getPort(), 10000);
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                Closer.closeQuietly(consumer);
            }
        });
        //
        Thread thread = new Thread() {

            public void run() {
                long index = startingIndex;
                int consumed = 0;
                while (true) {
                    try {
                    	ConsumeRequest consumeRequest = new ConsumeRequest();
                    	consumeRequest.setTopic(topic);
                    	consumeRequest.setStartIndex(index);
                    	consumeRequest.setMaxFetchSize(1000000);
                    	
                    	ConsumeResponse consumeResponse = consumer.consume(consumeRequest);
                    	
                    	Result result = consumeResponse.getResult();
                    	if (result.getResultCode() == ResultCode.SUCCESS) {;
                    		for(ByteBuffer buffer : consumeResponse.getItemList()) {
                                consumed++;
                                
                                MessageList messageList = MessageList.fromThriftBuffer(buffer);
                                for(Message message : messageList) {
	                                System.out.println(String.format("[%d] %d: %s", consumed, index, //
	                                        Utils.toString(message.getBufferDuplicate(), "UTF-8")));
                                }
                                index++;
                    			
                    		}
                    	} else {
                    		if (result.getErrorCode() == ErrorCode.ALL_MESSAGE_CONSUMED) {
                    			break;
                    		}
                    		
                    		throw ErrorMapper.toException(result.getErrorCode(), result.getErrorMessage());
                    	}
                    } catch (RuntimeException re) {
                    	throw re;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        };
        thread.start();
        thread.join();
    }

    static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec<?>... optionSepcs) throws IOException {
        for (OptionSpec<?> arg : optionSepcs) {
            if (!options.has(arg)) {
                System.err.println("Missing required argument " + arg);
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }
    }
}
