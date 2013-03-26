package com.leansoft.luxun.console;

import java.io.IOException;
import java.util.Arrays;

import org.apache.thrift.TException;

import com.leansoft.luxun.api.generated.DeleteTopicRequest;
import com.leansoft.luxun.api.generated.DeleteTopicResponse;
import com.leansoft.luxun.api.generated.Result;
import com.leansoft.luxun.api.generated.ResultCode;
import com.leansoft.luxun.client.AbstractClient;
import com.leansoft.luxun.common.exception.ErrorMapper;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.lang.String.format;

public class AdminConsole extends AbstractClient {
	
    public AdminConsole(String host, int port) {
		super(host, port);
	}
    
	/**
	 * delete topic no longer used
	 * 
	 * @param topic topic name
	 * @param password authentication password
	 * @return number of partitions deleted
	 * @throws TException if any thrift error occurs
	 */
	public boolean deleteTopic(String topic, String password) throws TException {
    	synchronized (lock) {
    		getOrMakeConnection();
    	
    		DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest();
    		deleteTopicRequest.setTopic(topic);
    		deleteTopicRequest.setPassword(password);
    		
    		DeleteTopicResponse deleteTopicResponse = this.luxunClient.deleteTopic(deleteTopicRequest);
    		
    		
    		Result result = deleteTopicResponse.getResult();
    		if (result.getResultCode() == ResultCode.SUCCESS) {
    			return true;
    		} else {
    			RuntimeException runtimeException = ErrorMapper.toException(result.getErrorCode(), result.getErrorMessage());
    			throw runtimeException;
    		}
    	}
	}

	public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> hostOpt = parser.acceptsAll(Arrays.asList("h", "host"), "server address")//
                .withRequiredArg().describedAs("host").ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> portOpt = parser.acceptsAll(Arrays.asList("p", "port"), "server port")//
                .withRequiredArg().describedAs("port").ofType(int.class);
        ArgumentAcceptingOptionSpec<String> topicOpt = parser.acceptsAll(Arrays.asList("t", "topic"), "topic name")//
                .withRequiredArg().describedAs("topic").ofType(String.class);
        //
        parser.acceptsAll(Arrays.asList("d", "delete"), "delete topic");
        //
        ArgumentAcceptingOptionSpec<String> passwordOpt = parser
                .acceptsAll(Arrays.asList("password"), "luxun password")//
                .withOptionalArg().describedAs("password").ofType(String.class);
        //
        OptionSet options = parser.parse(args);
        boolean delete = options.has("d") || options.has("delete");
        if (!delete) {
            printHelp(parser, null);
        }
        checkRequiredArgs(parser, options, hostOpt, portOpt, topicOpt);

        String topic = options.valueOf(topicOpt);
        String host = options.valueOf(hostOpt);
        int port = options.valueOf(portOpt);

        AdminConsole admin = new AdminConsole(host, port);
        try {
            final String password = options.valueOf(passwordOpt);
            boolean result = admin.deleteTopic(topic, password);
            if (result) {
            	System.out.println(format("delete topic [%s]", topic));
            } else {
            	System.out.println(format("fail to delete topic [%s]", topic));                	
            }
        } finally {
            admin.close();
        }
    }
	
    static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec<?>... optionSepcs)
            throws IOException {
        for (OptionSpec<?> arg : optionSepcs) {
            if (!options.has(arg)) {
                printHelp(parser, arg);
            }
        }
    }
	
    private static void printHelp(OptionParser parser, OptionSpec<?> arg) throws IOException {
        System.err.println("Delete Topic");
        System.err.println("Usage: -d -h <host> -p <port> -t <topic> --password <password>");
        if (arg != null) {
            System.err.println("Missing required argument " + arg);
        }
        // parser.formatHelpWith(new MyFormatter());
        parser.printHelpOn(System.err);
        System.exit(1);
    }

}
