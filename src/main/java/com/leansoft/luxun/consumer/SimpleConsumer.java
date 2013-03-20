package com.leansoft.luxun.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

//import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.leansoft.luxun.api.generated.ConsumeRequest;
import com.leansoft.luxun.api.generated.ConsumeResponse;
import com.leansoft.luxun.api.generated.ErrorCode;
import com.leansoft.luxun.api.generated.FindClosestIndexByTimeRequest;
import com.leansoft.luxun.api.generated.FindClosestIndexByTimeResponse;
import com.leansoft.luxun.api.generated.Result;
import com.leansoft.luxun.api.generated.ResultCode;
import com.leansoft.luxun.client.AbstractClient;
import com.leansoft.luxun.common.annotations.ClientSide;
import com.leansoft.luxun.common.annotations.ThreadSafe;
import com.leansoft.luxun.common.exception.ErrorMapper;
import com.leansoft.luxun.message.MessageList;

/**
 * Simple message consumer with Luxun server
 * 
 * @author bulldog
 *
 */
@ThreadSafe
@ClientSide
public class SimpleConsumer extends AbstractClient {
    
    public SimpleConsumer(String host, int port) {
        super(host, port);
    }

    public SimpleConsumer(String host, int port, int soTimeout) {
    	super(host, port, soTimeout);
    }
    
    public long findClosestIndexByTime( String topic, long timeStamp) throws TException {
    	FindClosestIndexByTimeRequest request = new FindClosestIndexByTimeRequest();
    	request.setTopic(topic);
    	request.setTimestamp(timeStamp);
    	
    	FindClosestIndexByTimeResponse response = this.findClosestIndexByTime(request);
    	Result result = response.getResult();
    	if (ResultCode.SUCCESS == result.getResultCode()) {
    		return response.getIndex();
    	} else {
    		throw ErrorMapper.toException(result.getErrorCode(), result.getErrorMessage());
    	}
    }
    
    /**
     * Consume by index
     * 
     * @param topic topic to consume
     * @param index start index to consume
     * @param fetchSize max fetch size in bytes
     * @return a list of luxun message list
     * @throws TException exception during the operation
     */
    public List<MessageList> consume(String topic, long index, int fetchSize) throws TException {
    	ConsumeRequest request = new ConsumeRequest();
    	request.setTopic(topic);
    	request.setStartIndex(index);
    	request.setMaxFetchSize(fetchSize);
    	
    	ConsumeResponse response = this.consume(request);
    	Result result = response.getResult();
    	if (ResultCode.SUCCESS == result.getResultCode()) {
    		List<ByteBuffer> bufList = response.getItemList();
    		List<MessageList> listOfMessageList = new ArrayList<MessageList>();
    		for(ByteBuffer buf : bufList) {
    			listOfMessageList.add(MessageList.fromThriftBuffer(buf));
    		}
    		return listOfMessageList;
    	} else {
    		if (result.getErrorCode() == ErrorCode.ALL_MESSAGE_CONSUMED) {
    			return new ArrayList<MessageList>();
    		}
    		
    		throw ErrorMapper.toException(result.getErrorCode(), result.getErrorMessage());
    	}
    }
    
    
    /**
     * Consume by fanoutId(aka consumer group name)
     * 
     * @param topic topic to consume
     * @param fanoudId fanout queue to consume
     * @param fetchSize max fetch size in bytes
     * @return a list of luxun message list
     * @throws TException exception during the operation
     */
    public List<MessageList> consume(String topic, String fanoutId, int fetchSize) throws TException {
    	ConsumeRequest request = new ConsumeRequest();
    	request.setTopic(topic);
    	request.setFanoutId(fanoutId);
    	request.setMaxFetchSize(fetchSize);
    	
    	ConsumeResponse response = this.consume(request);
    	Result result = response.getResult();
    	if (ResultCode.SUCCESS == result.getResultCode()) {
    		List<ByteBuffer> bufList = response.getItemList();
    		List<MessageList> listOfMessageList = new ArrayList<MessageList>();
    		for(ByteBuffer buf : bufList) {
    			listOfMessageList.add(MessageList.fromThriftBuffer(buf));
    		}
    		return listOfMessageList;
    	} else {
    		if (result.getErrorCode() == ErrorCode.ALL_MESSAGE_CONSUMED) {
    			return new ArrayList<MessageList>();
    		}
    		
    		throw ErrorMapper.toException(result.getErrorCode(), result.getErrorMessage());
    	}
    }
    
    public FindClosestIndexByTimeResponse findClosestIndexByTime(FindClosestIndexByTimeRequest request) throws TException {
    	synchronized (lock) {
    		getOrMakeConnection();
    		try {
				return luxunClient.findClosestIndexByTime(request);
			} catch (TException e) {
				reconnect();
				return luxunClient.findClosestIndexByTime(request);
			}
    	}
    }
    
    public List<ConsumeResponse> multiConsume(List<ConsumeRequest> consumeRequests) throws TException {
    	List<ConsumeResponse> consumeResponses = new ArrayList<ConsumeResponse>();
    	for(ConsumeRequest consumeRequest : consumeRequests) {
    		ConsumeResponse consumeResponse = this.consume(consumeRequest);
    		consumeResponses.add(consumeResponse);
    	}
    	return consumeResponses;
    }
    
    public ConsumeResponse consume(ConsumeRequest consumeRequest) throws TException {
    	synchronized (lock) {
    		getOrMakeConnection();
    		try {
				return luxunClient.consume(consumeRequest);
			} catch (TException e) {
				reconnect();
				return luxunClient.consume(consumeRequest);
			}
    	}
    }

}
