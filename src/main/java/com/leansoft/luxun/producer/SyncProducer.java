package com.leansoft.luxun.producer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

//import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.leansoft.luxun.api.generated.ProduceRequest;
import com.leansoft.luxun.client.AbstractClient;
import com.leansoft.luxun.common.exception.MessageSizeTooLargeException;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.mx.SyncProducerStats;

public class SyncProducer extends AbstractClient {
    
    private static final Random randomGenerator = new Random();
    
    private final SyncProducerConfig config;
    
    private int sentOnConnection = 0;

    private long lastConnectionTime;

	public SyncProducer(SyncProducerConfig config) {
        super(config.getHost(), config.getPort(), config.getSocketTimeoutMs(), config.getConnectTimeoutMs());
        this.config = config;
        this.lastConnectionTime = System.currentTimeMillis() - (long) (randomGenerator.nextDouble() * config.reconnectTimeInterval);
	}
	
	public void send(String topic, MessageList messageList) {
		
		ByteBuffer buffer = messageList.toThriftBuffer();
		
		if (buffer.remaining() > config.maxMessageSize) {
            throw new MessageSizeTooLargeException("payload size of " + buffer.remaining() + " larger than " + config.maxMessageSize);
		}
		
		ProduceRequest produceRequest = new ProduceRequest();
		produceRequest.setTopic(topic);
		produceRequest.setItem(buffer);
		send(produceRequest);
	}
	
	public void multiSend(List<ProduceRequest> requests) {
		for(ProduceRequest produceRequest : requests) {
			send(produceRequest);
		}
	}
	
	private void send(ProduceRequest produceRequest) {
		synchronized(lock) {
			long startTime = System.nanoTime();
			getOrMakeConnection();
			try {
				
				this.luxunClient.asyncProduce(produceRequest);
				
//				ProduceResponse produceResponse = this.luxunClient.produce(produceRequest);
//				Result result = produceResponse.getResult();
//				if (result.getResultCode() != ResultCode.SUCCESS) {
//					RuntimeException runtimeException = ErrorMapper.toException(result.getErrorCode(), result.getErrorMessage());
//					throw runtimeException;
//				}
			} catch (TException e) {
                // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
				disconnect();
                throw new RuntimeException(e);
			}
            sentOnConnection++;
            if (sentOnConnection >= config.reconnectCount//
                    || (config.reconnectTimeInterval >= 0 && System.currentTimeMillis() - lastConnectionTime >= config.reconnectTimeInterval)) {
                reconnect();
                sentOnConnection = 0;
                lastConnectionTime = System.currentTimeMillis();
            }
            final long endTime = System.nanoTime();
            SyncProducerStats.recordProduceRequest(endTime - startTime);
		}
	}

}
