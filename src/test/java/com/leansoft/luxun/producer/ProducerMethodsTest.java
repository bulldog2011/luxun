package com.leansoft.luxun.producer;

import static org.junit.Assert.*;

import java.util.Properties;

import com.leansoft.luxun.broker.BrokerInfo;
import com.leansoft.luxun.common.exception.NoBrokersForTopicException;
import com.leansoft.luxun.producer.Producer;
import com.leansoft.luxun.producer.ProducerConfig;
import com.leansoft.luxun.producer.ProducerData;
import com.leansoft.luxun.producer.ProducerPool;

import org.easymock.EasyMock;
import org.junit.Test;

public class ProducerMethodsTest {
	
	@SuppressWarnings("unchecked")
	@Test
	public void producerThrowsNoBrokerException() {
		Properties props = new Properties();
		props.put("broker.list", "placeholder");
		ProducerConfig config = new ProducerConfig(props);
		ProducerPool<String> mockProducerPool = EasyMock.createMock(ProducerPool.class);
		BrokerInfo mockBrokerInfo = EasyMock.createMock(BrokerInfo.class);
		
		Producer<String> producer = new Producer<String>(config, mockProducerPool, false, mockBrokerInfo);
		
		try {
			ProducerData<String> producerData = new ProducerData<String>("the_topic", "the_datum");
			producer.send(producerData);
			fail("Should have thrown a NoBrokersForPartitionException.");
		} catch (NoBrokersForTopicException e) {
			assertTrue(e.getMessage().contains("the_topic"));
		}
	}

}
