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
		IPartitioner<String> mockPartitioner = EasyMock.createMock(IPartitioner.class);
		ProducerPool<String> mockProducerPool = EasyMock.createMock(ProducerPool.class);
		BrokerInfo mockBrokerInfo = EasyMock.createMock(BrokerInfo.class);
		
		Producer<String, String> producer = new Producer<String, String>(config, mockPartitioner, mockProducerPool, false, mockBrokerInfo);
		
		try {
			ProducerData<String, String> producerData = new ProducerData<String, String>("the_topic", "the_datum");
			producer.send(producerData);
			fail("Should have thrown a NoBrokersForTopicException.");
		} catch (NoBrokersForTopicException e) {
			assertTrue(e.getMessage().contains("the_topic"));
		}
	}

}
