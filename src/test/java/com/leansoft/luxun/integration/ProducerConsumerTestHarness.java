package com.leansoft.luxun.integration;

import java.util.Properties;

import com.leansoft.luxun.consumer.SimpleConsumer;
import com.leansoft.luxun.producer.SyncProducer;
import com.leansoft.luxun.producer.SyncProducerConfig;

import junit.framework.TestCase;

public abstract class ProducerConsumerTestHarness extends TestCase {
	
	protected int port;
	String host = "localhost";
	SyncProducer producer = null;
	SimpleConsumer consumer = null;
	
	@Override
	public void setUp() throws Exception {
		Properties props = new Properties();
	      props.put("host", host);
	      props.put("port", String.valueOf(port));
	      props.put("connect.timeout.ms", "100000");
	      props.put("reconnect.interval", "10000");
	      producer = new SyncProducer(new SyncProducerConfig(props));
	      consumer = new SimpleConsumer(host,
	                                   port,
	                                   1000000);
	}
	
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		producer.close();
		consumer.close();
	}

}
