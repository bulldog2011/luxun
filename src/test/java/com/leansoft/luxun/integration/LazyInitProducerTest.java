package com.leansoft.luxun.integration;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TException;
import org.junit.Test;

import com.leansoft.luxun.api.generated.ConsumeRequest;
import com.leansoft.luxun.api.generated.ConsumeResponse;
import com.leansoft.luxun.api.generated.ErrorCode;
import com.leansoft.luxun.api.generated.ProduceRequest;
import com.leansoft.luxun.api.generated.ResultCode;
import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

public class LazyInitProducerTest extends ProducerConsumerTestHarness {
	
	Properties props;
	ServerConfig config;
	List<ServerConfig> configs;
	List<LuxunServer> servers;
	

	@Override
	public void setUp() throws Exception {
		port = TestUtils.choosePort();
		props = TestUtils.createBrokerConfig(0, port);
		config = new ServerConfig(props);
		configs = new ArrayList<ServerConfig>();
		configs.add(config);
		servers = new ArrayList<LuxunServer>();
		servers.add(TestUtils.createServer(config));
		super.setUp();
	}
	
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		
		for(LuxunServer server : servers) {
			server.close();
		}
		
		for(ServerConfig config : configs) {
			Utils.deleteDirectory(new File(config.getLogDir()));
		}
	}
	
	@Test
	public void testProduceAndFetchByIndex() throws TException {
		// send some messages
		String topic = "test";
		MessageList messageList = new MessageList();
		messageList.add(new Message("hello".getBytes()));
		messageList.add(new Message("there".getBytes()));
		
		producer.send(topic, messageList);
		List<MessageList> listOfMessageList = consumer.consume(topic, 0, 10000);
		assertTrue(listOfMessageList.size() == 1);
		assertTrue(isMessageListEqual(messageList, listOfMessageList.get(0)));
		
		try {
			consumer.consume(topic, -1, 10000);
			fail("excepted IndexOutOfBoundsException was not thrown");
		} catch(IndexOutOfBoundsException e) {
			// expected
		}
	}
	
	@Test
	public void testProduceAndFetchByFanoutId() throws TException {
		// send some messages
		String topic = "test";
		MessageList messageList = new MessageList();
		messageList.add(new Message("hello".getBytes()));
		messageList.add(new Message("there".getBytes()));
		
		producer.send(topic, messageList);
		List<MessageList> listOfMessageList = consumer.consume(topic, "fan1", 10000);
		assertTrue(listOfMessageList.size() == 1);
		assertTrue(isMessageListEqual(messageList, listOfMessageList.get(0)));
		
		// fan1 is empty now
		listOfMessageList = consumer.consume(topic, "fan1", 10000);
		assertTrue(listOfMessageList.isEmpty());
	}
	
	@Test
	public void testProduceAndFetchOneByOneByIndex() throws TException {
		// send some messages
		String topic = "test";
		for(int i = 0; i < 10; i++) {
			MessageList messageList = new MessageList();
			messageList.add(new Message(("hello" + i).getBytes()));
			producer.send(topic, messageList);
		}
		
		// consume one by one
		for(int i = 0; i < 5; i++) {
			List<MessageList> listOfMessageList = consumer.consume(topic, i, -1);
			assertTrue(listOfMessageList.size() == 1);
			MessageList msgList = listOfMessageList.get(0);
			assertTrue(msgList.size() == 1);
			Message msg = msgList.get(0);
			assertEquals("hello" + i, new String(msg.getBytes()));
		}
		
		// consume remaining in a batch
		List<MessageList> listOfMessageList = consumer.consume(topic, 5, 10000);
		assertTrue(listOfMessageList.size() == 5);
		for(int i = 5; i < 10; i++) {
			MessageList msgList = listOfMessageList.get(i - 5);
			assertTrue(msgList.size() == 1);
			Message msg = msgList.get(0);
			assertEquals("hello" + i, new String(msg.getBytes()));
		}
	}
	
	@Test
	public void testProduceAndFetchOneByOneByFanoutId() throws TException {
		// send some messages
		String topic = "test";
		for(int i = 0; i < 10; i++) {
			MessageList messageList = new MessageList();
			messageList.add(new Message(("hello" + i).getBytes()));
			producer.send(topic, messageList);
		}
		
		// consume one by one
		for(int i = 0; i < 5; i++) {
			List<MessageList> listOfMessageList = consumer.consume(topic, "fan", 0);
			assertTrue(listOfMessageList.size() == 1);
			MessageList msgList = listOfMessageList.get(0);
			assertTrue(msgList.size() == 1);
			Message msg = msgList.get(0);
			assertEquals("hello" + i, new String(msg.getBytes()));
		}
		
		// consume remaining in a batch
		List<MessageList> listOfMessageList = consumer.consume(topic, "fan", 10000);
		assertTrue(listOfMessageList.size() == 5);
		for(int i = 5; i < 10; i++) {
			MessageList msgList = listOfMessageList.get(i - 5);
			assertTrue(msgList.size() == 1);
			Message msg = msgList.get(0);
			assertEquals("hello" + i, new String(msg.getBytes()));
		}
	}
	
	public void testProduceAndMultiFetchByIndex() throws TException {
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		for(String topic : topics) {
			MessageList messageList = new MessageList();
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			producer.send(topic, messageList);
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setStartIndex(0);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
		}
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 1);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			index++;
		}
		
		
		fetches.clear();
		// send some invalid indexes
		for(String topic : topics) {
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setStartIndex(-1);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
		}
		
		responses = consumer.multiConsume(fetches);
		for(ConsumeResponse response : responses) {
			assertTrue(response.getResult().getResultCode() == ResultCode.FAILURE);
			assertTrue(response.getResult().getErrorCode() == ErrorCode.INDEX_OUT_OF_BOUNDS);
		}
	}
	
	public void testProduceAndMultiFetchByFanoutId() throws TException {
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		for(String topic : topics) {
			MessageList messageList = new MessageList();
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			producer.send(topic, messageList);
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setFanoutId("fan1");
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
		}
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 1);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			index++;
		}
	}
	
	public void testProduceAndMultiFetchByIndexWithCompression() throws TException {
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		for(String topic : topics) {
			MessageList messageList = new MessageList(CompressionCodec.GZIP);
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			producer.send(topic, messageList);
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setStartIndex(0);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
		}
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 1);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			index++;
		}
	}
	
	public void testProduceAndMultiFetchByFanoutIdWithCompression() throws TException {
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		for(String topic : topics) {
			MessageList messageList = new MessageList(CompressionCodec.GZIP);
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			producer.send(topic, messageList);
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setFanoutId("fan0");
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
		}
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 1);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			index++;
		}
	}
	
	
	public void testMultiProduceThenFetchByIndex() throws TException {
		// send some messages
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		List<ProduceRequest> produceList = new ArrayList<ProduceRequest>();
		for(String topic : topics) {
			MessageList messageList = new MessageList();
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			produceList.add(new ProduceRequest(messageList.toThriftBuffer(), topic));
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setStartIndex(0);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
		}
		
		producer.multiSend(produceList);
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 1);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			index++;
		}
	}
	
	public void testMultiProduceThenFetchByFanoutId() throws TException {
		// send some messages
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		List<ProduceRequest> produceList = new ArrayList<ProduceRequest>();
		int i = 0;
		for(String topic : topics) {
			MessageList messageList = new MessageList();
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			produceList.add(new ProduceRequest(messageList.toThriftBuffer(), topic));
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setFanoutId("fan" + i);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
			i++;
		}
		
		producer.multiSend(produceList);
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 1);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			index++;
		}
	}
	
	public void testMultiProduceWithCompressionThenFetchByIndex() throws TException {
		// send some messages
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		List<ProduceRequest> produceList = new ArrayList<ProduceRequest>();
		for(String topic : topics) {
			MessageList messageList = new MessageList(CompressionCodec.GZIP);
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			produceList.add(new ProduceRequest(messageList.toThriftBuffer(), topic));
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setStartIndex(0);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
		}
		
		producer.multiSend(produceList);
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 1);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			index++;
		}
	}
	
	public void testMultiProduceWithCompressionThenFetchByFanoutId() throws TException {
		// send some messages
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		List<ProduceRequest> produceList = new ArrayList<ProduceRequest>();
		int i = 0;
		for(String topic : topics) {
			MessageList messageList = new MessageList(CompressionCodec.GZIP);
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			produceList.add(new ProduceRequest(messageList.toThriftBuffer(), topic));
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setFanoutId("fan" + i);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
			i++;
		}
		
		producer.multiSend(produceList);
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 1);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			index++;
		}
	}
	
	public void testMultiProduceResendThenFetchByIndex() throws TException {
		// send some messages
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		List<ProduceRequest> produceList = new ArrayList<ProduceRequest>();
		for(String topic : topics) {
			MessageList messageList = new MessageList();
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			produceList.add(new ProduceRequest(messageList.toThriftBuffer(), topic));
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setStartIndex(0);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
		}
		
		producer.multiSend(produceList);
		
		// resend the same ultisend
		producer.multiSend(produceList);
		
		TestUtils.sleepQuietly(1000);  // give time to broker to save the messages
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 2);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(1)));
			index++;
		}
	}
	
	public void testMultiProduceResendThenFetchByFanoutId() throws TException {
		// send some messages
		Map<String, MessageList> messages = new HashMap<String, MessageList>();
		List<String> topics = new ArrayList<String>();
		topics.add("test1");
		topics.add("test2");
		topics.add("test3");
		
		List<ConsumeRequest> fetches = new ArrayList<ConsumeRequest>();
		List<ProduceRequest> produceList = new ArrayList<ProduceRequest>();
		int i = 0;
		for(String topic : topics) {
			MessageList messageList = new MessageList();
			messageList.add(new Message(("a_" + topic).getBytes()));
			messageList.add(new Message(("b_" + topic).getBytes()));
			messages.put(topic, messageList);
			produceList.add(new ProduceRequest(messageList.toThriftBuffer(), topic));
			ConsumeRequest consumeRequest = new ConsumeRequest();
			consumeRequest.setTopic(topic);
			consumeRequest.setFanoutId("fan" + i);
			consumeRequest.setMaxFetchSize(10000);
			fetches.add(consumeRequest);
			i++;
		}
		
		producer.multiSend(produceList);
		
		// resend the same ultisend
		producer.multiSend(produceList);
		
		TestUtils.sleepQuietly(1000); // give time to broker to save the messages
		
		List<ConsumeResponse> responses = consumer.multiConsume(fetches);
		int index = 0;
		for(String topic : topics) {
			ConsumeResponse response = responses.get(index);
			List<ByteBuffer> itemList = response.getItemList();
			List<MessageList> listOfMessageList = new ArrayList<MessageList>();
			for(ByteBuffer buffer : itemList) {
				listOfMessageList.add(MessageList.fromThriftBuffer(buffer));
			}
			assertTrue(listOfMessageList.size() == 2);
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(0)));
			assertTrue(isMessageListEqual(messages.get(topic), listOfMessageList.get(1)));
			index++;
		}
	}
	
	
	public void testConsumeNotExistTopicByIndex() throws TException {
		String newTopic = "new-topic";
		ConsumeRequest consumeRequest = new ConsumeRequest();
		consumeRequest.setTopic(newTopic);
		consumeRequest.setStartIndex(0);
		consumeRequest.setMaxFetchSize(10000);
		ConsumeResponse consumeResponse = consumer.consume(consumeRequest);
		assertTrue(consumeResponse.getResult().getResultCode() == ResultCode.FAILURE);
		assertTrue(consumeResponse.getResult().getErrorCode() == ErrorCode.TOPIC_NOT_EXIST);
		File logFile = new File(config.getLogDir(), newTopic);
		assertTrue(!logFile.exists());
	}
	
	public void testConsumeNotExistTopicByFanoutId() throws TException {
		String newTopic = "new-topic";
		ConsumeRequest consumeRequest = new ConsumeRequest();
		consumeRequest.setTopic(newTopic);
		consumeRequest.setFanoutId("fan8");
		consumeRequest.setMaxFetchSize(10000);
		ConsumeResponse consumeResponse = consumer.consume(consumeRequest);
		assertTrue(consumeResponse.getResult().getResultCode() == ResultCode.FAILURE);
		assertTrue(consumeResponse.getResult().getErrorCode() == ErrorCode.TOPIC_NOT_EXIST);
		File logFile = new File(config.getLogDir(), newTopic);
		assertTrue(!logFile.exists());
	}
	
	private boolean isMessageListEqual(MessageList source, MessageList target) {
		if (source.size() != target.size()) {
			return false;
		}
		
		for(int i = 0; i < source.size(); i++) {
			if (!source.get(i).equals(target.get(i))) {
				return false;
			}
		}
		
		return true;
	}
}
