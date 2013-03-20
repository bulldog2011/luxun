package com.leansoft.luxun.message;

import static org.junit.Assert.*;

import org.junit.Test;

import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.message.MessageList;
import com.leansoft.luxun.message.generated.CompressionCodec;

public class MessageListTest {
	
	@Test
	public void testMessageList() {
		MessageList messageList1 = new MessageList();
		messageList1.add(new Message("hello".getBytes()));
		messageList1.add(new Message("there".getBytes()));
		
		MessageList messageList2 = new MessageList();
		messageList2.add(new Message("hello".getBytes()));
		messageList2.add(new Message("there".getBytes()));
		
		assertEquals(messageList1, messageList2);
		assertTrue(messageList1.equals(messageList2));
		
		messageList1 = new MessageList();
		messageList1.add(new Message("hello".getBytes()));
		messageList1.add(new Message("world".getBytes()));
		messageList1 = MessageList.fromThriftBuffer(messageList1.toThriftBuffer());
		messageList1 = MessageList.fromThriftBuffer(messageList1.toThriftBuffer());
		
		messageList2 = new MessageList();
		messageList2.add(new Message("hello".getBytes()));
		messageList2.add(new Message("there".getBytes()));
		
		assertFalse(messageList1.equals(messageList2));
		
		messageList1 = new MessageList(CompressionCodec.GZIP);
		messageList1.add(new Message("hello".getBytes()));
		messageList1.add(new Message("there".getBytes()));
		messageList1 = MessageList.fromThriftBuffer(messageList1.toThriftBuffer());
		messageList1 = MessageList.fromThriftBuffer(messageList1.toThriftBuffer());
		
		MessageList messageList11 = MessageList.fromThriftBuffer(messageList1.toThriftBuffer());
		assertTrue(messageList1.equals(messageList11));
		
		messageList2 = new MessageList(CompressionCodec.SNAPPY);
		messageList2.add(new Message("hello".getBytes()));
		messageList2.add(new Message("there".getBytes()));
		
		assertFalse(messageList1.equals(messageList2));
		
	}

}
