package com.leansoft.luxun.message;

import static org.junit.Assert.*;

import org.junit.Test;

import com.leansoft.luxun.message.Message;

public class MessageTest {
	
	@Test
	public void testMessage() {
		Message msg = new Message("test".getBytes());
		assertEquals("test", new String(msg.getBytes()));
		
		int length = msg.getBufferDuplicate().remaining();
		assertTrue(length == 4);
		byte[] data = new byte[length];
		msg.getBufferDuplicate().get(data);
		assertEquals("test", new String(data));
		
		// double check
		assertEquals("test", new String(msg.getBytes()));
		
		length = msg.getBufferDuplicate().remaining();
		assertTrue(length == 4);
		data = new byte[length];
		msg.getBufferDuplicate().get(data);
		assertEquals("test", new String(data));
		
		assertEquals(msg, new Message("test".getBytes()));
	}

}
