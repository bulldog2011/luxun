package com.leansoft.luxun.quickstart;

import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.serializer.Encoder;
import com.leansoft.luxun.serializer.ThriftConverter;

public class LogEventEncoder implements Encoder<LogEvent> {

	@Override
	public Message toMessage(LogEvent event) {
		byte[] binary = ThriftConverter.toBytes(event);
		return new Message(binary);
 	}

}
