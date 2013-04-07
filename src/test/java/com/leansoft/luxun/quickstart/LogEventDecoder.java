package com.leansoft.luxun.quickstart;

import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.serializer.Decoder;
import com.leansoft.luxun.serializer.ThriftConverter;

public class LogEventDecoder implements Decoder<LogEvent> {

	@Override
	public LogEvent toEvent(Message message) {
		byte[] binary = message.getBytes();
		return (LogEvent) ThriftConverter.toEvent(binary, LogEvent.class);
	}

}
