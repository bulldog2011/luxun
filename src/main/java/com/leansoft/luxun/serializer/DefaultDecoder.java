package com.leansoft.luxun.serializer;

import com.leansoft.luxun.message.Message;

public class DefaultDecoder implements Decoder<Message> {

	@Override
	public Message toEvent(Message message) {
		return message;
	}

}
