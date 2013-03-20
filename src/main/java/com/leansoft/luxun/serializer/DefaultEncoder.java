package com.leansoft.luxun.serializer;

import com.leansoft.luxun.message.Message;

public class DefaultEncoder implements Encoder<Message> {

	@Override
	public Message toMessage(Message event) {
		return event;
	}

}
