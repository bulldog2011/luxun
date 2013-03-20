package com.leansoft.luxun.serializer;

import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.utils.Utils;

public class StringEncoder implements Encoder<String> {

	@Override
	public Message toMessage(String event) {
		return new Message(Utils.getBytes(event, "UTF-8"));
	}

}
