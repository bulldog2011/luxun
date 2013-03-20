package com.leansoft.luxun.serializer;

import com.leansoft.luxun.message.Message;
import com.leansoft.luxun.utils.Utils;

public class StringDecoder implements Decoder<String> {

	@Override
	public String toEvent(Message message) {
		return Utils.fromBytes(message.getBytes());
	}

}
