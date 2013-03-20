package com.leansoft.luxun.serializer;

import com.leansoft.luxun.message.Message;

/**
 * convert a luxun message to object of type T
 * 
 * @author bulldog
 *
 * @param <T>
 */
public interface Decoder<T> {
	
	T toEvent(Message message);
}
