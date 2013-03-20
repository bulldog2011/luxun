package com.leansoft.luxun.serializer;

import com.leansoft.luxun.message.Message;


/**
 * Convert object of type T to Luxun message.
 * 
 * @author bulldog
 *
 * @param <T>
 */
public interface Encoder<T> {

	Message toMessage(T event);

}
