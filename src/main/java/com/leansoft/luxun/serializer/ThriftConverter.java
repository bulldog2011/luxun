package com.leansoft.luxun.serializer;

import java.nio.ByteBuffer;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

@SuppressWarnings("rawtypes")
public abstract class ThriftConverter {
	
	static ThreadLocalTSerializer tSerializerLocal = new ThreadLocalTSerializer();
	static ThreadLocalTDeserializer tDeserializerLocal = new ThreadLocalTDeserializer();
	
	static class ThreadLocalTSerializer extends ThreadLocal<TSerializer> {
    	@Override
    	protected synchronized TSerializer initialValue() {
    		return new TSerializer();
    	}
	}
	
	static class ThreadLocalTDeserializer extends ThreadLocal<TDeserializer> {
    	@Override
    	protected synchronized TDeserializer initialValue() {
    		return new TDeserializer();
    	}
	}

	public static TBase toEvent(byte[] data, Class<? extends TBase> clazz) {
		TBase tBase;
		try {
			tBase = clazz.newInstance();
			TDeserializer tDeserializer = tDeserializerLocal.get();
			tDeserializer.deserialize(tBase, data);
		} catch (Exception e) {
			throw new RuntimeException("fail to deserialize into thrift object of type " + clazz, e);
		}

		return tBase;
	}

	public static TBase toEvent(ByteBuffer buffer, Class<? extends TBase> clazz) {
		byte[] data = new byte[buffer.remaining()];
		buffer.get(data);
		return toEvent(data, clazz);
	}
	
	public static byte[] toBytes(TBase event) {
		TSerializer tSerializer = tSerializerLocal.get();
		byte[] data;
		try {
			data = tSerializer.serialize(event);
		} catch (TException e) {
			throw new RuntimeException("fail to serialize thrift object of type " + event.getClass());
		}
		return data;
	}

	public static ByteBuffer toBuffer(TBase event) {
		return ByteBuffer.wrap(toBytes(event));
	}
}
