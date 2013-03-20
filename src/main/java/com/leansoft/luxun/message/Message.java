package com.leansoft.luxun.message;

import java.nio.ByteBuffer;

import com.leansoft.luxun.utils.Utils;

/**
 * A Luxun message,  wrapper of ByteBuffer
 * 
 * @author bulldog
 *
 */
public class Message {
	
	private ByteBuffer buffer;
	
	public Message(byte[] data) {
		buffer = ByteBuffer.wrap(data);
	}
	
	public Message(ByteBuffer buffer) {
		this.buffer = buffer;
	}
	
	public byte[] getBytes() {
		if (buffer != null) {
			buffer.mark();
			byte[] data = new byte[buffer.remaining()];
			buffer.get(data);
			buffer.reset();
			return data;
		}
		return null;
	}
	
	public int length() {
		if (buffer == null) return 0;
		return buffer.remaining();
	}
	
	public ByteBuffer getBufferDuplicate() {
		if (buffer == null) return null;
		return buffer.duplicate();
	}
	
	public void setBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
	}
	
	public String toString() {
		if (buffer == null) return null;
		return Utils.fromBytes(this.getBytes());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((buffer == null) ? 0 : buffer.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		if (buffer == null) {
			if (other.buffer != null)
				return false;
		} else if (!buffer.equals(other.buffer))
			return false;
		return true;
	}
	
}
