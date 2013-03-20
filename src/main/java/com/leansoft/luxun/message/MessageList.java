package com.leansoft.luxun.message;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.leansoft.luxun.message.compress.CompressionUtils;
import com.leansoft.luxun.message.generated.CompressionCodec;
import com.leansoft.luxun.message.generated.TMessageList;
import com.leansoft.luxun.message.generated.TMessagePack;
import com.leansoft.luxun.serializer.ThriftConverter;

/**
 * Basic unit for Luxun messages transportation and storage.
 * 
 * @author bulldog
 *
 */
public class MessageList extends ArrayList<Message> {

	private static final long serialVersionUID = 1L;
	private CompressionCodec codec;
	
	public MessageList() {
		this(CompressionCodec.NO_COMPRESSION);
	}
	
	public MessageList(CompressionCodec codec) {
		super();
		this.codec = codec;
	}
	
	/**
	 * Get compression codec used
	 * 
	 * @return
	 */
	public CompressionCodec getCompressionCodec() {
		return this.codec;
	}
	
	/**
	 * Set compression codec to be used
	 * 
	 * @param codec
	 */
	public void setCompressionCodec(CompressionCodec codec) {
		this.codec = codec;
	}
	
	/**
	 * Convert the message list to thrift buffer, compress accordingly.
	 * 
	 * @return a TMessagePack serialized buffer
	 */
	public ByteBuffer toThriftBuffer() {
		TMessagePack tMessagePack = new TMessagePack();
		if (codec != null) {
			tMessagePack.setAttribute((byte) codec.getValue());
		}
		
		TMessageList tMessageList = new TMessageList();
		for(Message message : this) {
			ByteBuffer buffer = message.getBufferDuplicate();
			if (buffer != null) {
				tMessageList.addToMessages(buffer);
			}
		}
		
		byte[] messageListData = ThriftConverter.toBytes(tMessageList);
		if (codec != null && codec != CompressionCodec.NO_COMPRESSION) {
			byte[] compressedData = CompressionUtils.compress(messageListData, codec);
			tMessagePack.setMessageList(compressedData);
		} else {
			tMessagePack.setMessageList(messageListData);
		}
		byte[] messagePackData = ThriftConverter.toBytes(tMessagePack);
		return ByteBuffer.wrap(messagePackData);
	}
	
	/**
	 * Factory method, convert a thrift buffer to MessageList, decompress accordingly
	 * 
	 * @param buffer TMessagePack serialized buffer
	 * @return a MessageList instance
	 */
	public static MessageList fromThriftBuffer(ByteBuffer buffer) {
		MessageList messageList = new MessageList();
		if (buffer == null) return messageList;
		TMessagePack messagePack = (TMessagePack) ThriftConverter.toEvent(buffer, TMessagePack.class);
    	byte[] messageListData = messagePack.getMessageList();
		CompressionCodec codec = CompressionCodec.findByValue(messagePack.getAttribute());
		messageList.codec = codec;
    	if (codec != null && codec != CompressionCodec.NO_COMPRESSION) {
    		messageListData = CompressionUtils.decompress(messageListData, codec);
		}
    	TMessageList tMessageList = (TMessageList) ThriftConverter.toEvent(messageListData, TMessageList.class);
    	if (tMessageList.getMessagesSize() > 0) {
	    	for(ByteBuffer msgBuf :tMessageList.getMessages()) {
	    		messageList.add(new Message(msgBuf));
	    	}
    	}
    	
    	return messageList;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((codec == null) ? 0 : codec.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageList other = (MessageList) obj;
		if (codec != other.codec)
			return false;
		return true;
	}
}
