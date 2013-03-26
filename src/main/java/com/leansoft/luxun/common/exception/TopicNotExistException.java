package com.leansoft.luxun.common.exception;

/**
 * Topic does not exist on the borker
 * 
 * @author bulldog
 *
 */
public class TopicNotExistException extends RuntimeException {
	
    private static final long serialVersionUID = 1L;

	public TopicNotExistException() {
	}

	public TopicNotExistException(String message) {
		super(message);
	}

	public TopicNotExistException(Throwable cause) {
		super(cause);
	}

	public TopicNotExistException(String message, Throwable cause) {
		super(message, cause);
	}

}
