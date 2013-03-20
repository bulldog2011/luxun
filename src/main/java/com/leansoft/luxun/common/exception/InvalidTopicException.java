package com.leansoft.luxun.common.exception;

public class InvalidTopicException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public InvalidTopicException() {
	}

	public InvalidTopicException(String message) {
		super(message);
	}

	public InvalidTopicException(Throwable cause) {
		super(cause);
	}

	public InvalidTopicException(String message, Throwable cause) {
		super(message, cause);
	}

}
