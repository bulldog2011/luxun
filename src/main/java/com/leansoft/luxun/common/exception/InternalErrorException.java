package com.leansoft.luxun.common.exception;

public class InternalErrorException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;

	public InternalErrorException() {
	}

	public InternalErrorException(String message) {
		super(message);
	}

	public InternalErrorException(Throwable cause) {
		super(cause);
	}

	public InternalErrorException(String message, Throwable cause) {
		super(message, cause);
	}

}
