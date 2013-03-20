package com.leansoft.luxun.common.exception;

public class AuthenticationFailException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public AuthenticationFailException() {
	}

	public AuthenticationFailException(String message) {
		super(message);
	}

	public AuthenticationFailException(Throwable cause) {
		super(cause);
	}

	public AuthenticationFailException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
