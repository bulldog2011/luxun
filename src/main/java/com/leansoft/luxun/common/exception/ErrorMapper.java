package com.leansoft.luxun.common.exception;

import com.leansoft.luxun.api.generated.ErrorCode;

/**
 * Error mapping between error code and exception
 * 
 * @author bulldog
 *
 */
public class ErrorMapper {
	
	public static ErrorCode toErrorCode(Exception e) {
		
		if (e instanceof InvalidTopicException) {
			return ErrorCode.INVALID_TOPIC;
		} else if (e instanceof IndexOutOfBoundsException) {
			return ErrorCode.INDEX_OUT_OF_BOUNDS;
		} else if (e instanceof MessageSizeTooLargeException) {
			return ErrorCode.MESSAGE_SIZE_TOO_LARGE;
		} else {
			return ErrorCode.INTERNAL_ERROR;
		}
		
	}
	
	public static RuntimeException toException(ErrorCode errorCode, String errorMessage) {
		switch (errorCode) {	
			case INVALID_TOPIC:
				return new InvalidTopicException(errorMessage);
			case INDEX_OUT_OF_BOUNDS:
				return new IndexOutOfBoundsException(errorMessage);
			case MESSAGE_SIZE_TOO_LARGE:
				return new MessageSizeTooLargeException(errorMessage);
			case AUTHENTICATION_FAILURE:
				return new AuthenticationFailException(errorMessage);
			default:
				return new InternalErrorException(errorMessage);
		}
	}

}
