package com.sproutloud.starter.stream.exception;

/**
 * Runtime Exception when data is missing or incorrect or insufficient
 *
 * @author sgoyal
 */
public class InsufficientDataException extends RuntimeException {

    /**
     * {@link RuntimeException} when a message is passed
     *
     * @param message to be sent in the exception with details on why exception occurred
     */
    public InsufficientDataException(String message) {
        super(message);
    }

    /**
     * {@link RuntimeException} if throwable object is passed with details of the exception
     *
     * @param throwable exception object
     */
    public InsufficientDataException(Throwable throwable) {
        super(throwable);
    }
}
