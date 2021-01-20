package com.sproutloud.starter.stream.exception;

/**
 * Runtime Exception when data fails the validations
 *
 * @author sgoyal
 */
public class FieldValidationException extends RuntimeException {

    /**
     * {@link RuntimeException} when a message is passed
     *
     * @param message to be sent in the exception with details on why exception occurred
     */
    public FieldValidationException(String message) {
        super(message);
    }

}
