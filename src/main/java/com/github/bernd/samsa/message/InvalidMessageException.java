package com.github.bernd.samsa.message;

public class InvalidMessageException extends RuntimeException {
    public InvalidMessageException(final String message) {
        super(message);
    }
}
