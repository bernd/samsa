package com.github.bernd.samsa;

/**
 * Generic Samsa/Kafka exception
 */
public class SamsaException extends RuntimeException {
    public SamsaException() {
    }

    public SamsaException(String message) {
        super(message);
    }

    public SamsaException(String message, Throwable cause) {
        super(message, cause);
    }

    public SamsaException(Throwable cause) {
        super(cause);
    }

    public SamsaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
