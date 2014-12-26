package com.github.bernd.samsa;

public class SamsaStorageException extends Exception {
    public SamsaStorageException(String message) {
        super(message);
    }

    public SamsaStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
