package edu.uci.ics.textdb.common.exception;

public class StorageException extends Exception {

    private static final long serialVersionUID = -7393624288798221759L;

    public StorageException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }
    
    public StorageException(String errorMessage) {
        super(errorMessage);
    }
}
