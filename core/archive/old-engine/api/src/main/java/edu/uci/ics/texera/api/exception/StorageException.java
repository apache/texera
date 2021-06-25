package edu.uci.ics.texera.api.exception;

/*
 * Thrown to indicate that an exception occurs when writing/reading data.
 */
public class StorageException extends TexeraException {

    private static final long serialVersionUID = -7393624288798221759L;
    
    public StorageException(String errorMessage) {
        super(errorMessage);
    }

    public StorageException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }
    
    public StorageException(Throwable throwable) {
        super(throwable);
    }
    
}
