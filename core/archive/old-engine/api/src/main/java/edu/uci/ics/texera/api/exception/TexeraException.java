package edu.uci.ics.texera.api.exception;

/**
 * Superclass of all exceptions inside Texera Engine.
 */
public class TexeraException extends RuntimeException {

    private static final long serialVersionUID = 4359106470500687632L;

    public TexeraException(String errorMessage) {
        super(errorMessage);
    }
    
    public TexeraException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }
    
    public TexeraException(Throwable throwable) {
        super(throwable);
    }
    
}
