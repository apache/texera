package Engine.Common.Exception;

/**
 * Superclass of all exceptions inside Amber Engine.
 */
public class AmberException extends RuntimeException {

    private static final long serialVersionUID = 4359106470500687632L;

    public AmberException(String errorMessage) {
        super(errorMessage);
    }
    
    public AmberException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }
    
    public AmberException(Throwable throwable) {
        super(throwable);
    }
    
}
