/**
 * 
 */
package Engine.SchemaSupport.exception;

/**
 *  Thrown to indicate that an exception occurs when a Texera operator processes data.
 */
public class DataflowException extends AmberException {

    private static final long serialVersionUID = -4779329768850579335L;

    public DataflowException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }

    public DataflowException(String errorMessage) {
        super(errorMessage);
    }
    
    public DataflowException(Throwable throwable) {
        super(throwable);
    }
    
}
