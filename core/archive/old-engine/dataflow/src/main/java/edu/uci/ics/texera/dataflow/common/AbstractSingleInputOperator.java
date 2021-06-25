package edu.uci.ics.texera.dataflow.common;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/**
 * AbstractSingleInputOperator is an abstract class that can be used by many operators.
 * This class implements logic such as open and close the input operator, manage the cursor, manage its
 * limit and offset, etc. An operator that extends this class must implement:
 * 
 * setUp(): It is called in open(). 
 *          Its purpose is to initialize resources, and build the output schema.
 * computeNextMatchingTuple(): It is called in getNextTuple().
 *          It returns the next available matching tuple, null if there's no more match.
 * cleanUp(). It is called in close(). 
 *          Its purpose is to deallocates resources.

 * @author Zuozhi Wang (zuozhiw)
 *
 */
public abstract class AbstractSingleInputOperator implements IOperator {
    
    protected IOperator inputOperator;
    protected Schema outputSchema;
    
    protected int cursor = CLOSED;
    
    protected int limit = Integer.MAX_VALUE;
    protected int offset = 0;
    
    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        try {
            if (this.inputOperator == null) {
                throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
            }
            inputOperator.open();
            setUp();
            
        } catch (Exception e) {
            throw new DataflowException(e.getMessage(), e);
        }
        cursor = OPENED;
    }
    
    /**
     * setUp necessary resources, variables in this function.
     * outputSchema MUST be initialized in setUP().
     * @throws TexeraException
     */
    protected abstract void setUp() throws TexeraException;

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        if (cursor >= limit + offset){
            return null;
        }
        try {
            Tuple resultTuple = null;
            while (true) {
                resultTuple = computeNextMatchingTuple();
                if (resultTuple == null) {
                    break;
                }
                cursor++;
                if (cursor > offset) {
                    break;
                }
            }
            return resultTuple;
        } catch (Exception e) {
            throw new DataflowException(e.getMessage(), e);
        }
    }

    /**
     * Give the input tuples, compute the next matching tuple. Return null if there's no more matching tuple.
     * 
     * @return next matching tuple, null if there's no more matching tuple.
     * @throws TexeraException
     */
    protected abstract Tuple computeNextMatchingTuple() throws TexeraException;

    public abstract Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException;

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        try {
            if (inputOperator != null) {
                inputOperator.close();
            }
            cleanUp();
        } catch (Exception e) {
            throw new DataflowException(e.getMessage(), e);
        }
        cursor = CLOSED;
    }
    
    protected abstract void cleanUp() throws TexeraException;

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }
    
    public IOperator getInputOperator() {
        return inputOperator;
    }
    
    public void setLimit(int limit) {
        this.limit = limit;
    }
    
    public void setOffset(int offset) {
        this.offset = offset;
    }
    
    public int getLimit() {
        return limit;
    }
    
    public int getOffset() {
        return offset;
    }
    
}
