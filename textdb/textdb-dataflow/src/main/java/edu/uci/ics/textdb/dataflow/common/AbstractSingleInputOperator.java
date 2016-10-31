package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;

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
    
    protected int resultCursor = -1;
    protected int limit = Integer.MAX_VALUE;
    protected int offset = 0;
    
    @Override
    public void open() throws Exception {
        if (cursor != CLOSED) {
            return;
        }
        try {
            if (this.inputOperator == null) {
                throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
            }
            inputOperator.open();
            setUp();
            
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = OPENED;
    }
    
    /**
     * setUp necessary resources, variables in this function.
     * outputSchema MUST be initialized in setUP().
     * @throws DataFlowException
     */
    protected abstract void setUp() throws DataFlowException;

    @Override
    public ITuple getNextTuple() throws Exception {
        if (cursor == CLOSED) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        if (resultCursor >= limit + offset - 1){
            return null;
        }
        try {
            ITuple resultTuple = null;
            while (true) {
                resultTuple = computeNextMatchingTuple();
                if (resultTuple == null) {
                    break;
                }
                resultCursor++;
                if (resultCursor >= offset) {
                    break;
                }
            }
            return resultTuple;
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * Give the input tuples, compute the next matching tuple. Return null if there's no more matching tuple.
     * 
     * @return next matching tuple, null if there's no more matching tuple.
     * @throws Exception
     */
    protected abstract ITuple computeNextMatchingTuple() throws Exception;

    public abstract ITuple processOneInputTuple(ITuple tuple) throws DataFlowException;

    @Override
    public void close() throws DataFlowException {
        if (cursor == CLOSED) {
            return;
        }
        try {
            if (inputOperator != null) {
                inputOperator.close();
            }
            cleanUp();
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = CLOSED;
    }
    
    protected abstract void cleanUp() throws DataFlowException;

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
    
    public int getLimit() {
        return limit;
    }
    
    public void setOffset(int offset) {
        this.offset = offset;
    }
    
    public int getOffset() {
        return offset;
    }
    
}
