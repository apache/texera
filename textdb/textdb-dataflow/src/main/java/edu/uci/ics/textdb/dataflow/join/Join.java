package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;


/**
 * The Join operator is an operator which intends to perform a "join" over the
 * the outputs of two other operators based on certain conditions defined
 * using the JoinPredicate.
 * 
 * The JoinPredicate currently takes:
 * ID attribute -> Which serves as the document/tuple ID. Only for the tuples
 * whose IDs match, we perform the join.
 * Join Attribute -> The attribute to perform Join on.
 * and Threshold -> The value within which the difference of span starts and
 * the difference of span ends should be for the join to take place.
 * 
 * Join takes two operators: innerOperator and outerOperator.
 * Each operator has a stream of output tuples, Join performs join on 
 * two tuples' span lists only if two tuples have the same ID.
 * 
 * Two operators usually have the same schema, but they don't necessarily have to.
 * Join requires two operators to share ID attribute and attribute to join.
 * For other attributes, join will perform an intersection on them.
 * 
 * Join assumes two tuples are the same if their ID are same.
 * If some attribute values of two tuples are different, if the attribute is the 
 * join attribute, the tuple is discarded. If the attribute is not join attribute,
 * then one of the values will be chosen to become the output value.
 * 
 * @author Sripad Kowshik Subramanyam (sripadks)
 *
 */
public class Join implements IOperator {

    private IOperator outerOperator;
    private IOperator innerOperator;
    private IJoinPredicate joinPredicate;
    // To indicate if next result from outer operator has to be obtained.
    private boolean shouldIGetOuterOperatorNextTuple;
    private Tuple outerTuple = null;
    private Tuple innerTuple = null;
    private List<Tuple> innerTupleList = new ArrayList<>();
    // Cursor to maintain the position of tuple to be obtained from
    // innerTupleList.
    private Integer innerOperatorCursor = 0;
    private Schema outputSchema;

    private int cursor = CLOSED;
    
    private int resultCursor = -1;
    private int limit = Integer.MAX_VALUE;
    private int offset = 0;
    
    /**
     * Constructs a Join operator using a predicate which specifies the fields and
     *   constraints over which join happens.
     * 
     * @param joinPredicate
     */
    public Join(IJoinPredicate joinPredicate) {
        this.joinPredicate = joinPredicate;
    }

    @Override
    public void open() throws TextDBException {
        if (cursor != CLOSED) {
        	return;
        }
        
        if (innerOperator == null) {
            throw new DataFlowException("Inner Input Operator is not set.");
        }
        if (outerOperator == null) {
            throw new DataFlowException("Outer Input Operator is not set.");
        }
        
        // generate output schema from schema of inner and outer operator
        innerOperator.open();
        Schema innerOperatorSchema = innerOperator.getOutputSchema();
        innerOperator.close();
        
        outerOperator.open();
        Schema outerOperatorSchema = outerOperator.getOutputSchema();
        outerOperator.close();
        
        this.outputSchema = joinPredicate.generateOutputSchema(outerOperatorSchema, innerOperatorSchema);
        
        // load all tuples from inner operator into memory
        innerOperator.open();
        while ((innerTuple = innerOperator.getNextTuple()) != null) {
            innerTupleList.add(innerTuple);
        }
        innerOperator.close();

        // open outer operator
        outerOperator.open();

        shouldIGetOuterOperatorNextTuple = true;
        cursor = OPENED;
    }

    /**
     * Gets the next tuple which is a joint of two tuples which passed the
     * criteria set in the JoinPredicate. <br>
     * Example in JoinPredicate.java
     * 
     * @return nextTuple
     */
    @Override
    public Tuple getNextTuple() throws TextDBException {
    	if (cursor == CLOSED) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }

        if (resultCursor >= limit + offset - 1 || limit == 0){
            return null;
        }

        try {
            Tuple resultTuple = null;
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

    /*
     * Called from getNextTuple() method in order to obtain the next tuple 
     * that satisfies the predicate. 
     * 
     * It returns null if there's no more tuples.
     */
    protected  Tuple computeNextMatchingTuple() throws Exception {
        if (innerTupleList.isEmpty()) {
            return null;
        }
        
        Tuple nextTuple = null;
        while (nextTuple == null) {
            if (shouldIGetOuterOperatorNextTuple == true) {
                if ((outerTuple = outerOperator.getNextTuple()) == null) {
                    return null;
                }
                shouldIGetOuterOperatorNextTuple = false;
            }

            if (innerOperatorCursor <= innerTupleList.size() - 1) {
                innerTuple = innerTupleList.get(innerOperatorCursor);
                innerOperatorCursor++;
                if (innerOperatorCursor == innerTupleList.size()) {
                    innerOperatorCursor = 0;
                    shouldIGetOuterOperatorNextTuple = true;
                }
            }

            nextTuple = joinPredicate.joinTuples(outerTuple, innerTuple, outputSchema);
        }
        
    	return nextTuple;
    }

    @Override
    public void close() throws TextDBException {
    	if (cursor == CLOSED) {
            return;
        }

        try {
            outerOperator.close();
            // innerOperator.close(); already called in open()

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
        // Clear the inner tuple list from memory on close.
        innerTupleList.clear();
        cursor = CLOSED;
    }


    
    public void setInnerInputOperator(IOperator innerInputOperator) {
        this.innerOperator = innerInputOperator;
    }
    
    public IOperator getInnerInputOperator() {
        return this.innerOperator;
    }
    
    public void setOuterInputOperator(IOperator outerInputOperator) {
        this.outerOperator = outerInputOperator;
    }
    
    public IOperator getOuterInputOperator() {
        return this.outerOperator;
    }    

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
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
    
    public IJoinPredicate getPredicate() {
        return this.joinPredicate;
    }
}
