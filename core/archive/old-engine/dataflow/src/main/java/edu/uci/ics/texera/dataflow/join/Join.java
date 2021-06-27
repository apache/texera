package edu.uci.ics.texera.dataflow.join;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;


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
 * @author Zuozhi Wang
 *
 */
public class Join implements IOperator {

    private IOperator innerOperator;
    private IOperator outerOperator;
    private IJoinPredicate joinPredicate;
    
    private List<Tuple> innerTupleList = null;
    // Cursor to maintain the position of tuple to be obtained from innerTupleList.
    private Integer innerTupleListCursor = 0;
    private Tuple currentOuterTuple;
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
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
        	return;
        }
        
        if (innerOperator == null) {
            throw new DataflowException("Inner Input Operator is not set.");
        }
        if (outerOperator == null) {
            throw new DataflowException("Outer Input Operator is not set.");
        }
        
        // generate output schema from schema of inner and outer operator
        innerOperator.open();
        Schema innerOperatorSchema = innerOperator.getOutputSchema();
        
        outerOperator.open();
        Schema outerOperatorSchema = outerOperator.getOutputSchema();
        
        this.outputSchema = joinPredicate.generateOutputSchema(innerOperatorSchema, outerOperatorSchema);

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
    public Tuple getNextTuple() throws TexeraException {
    	if (cursor == CLOSED) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
    	
        // load all tuples from inner operator into memory in the first time
    	if (innerTupleList == null) {
    	    innerTupleList = new ArrayList<>();
    	    Tuple tuple;
            while ((tuple = innerOperator.getNextTuple()) != null) {
                innerTupleList.add(tuple);
            }
    	}
    	
    	// load the first outer tuple
    	currentOuterTuple = outerOperator.getNextTuple();
    	
    	// return null if either
    	//   inner tuple list is empty, or
    	//   all outer tuples have been consumed
    	if (innerTupleList.isEmpty() || currentOuterTuple == null) {
    	    return null;
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
            throw new DataflowException(e.getMessage(), e);
        }
    }

    /*
     * Called from getNextTuple() method in order to obtain the next tuple 
     * that satisfies the predicate. 
     * 
     * It returns null if there's no more tuples.
     */
    private Tuple computeNextMatchingTuple() throws Exception {
        if (innerTupleList.isEmpty()) {
            return null;
        }
        
        Tuple nextTuple = null;
        while (nextTuple == null) {
            // if reach the end of inner tuple list
            if (innerTupleListCursor >= innerTupleList.size()) {
                // get next outer tuple
                currentOuterTuple = outerOperator.getNextTuple();
                if (currentOuterTuple == null) {
                    return null;
                }
                // reset cursor if outerTuple is not null
                innerTupleListCursor = 0;
            }
            // compute next tuple
            nextTuple = joinPredicate.joinTuples(
                    innerTupleList.get(innerTupleListCursor), currentOuterTuple, outputSchema);
            // increment cursor
            innerTupleListCursor++;
        }
        
    	return nextTuple;
    }

    @Override
    public void close() throws TexeraException {
    	if (cursor == CLOSED) {
            return;
        }

        try {
            innerOperator.close();
            outerOperator.close();
        } catch (Exception e) {
            throw new DataflowException(e.getMessage(), e);
        }
        
        // Set the inner tuple list back to null on close.
        innerTupleList = null;
        innerTupleListCursor = 0;
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

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 2)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 2, inputSchema.length));

        return joinPredicate.generateOutputSchema(inputSchema[0], inputSchema[1]);
    }
}
