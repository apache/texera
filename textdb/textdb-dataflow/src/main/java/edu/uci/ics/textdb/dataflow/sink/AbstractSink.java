package edu.uci.ics.textdb.dataflow.sink;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;

/**
 * Created by chenli on 5/11/16.
 *
 * This abstract class leaves the @processOneTuple() function to
 * be implemented by the subclass based on the logic of handling each tuple
 * coming from the subtree.
 *
 */
public abstract class AbstractSink implements ISink {

    private IOperator inputOperator;

    public AbstractSink(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

    /**
     * @about Opens the child operator.
     */
    @Override
    public void open() throws Exception {
    	inputOperator.open();
    }
    
	public void setInputOperator(IOperator inputOperator) {
		this.inputOperator = inputOperator;
	}
	
	public IOperator getInputOperator() {
		return this.inputOperator;
	}

    @Override
    public void processTuples() throws Exception {

        ITuple nextTuple;

        while ((nextTuple = inputOperator.getNextTuple()) != null) {
            processOneTuple(nextTuple);
        }

    }

    /**
     *
     * @param nextTuple A tuple that needs to be processed during each iteration
     */
    protected abstract void processOneTuple(ITuple nextTuple) throws Exception;

    @Override
    public void close() throws Exception {
    	inputOperator.close();
    }
}
