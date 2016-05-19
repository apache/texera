package edu.uci.ics.textdb.dataflow.fuzzytokenmatcher;
import java.util.List;

import edu.uci.ics.textdb.api.common.IField;
/**
 *  @author Parag Saraogi, Varun Bharill
 *  This class takes a predicate as input, constructs the source operator, executes the query
 *  and returns the results iteratively
 */
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

public class FuzzyTokenMatcher implements IOperator{
    private final FuzzyTokenPredicate predicate;
    private ISourceOperator sourceOperator;

    public FuzzyTokenMatcher(IPredicate predicate) {
    	this.predicate = (FuzzyTokenPredicate)predicate;
    	DataReaderPredicate dataReaderPredicate = this.predicate.getDataReaderPredicate();
    	this.sourceOperator = new IndexBasedSourceOperator(dataReaderPredicate);
    }
    
    @Override
    public void open() throws DataFlowException {
    	return;
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
	return null;
    }

    @Override
    public void close() throws DataFlowException {
	return;
    }
}
