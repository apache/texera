package edu.uci.ics.textdb.dataflow.keywordmatch;

import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;

/**
 * Created by akshay on 4/17/2016.
 */
public class KeywordMatcher implements IOperator {
    private final IPredicate predicate;
    private ISourceOperator sourceOperator;
    private Query luceneQuery;

    public KeywordMatcher(IPredicate predicate, ISourceOperator sourceOperator) {
        this.predicate = predicate;
        this.sourceOperator = sourceOperator;
        //TODO build the luceneQuery by given keyword.
    }

    @Override
    public void open() throws DataFlowException {
        try {
            sourceOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {

        try {
        	while(true){
	            ITuple sourceTuple = sourceOperator.getNextTuple();
	            if(sourceTuple == null){
	                return null;
	            }
	            if (predicate.satisfy(sourceTuple)) {
	                return sourceTuple;
	            }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }

    }

    @Override
    public void close() throws DataFlowException {
        try {
            sourceOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
