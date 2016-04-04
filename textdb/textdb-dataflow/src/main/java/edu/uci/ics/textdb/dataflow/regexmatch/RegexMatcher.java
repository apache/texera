package edu.uci.ics.textdb.dataflow.regexmatch;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import org.apache.lucene.search.Query;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcher implements IOperator {
    private final IPredicate predicate;
    private ISourceOperator sourceOperator;
    private Query luceneQuery;

    public RegexMatcher(IPredicate predicate, ISourceOperator sourceOperator) {
        this.predicate = predicate;
        this.sourceOperator = sourceOperator;
        //TODO build the luceneQuery by given regex.
    }

    @Override
    public void open() {

    }

    public ITuple getNextTuple() {

        ITuple sourceTuple = sourceOperator.getNextTuple();
        if (predicate.satisfy(sourceTuple)) {
            return sourceTuple;
        } else {
            return getNextTuple();
        }

    }

    @Override
    public void close() {

    }
}
