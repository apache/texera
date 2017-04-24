package edu.uci.ics.textdb.exp.sink.tuple;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;

public class TupleSinkPredicate extends PredicateBase {

    @Override
    public IOperator newOperator() {
        return new TupleSink();
    }

}
