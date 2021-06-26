package edu.uci.ics.texera.textql.planbuilder.beans;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PredicateBase;

public class PassThroughPredicate extends PredicateBase {
    
    public PassThroughPredicate(String id) {
        this.setID(id);
    }

    @Override
    public IOperator newOperator() {
        throw new UnsupportedOperationException("not implemented");
    }

}
