package edu.uci.ics.texera.workflow.operators.filter;

import edu.uci.ics.amber.core.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec;
import scala.Function1;

import java.io.Serializable;

public class SpecializedFilterOpExec extends FilterOpExec {

    private final java.util.List<FilterPredicate> predicates;

    public SpecializedFilterOpExec(java.util.List<FilterPredicate> predicates) {
        this.predicates = predicates;
        setFilterFunc((Function1<Tuple, Boolean> & Serializable) this::filterFunc);
    }

    public Boolean filterFunc(Tuple tuple) {
        return predicates.stream().anyMatch(predicate -> predicate.evaluate(tuple));
    }

}
