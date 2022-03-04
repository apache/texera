package edu.uci.ics.texera.workflow.operators.filter;

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.Function1;
import scala.Serializable;

import java.util.List;

public class SpecializedFilterOpExec extends FilterOpExec {

    private final SpecializedFilterOpDesc opDesc;

    public SpecializedFilterOpExec(SpecializedFilterOpDesc opDesc) {
        this.opDesc = opDesc;
        setFilterFunc(
                // must cast the lambda function to "(Function & Serializable)" in Java
                (Function1<Tuple, Boolean> & Serializable) this::filterFunc);
    }

    public Boolean filterFunc(Tuple tuple) {
        return opDesc.predicates
                .stream().anyMatch(predicate -> predicate.evaluate(tuple, opDesc.context()));
    }

}
