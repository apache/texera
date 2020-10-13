package texera.operators.filter;

import scala.Function1;
import scala.Serializable;
import texera.common.operators.filter.TexeraFilterOpExec;
import texera.common.tuple.TexeraTuple;

public class SpecializedFilterOpExec extends TexeraFilterOpExec {

    private final SpecializedFilterOpDesc opDesc;

    public SpecializedFilterOpExec(SpecializedFilterOpDesc opDesc) {
        this.opDesc = opDesc;
        setFilterFunc(
                // must cast the lambda function to "(Function & Serializable)" in Java
                (Function1<TexeraTuple, Boolean> & Serializable) this::filterFunc);
    }

    public Boolean filterFunc(TexeraTuple tuple) {
        boolean satisfy = false;
        for (FilterPredicate predicate: opDesc.predicates) {
            satisfy = satisfy || predicate.evaluate(tuple, opDesc.context());
        }
        return satisfy;
    };

}
