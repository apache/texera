package texera.operators.filter;

import Engine.Common.tuple.Tuple;
import Engine.Operators.Common.Filter.FilterOpExecConfig;
import Engine.Operators.OpExecConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.collection.immutable.Set;
import texera.common.TexeraConstraintViolation;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.common.FilterOpDesc;

import java.util.List;

public class TexeraFilterOpDesc extends FilterOpDesc {

    @JsonProperty("predicates")
    @JsonPropertyDescription("multiple predicates in OR")
    public List<FilterPredicate> predicates;

    @Override
    public OpExecConfig amberOperator() {
        return new FilterOpExecConfig(this.amberOperatorTag(), this::filter);
    }

    public boolean filter(Tuple tuple) {
        boolean satisfy = false;
        for (FilterPredicate predicate: predicates) {
            satisfy = satisfy || predicate.evaluate(tuple, context());
        }
        return satisfy;
    }

    @Override
    public Set<TexeraConstraintViolation> validate() {
        return super.validate();
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Filter",
                "performs a filter operation",
                OperatorGroupConstants.SEARCH_GROUP(),
                1, 1);
    }
}
