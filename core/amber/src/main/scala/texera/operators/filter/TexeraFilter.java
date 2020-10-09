package texera.operators.filter;

import Engine.Common.tuple.Tuple;
import Engine.Common.Constants;
import Engine.Operators.Common.Filter.FilterGeneralMetadata;
import Engine.Operators.OperatorMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.Function1;
import scala.Serializable;
import scala.collection.immutable.Set;
import texera.common.TexeraConstraintViolation;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.common.workflow.TexeraOperator;
import texera.common.workflow.common.FilterOpDesc;

import java.util.List;

public class TexeraFilter extends FilterOpDesc {

    @JsonProperty("predicates")
    @JsonPropertyDescription("multiple predicates in OR")
    public List<FilterPredicate> predicates;

    @Override
    public OperatorMetadata amberOperator() {
        return new FilterGeneralMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                (Function1<Tuple, Boolean> & Serializable) t -> {
                    boolean satisfy = false;
                    for (FilterPredicate predicate: predicates) {
                        satisfy = satisfy || predicate.evaluate(t, this.context());
                    }
                    return satisfy;
                });
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
