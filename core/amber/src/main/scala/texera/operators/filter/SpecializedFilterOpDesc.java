package texera.operators.filter;

import Engine.Common.tuple.texera.TexeraTuple;
import Engine.Operators.OpExecConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.Function1;
import scala.Serializable;
import scala.collection.immutable.Set;
import texera.common.TexeraConstraintViolation;
import texera.common.metadata.OperatorGroupConstants;
import texera.common.metadata.TexeraOperatorInfo;
import texera.common.operators.filter.TexeraFilterOpDesc;
import texera.common.operators.filter.TexeraFilterOpExecConfig;

import java.util.List;

public class SpecializedFilterOpDesc extends TexeraFilterOpDesc {

    @JsonProperty("predicates")
    @JsonPropertyDescription("multiple predicates in OR")
    public List<FilterPredicate> predicates;

    @Override
    public TexeraFilterOpExecConfig texeraOpExec() {
        return new TexeraFilterOpExecConfig(this.amberOperatorTag(),
                // must cast the lambda function to "(Function & Serializable)" in Java
                (Function1<TexeraTuple, Boolean> & Serializable) this::filter);
    }

    public boolean filter(TexeraTuple tuple) {
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
    public TexeraOperatorInfo texeraOperatorInfo() {
        return new TexeraOperatorInfo(
                "Filter",
                "performs a filter operation",
                OperatorGroupConstants.SEARCH_GROUP(),
                1, 1);
    }
}
