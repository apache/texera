package texera.operators.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
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
    public TexeraFilterOpExecConfig texeraOperatorExecutor() {
        return new TexeraFilterOpExecConfig(this.operatorIdentifier(),
                () -> new SpecializedFilterOpExec(this));
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
