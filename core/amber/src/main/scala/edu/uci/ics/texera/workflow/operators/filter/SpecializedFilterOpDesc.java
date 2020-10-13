package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.TexeraOperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.filter.TexeraFilterOpDesc;
import edu.uci.ics.texera.workflow.common.operators.filter.TexeraFilterOpExecConfig;

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
