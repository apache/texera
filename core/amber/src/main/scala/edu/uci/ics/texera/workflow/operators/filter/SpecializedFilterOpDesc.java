package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc;
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExecConfig;

import java.util.List;

public class SpecializedFilterOpDesc extends FilterOpDesc {

    @JsonProperty("predicates")
    @JsonPropertyDescription("multiple predicates in OR")
    public List<FilterPredicate> predicates;

    @Override
    public FilterOpExecConfig operatorExecutor() {
        return new FilterOpExecConfig(this.operatorIdentifier(),
                () -> new SpecializedFilterOpExec(this));
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Filter",
                "performs a filter operation",
                OperatorGroupConstants.SEARCH_GROUP(),
                1, 1);
    }
}
