package edu.uci.ics.texera.workflow.operators.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc;

import java.util.List;

public class SpecializedFilterOpDesc extends FilterOpDesc {

    @JsonProperty(value = "predicates", required = true)
    @JsonPropertyDescription("multiple predicates in OR")
    public List<FilterPredicate> predicates;

    @Override
    public OneToOneOpExecConfig operatorExecutor() {
        return new OneToOneOpExecConfig(this.operatorIdentifier(),
                worker -> new SpecializedFilterOpExec(this));
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
