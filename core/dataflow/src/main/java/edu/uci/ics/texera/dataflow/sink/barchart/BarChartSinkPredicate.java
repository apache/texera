package edu.uci.ics.texera.dataflow.sink.barchart;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import java.util.List;
import java.util.Map;

public class BarChartSinkPredicate extends PredicateBase {

    private String nameColumn;
    private  List<String> dataColumn;

    @JsonCreator
    public BarChartSinkPredicate(
        @JsonProperty(value = PropertyNameConstants.NAME_COLUMN, required = true)
        String nameColumn,
        @JsonProperty(value = PropertyNameConstants.DATA_COLUMN, required = true)
            List<String> dataColumn) {
        this.nameColumn = nameColumn;
        this.dataColumn = dataColumn;
    }

    @JsonProperty(value = PropertyNameConstants.NAME_COLUMN)
    public String getNameColumn() {
        return this.nameColumn;
    }

    @JsonProperty(value = PropertyNameConstants.DATA_COLUMN)
    public List<String> getDataColumn() {
        return this.dataColumn;
    }

    @Override
    public BarChartSink newOperator() {
        return new BarChartSink(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Bar Chart")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "View the result in bar chart")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.RESULT_GROUP)
            .build();
    }
}
