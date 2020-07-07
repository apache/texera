package edu.uci.ics.texera.dataflow.sink.piechart;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import java.util.Map;

public class PieChartSinkPredicate extends PredicateBase {
    private String nameColumn;
    private String dataColumn;
    private Double ratio;

    @JsonCreator
    public PieChartSinkPredicate(
        @JsonProperty(value = PropertyNameConstants.NAME_COLUMN, required = true)
            String nameColumn,
        @JsonProperty(value = PropertyNameConstants.DATA_COLUMN, required =  true)
            String dataColumn,
        @JsonProperty(value = PropertyNameConstants.PRUNE_RATIO, required = true, defaultValue = "0.9")
            Double ratio) {

        this.nameColumn = nameColumn;
        this.dataColumn = dataColumn;
        this.ratio = ratio;

    }

    @JsonProperty(value = PropertyNameConstants.NAME_COLUMN)
    public String getNameColumn() {
        return this.nameColumn;
    }

    @JsonProperty(value = PropertyNameConstants.DATA_COLUMN)
    public String getDataColumn() {
        return this.dataColumn;
    }

    @JsonProperty(value = PropertyNameConstants.PRUNE_RATIO)
    public Double getPruneRatio() { return this.ratio; }

    @Override
    public PieChartSink newOperator() {
        return new PieChartSink(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Pie Chart")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "View the result in pie chart")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.RESULT_GROUP)
            .build();
    }


}
