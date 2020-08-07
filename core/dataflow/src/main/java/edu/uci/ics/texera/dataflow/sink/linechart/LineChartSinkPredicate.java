package edu.uci.ics.texera.dataflow.sink.linechart;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.sink.VisualizationConstants;
import edu.uci.ics.texera.dataflow.sink.barchart.BarChartSink;
import edu.uci.ics.texera.dataflow.sink.visualization.LineChartEnum;

import java.util.List;
import java.util.Map;

public class LineChartSinkPredicate extends PredicateBase  {
    private String nameColumn;
    private List<String> dataColumn;
    private LineChartEnum lineChartEnum;


    @JsonCreator
    public LineChartSinkPredicate(
            @JsonProperty(value = PropertyNameConstants.NAME_COLUMN, required = true)
                    String nameColumn,
            @JsonProperty(value = PropertyNameConstants.DATA_COLUMN, required = true)
                    List<String> dataColumn,
            @JsonProperty(value = PropertyNameConstants.CHART_STYLE, required = true, defaultValue = VisualizationConstants.LINE)
                    LineChartEnum lineChartEnum) {
        this.nameColumn = nameColumn;
        this.dataColumn = dataColumn;
        this.lineChartEnum = lineChartEnum;
    }

    @JsonProperty(value = PropertyNameConstants.NAME_COLUMN)
    public String getNameColumn() {
        return this.nameColumn;
    }

    @JsonProperty(value = PropertyNameConstants.DATA_COLUMN)
    public List<String> getDataColumn() {
        return this.dataColumn;
    }

    @JsonProperty(value = PropertyNameConstants.CHART_STYLE)
    public LineChartEnum getLineChartEnum() {
        return this.lineChartEnum;
    }
    @Override
    public LineChartSink newOperator() {
        return new LineChartSink(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
                .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Line Chart")
                .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "View the result in line chart")
                .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.RESULT_GROUP)
                .build();
    }
}
