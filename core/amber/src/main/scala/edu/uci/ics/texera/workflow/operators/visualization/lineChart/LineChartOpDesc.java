package edu.uci.ics.texera.workflow.operators.visualization.lineChart;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * LineChart is a visualization operator that can be used to get tuples for line chart.
 * It returns tuples with data name (String) and at least one number (Integer or Double).
 * @author Mingji Han, Xiaozhen Liu
 *
 */
public class LineChartOpDesc extends VisualizationOperator {
    @JsonProperty(value = "name column", required = true)
    @JsonPropertyDescription("column of name (for x-axis)")
    public String nameColumn;

    @JsonProperty(value = "data column(s)", required = true)
    @JsonPropertyDescription("column(s) of data (for y-axis)")
    public List<String> dataColumns;

    @JsonProperty(value = "chart style", required = true)
    public LineChartEnum lineChartEnum;

    @Override
    public String chartType() {
        return lineChartEnum.getChartStyle();
    }

    @Override
    public OneToOneOpExecConfig operatorExecutor() {
        if (nameColumn == null) {
            throw new RuntimeException("line chart: name column is null");
        }
        if (dataColumns == null || dataColumns.isEmpty()) {
            throw new RuntimeException("line chart: data column is null or empty");
        }
        return new OneToOneOpExecConfig(operatorIdentifier(), worker -> new LineChartOpExec(this));
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Line Chart",
                "View the result in line chart",
                OperatorGroupConstants.VISUALIZATION_GROUP(),
                1, 1);
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(schemas[0].getAttribute(nameColumn));
        for(String s : dataColumns) attributes.add(schemas[0].getAttribute(s));
        return Schema.newBuilder().add(attributes).build();
    }
}
