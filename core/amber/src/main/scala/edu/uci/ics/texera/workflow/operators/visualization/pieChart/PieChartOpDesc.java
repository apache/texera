package edu.uci.ics.texera.workflow.operators.visualization.pieChart;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

/**
 * PieChart is a visualization operator that can be used to get tuples for pie chart.
 * PieChart returns tuples with name of data (String) and a number (the input can be int, double or String number,
 * but the output will be Double).
 * Note here we assume every name has exactly one data.
 * @author Mingji Han, Xiaozhen Liu
 *
 */
public class PieChartOpDesc extends VisualizationOperator {

    @JsonProperty(value = "name column", required = true)
    @AutofillAttributeName
    public String nameColumn;

    @JsonProperty(value = "data column", required = true)
    @AutofillAttributeName
    public String dataColumn;

    @JsonProperty(value = "prune ratio", required = true)
    public Double pruneRatio;

    @JsonProperty(value = "chart style", required = true)
    public PieChartEnum pieChartEnum;

    @Override
    public String chartType() {
        return pieChartEnum.getChartStyle();
    }

    @Override
    public OpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        if (nameColumn == null) {
            throw new RuntimeException("pie chart: name column is null");
        }
        if (dataColumn == null) {
            throw new RuntimeException("pie chart: data column is null");
        }
        if (pruneRatio < 0 || pruneRatio > 1) throw new RuntimeException("pie chart: prune ratio not within in [0,1]");

        return new PieChartOpExecConfig(this.operatorIdentifier(), Constants.defaultNumWorkers(), nameColumn, dataColumn, pruneRatio);
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Pie Chart",
                "View the result in pie chart",
                OperatorGroupConstants.VISUALIZATION_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        AttributeType dataType = schemas[0].getAttribute(dataColumn).getType();
        if (dataType != AttributeType.DOUBLE && dataType != AttributeType.INTEGER && dataType != AttributeType.STRING) {
            throw new RuntimeException("pie chart: data must be number");
        }

        return Schema.newBuilder().add(
                schemas[0].getAttribute(nameColumn),
                schemas[0].getAttribute(dataColumn)
        ).build();
    }
}
