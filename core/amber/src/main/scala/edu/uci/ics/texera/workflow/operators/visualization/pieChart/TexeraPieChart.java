package texera.operators.visualization.pieChart;

import Engine.Common.Constants;
import Engine.Operators.OperatorMetadata;
import Engine.Operators.Visualization.PieChart.PieChartMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.operators.visualization.VisualizationOperator;

public class TexeraPieChart extends VisualizationOperator {

    @JsonProperty("name column")
    public String nameColumn;

    @JsonProperty("data column")
    public String dataColumn;

    @JsonProperty("prune ratio")
    public Double pruneRatio;

    @JsonProperty("chart style")
    public PieChartEnum pieChartEnum;

    @Override
    public OperatorMetadata amberOperator() {
        if (nameColumn == null) {
            throw new RuntimeException("pie chart: name column is null");
        }
        if (dataColumn == null) {
            throw new RuntimeException("pie chart: data column is null");
        }
        if (pruneRatio < 0 || pruneRatio > 1) throw new RuntimeException("pie chart: prune ratio not within in [0,1]");
        int nameColumnIndex = this.context().fieldIndexMapping(nameColumn);
        int dataColumnIndex = this.context().fieldIndexMapping(dataColumn);
        return new PieChartMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                nameColumnIndex, dataColumnIndex, pruneRatio);
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Pie Chart",
                "View the result in pie chart",
                OperatorGroupConstants.VISUALIZATION_GROUP(),
                1, 1);
    }

    @Override
    public String chartType() {
        return pieChartEnum.getChartStyle();
    }
}
