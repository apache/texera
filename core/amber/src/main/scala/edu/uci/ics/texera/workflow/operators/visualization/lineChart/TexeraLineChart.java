package texera.operators.visualization.lineChart;

import Engine.Common.AmberTuple.Tuple;
import Engine.Common.Constants;
import Engine.Operators.Common.Map.MapMetadata;
import Engine.Operators.OperatorMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import scala.Function1;
import scala.Serializable;
import texera.common.schema.OperatorGroupConstants;
import texera.common.schema.TexeraOperatorDescription;
import texera.operators.visualization.VisualizationOperator;

import java.util.ArrayList;
import java.util.List;

public class TexeraLineChart extends VisualizationOperator {
    @JsonProperty("name column")
    @JsonPropertyDescription("column of name (for x-axis)")
    public String nameColumn;

    @JsonProperty("data column(s)")
    @JsonPropertyDescription("column(s) of data (for y-axis)")
    public List<String> dataColumns;

    @JsonProperty("chart style")
    public LineChartEnum lineChartEnum;

    @Override
    public OperatorMetadata amberOperator() {
        if (nameColumn == null) {
            throw new RuntimeException("line chart: name column is null");
        }
        if (dataColumns == null || dataColumns.isEmpty()) {
            throw new RuntimeException("line chart: data column is null or empty");
        }
        int nameColumnIndex = this.context().fieldIndexMapping(nameColumn);
        List<Integer> dataColumnIndices= new ArrayList<>();
        for (String d : dataColumns) {
            dataColumnIndices.add(this.context().fieldIndexMapping(d));
        }
        return new MapMetadata(
                this.amberOperatorTag(), Constants.defaultNumWorkers(),
                (Function1<Tuple, Tuple> & Serializable) t -> {
                    String name = t.get(nameColumnIndex).toString();
                    List<Object> tupleData = new ArrayList<>();
                    tupleData.add(name);
                    for (Integer i : dataColumnIndices) {
                        Object o = t.get(i);
                        if (o.getClass() != Double.class && o.getClass() != Integer.class) {
                            if (o.getClass() == String.class) {
                                Double dataValue = Double.valueOf((String) o);
                                tupleData.add(dataValue);
                            }
                            else throw new RuntimeException("line chart: data column is not number type");
                        }
                        else tupleData.add(o);
                    }
                    return Tuple.fromJavaList(tupleData);
                });
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Line Chart",
                "View the result in line chart",
                OperatorGroupConstants.VISUALIZATION_GROUP(),
                1, 1);
    }

    @Override
    public String chartType() {
        return lineChartEnum.getChartStyle();
    }
}
