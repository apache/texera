package edu.uci.ics.texera.dataflow.sink.visualization;

import com.fasterxml.jackson.annotation.JsonValue;
import edu.uci.ics.texera.dataflow.sink.VisualizationConstants;

public enum LineChartEnum {
    LINE(VisualizationConstants.LINE),
    SPLINE(VisualizationConstants.SPLINE);

    private final String style;
    LineChartEnum(String style) {
        this.style = style;
    }

    @JsonValue
    public String getChartStyle() {
        return this.style;
    }
}
