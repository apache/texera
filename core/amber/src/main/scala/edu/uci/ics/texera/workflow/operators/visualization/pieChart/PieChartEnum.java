package edu.uci.ics.texera.workflow.operators.visualization.pieChart;

import com.fasterxml.jackson.annotation.JsonValue;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants;

public enum PieChartEnum {
    PIE(VisualizationConstants.PIE),
    DONUT(VisualizationConstants.DONUT);

    private final String style;
    PieChartEnum(String style) {
        this.style = style;
    }

    @JsonValue
    public String getChartStyle() {
        return this.style;
    }
}
