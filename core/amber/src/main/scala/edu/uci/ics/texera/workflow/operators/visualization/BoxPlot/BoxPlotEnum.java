package edu.uci.ics.texera.workflow.operators.visualization.boxPlot;

import com.fasterxml.jackson.annotation.JsonValue;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants;

public enum BoxPlotEnum {


    LINEAR_BOXPLOT(VisualizationConstants.LINEAR_BOXPLOT),
    INCLUSIVE_BOXPLOT(VisualizationConstants.INCLUSIVE_BOXPLOT),
    EXCLUSIVE_BOXPLOT(VisualizationConstants.EXCLUSIVE_BOXPLOT);
    private final String quertiletype;

    BoxPlotEnum(String quertiletype) {
        this.quertiletype = quertiletype;
    }

    @JsonValue
    public String getQuertiletype() {
        return this.quertiletype;
    }


}
