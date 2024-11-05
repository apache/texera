package edu.uci.ics.amber.operator.visualization.hierarchychart;

import com.fasterxml.jackson.annotation.JsonValue;

public enum HierarchyChartType {
    TREEMAP("treemap"),
    SUNBURSTCHART("sunburst");

    private final String plotlyExpressApiName;

    HierarchyChartType(String plotlyExpressApiName) {
        this.plotlyExpressApiName = plotlyExpressApiName;
    }

    // use the name string instead of enum string in JSON\
    @JsonValue
    public String getPlotlyExpressApiName() {
        return this.plotlyExpressApiName;
    }
}
