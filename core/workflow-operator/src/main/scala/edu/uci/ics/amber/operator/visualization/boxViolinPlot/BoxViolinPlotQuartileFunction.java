package edu.uci.ics.amber.operator.visualization.boxViolinPlot;

import com.fasterxml.jackson.annotation.JsonValue;

public enum BoxViolinPlotQuartileFunction {
    LINEAR("linear"),
    INCLUSIVE("inclusive"),
    EXCLUSIVE("exclusive");
    private final String quartiletype;

    BoxViolinPlotQuartileFunction(String quartiletype) {
        this.quartiletype = quartiletype;
    }

    @JsonValue
    public String getQuartiletype() {
        return this.quartiletype;
    }
}