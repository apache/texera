package edu.uci.ics.amber.operator.visualization.rangeSlider;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RangeSliderHandleDuplicateFunction  {
    MEAN("mean"),
    SUM("sum"),
    NOTHING("nothing");
    private final String functionType;

    RangeSliderHandleDuplicateFunction(String functionType) {
        this.functionType = functionType;
    }
    @JsonValue
    public String getFunctionType() {
        return this.functionType;
    }
}