package edu.uci.ics.amber.operator.visualization.rangeSlider;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RangeSliderHandleDuplicateFunction  {
    MEAN("mean"),
    SUM("sum"),
    NOTHING("nothing");
    private final String duplicateType;

    RangeSliderHandleDuplicateFunction(String duplicateType) {
        this.duplicateType = duplicateType;
    }
    @JsonValue
    public String getFunctionType() {
        return this.duplicateType;
    }
}