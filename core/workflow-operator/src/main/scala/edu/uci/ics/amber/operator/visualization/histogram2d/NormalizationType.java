package edu.uci.ics.amber.operator.visualization.histogram2d;

import com.fasterxml.jackson.annotation.JsonValue;

public enum NormalizationType {
    NONE(""),
    DENSITY("density"),
    PROBABILITY("probability"),
    PERCENT("percent");

    private final String value;

    NormalizationType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return this.value;
    }
}