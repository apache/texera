package edu.uci.ics.texera.workflow.operators.aggregate;

import com.fasterxml.jackson.annotation.JsonValue;

public enum AggregationFunction {

    SUM("sum"),

    COUNT("count"),

    AVERAGE("average"),

    MIN("min"),

    MAX("max");

    private final String name;

    AggregationFunction(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

}
