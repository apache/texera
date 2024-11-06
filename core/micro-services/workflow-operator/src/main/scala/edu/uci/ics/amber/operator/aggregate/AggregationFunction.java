package edu.uci.ics.amber.operator.aggregate;

import com.fasterxml.jackson.annotation.JsonValue;

public enum AggregationFunction {

    SUM("sum"),

    COUNT("count"),

    AVERAGE("average"),

    MIN("min"),

    MAX("max"),

    CONCAT("concat");

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
