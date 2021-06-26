/**
 *
 */
package edu.uci.ics.texera.dataflow.aggregator;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author avinash
 */
public enum AggregationType {
    MIN("min"),
    MAX("max"),
    AVERAGE("average"),
    SUM("sum"),
    COUNT("count");

    private final String name;

    private AggregationType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}