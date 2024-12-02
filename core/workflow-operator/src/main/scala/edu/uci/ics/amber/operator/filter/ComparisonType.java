package edu.uci.ics.amber.operator.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ComparisonType {
    EQUAL_TO("="),

    GREATER_THAN(">"),

    GREATER_THAN_OR_EQUAL_TO(">="),

    LESS_THAN("<"),

    LESS_THAN_OR_EQUAL_TO("<="),

    NOT_EQUAL_TO("!="),

    IS_NULL("is null"),

    IS_NOT_NULL("is not null");

    private final String name;

    private ComparisonType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }

    // Handle custom deserialization for enum
    @JsonCreator
    public static ComparisonType fromString(String value) {
        for (ComparisonType type : ComparisonType.values()) {
            if (type.name.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown comparison type: " + value);
    }
}