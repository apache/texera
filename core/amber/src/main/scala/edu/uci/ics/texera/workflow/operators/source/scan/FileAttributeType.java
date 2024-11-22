package edu.uci.ics.texera.workflow.operators.source.scan;

import com.fasterxml.jackson.annotation.JsonValue;
import edu.uci.ics.amber.core.tuple.AttributeType;

public enum FileAttributeType {
    STRING("string", AttributeType.STRING),
    SINGLE_STRING("single string", AttributeType.STRING),
    INTEGER("integer", AttributeType.INTEGER),
    LONG("long", AttributeType.LONG),
    DOUBLE("double", AttributeType.DOUBLE),
    BOOLEAN("boolean", AttributeType.BOOLEAN),
    TIMESTAMP("timestamp", AttributeType.TIMESTAMP),
    BINARY("binary", AttributeType.BINARY);


    private final String name;
    private final AttributeType type;

    FileAttributeType(String name, AttributeType type) {
        this.name = name;
        this.type = type;
    }

    @JsonValue
    public String getName() {
        return this.name;
    }

    public AttributeType getType() {
        return this.type;
    }

    @Override
    public String toString() {
        return this.getName();
    }

    public boolean isSingle() {
        return this == SINGLE_STRING || this == BINARY;
    }
}
