package edu.uci.ics.texera.workflow.operators.typeCasting;
import com.fasterxml.jackson.annotation.JsonValue;

public enum CastingType {
    STRING("string"),
    INTEGER("integer"),
    DOUBLE("double"),
    BOOLEAN("boolean");

    private final String name;

    CastingType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }
}
