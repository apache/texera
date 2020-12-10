package edu.uci.ics.texera.workflow.operators.typeCasting;
import com.fasterxml.jackson.annotation.JsonValue;

public enum CastingType {
    INTEGER("Int"),

    DOUBLE("Double"),

    STRING("Str"),

    BOOLEAN("Bool");

    private final String name;

    private CastingType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }
}
