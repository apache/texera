package edu.uci.ics.texera.workflow.operators.pythonUDF;

import com.fasterxml.jackson.annotation.JsonValue;

public enum PythonUDFType {
    MAP("map"),

    Filter("filter"),

    Training("training");


    private final String name;

    PythonUDFType(String name) {
        this.name = name;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }
}
