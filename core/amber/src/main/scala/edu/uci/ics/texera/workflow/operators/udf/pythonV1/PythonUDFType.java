package edu.uci.ics.texera.workflow.operators.udf.pythonV1;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.List;

public enum PythonUDFType {
    Map("map"),

    Filter("filter"),

    SupervisedTraining("supervised_training"),

    UnsupervisedTraining("unsupervised_training");

    public static List<PythonUDFType> supportsParallel = Arrays.asList(Map, Filter);

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
