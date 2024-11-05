package edu.uci.ics.amber.operator.machineLearning.Scorer;

import com.fasterxml.jackson.annotation.JsonValue;

public enum classificationMetricsFnc {
    accuracy("Accuracy"),
    precisionScore ("Precision Score"),
    recallScore("Recall Score"),
    f1Score("F1 Score"),;

    private final String name;
    classificationMetricsFnc(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return this.name;
    }
}