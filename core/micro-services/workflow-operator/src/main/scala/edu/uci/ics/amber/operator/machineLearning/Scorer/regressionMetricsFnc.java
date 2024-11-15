package edu.uci.ics.amber.operator.machineLearning.Scorer;

import com.fasterxml.jackson.annotation.JsonValue;

public enum regressionMetricsFnc {
    mse("MSE"),
    rmse("RMSE"),
    mae("MAE"),
    r2("R2"),;

    private final String name;
    regressionMetricsFnc(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return this.name;
    }
}
