
package edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer;

import edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.base.ParamClass;

public enum SklearnAdvancedKNNParameters implements ParamClass {
    n_neighbors("n_neighbors", "int"),
    p("p", "int"),
    weights("weights", "str"),
    algorithm("algorithm", "str"),
    leaf_size("leaf_size", "int"),
    metric("metric", "int"),
    metric_params("metric_params", "str");

    private final String name;
    private final String type;

    SklearnAdvancedKNNParameters(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public String getName() {
        return this.name;
    }
}
