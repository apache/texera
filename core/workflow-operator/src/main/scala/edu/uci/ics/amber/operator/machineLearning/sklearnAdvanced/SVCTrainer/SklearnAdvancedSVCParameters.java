package edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer;

import edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.base.ParamClass;

public enum SklearnAdvancedSVCParameters implements ParamClass {
    C("C", "float"),
    kernel("kernel", "str"),
    gamma("gamma", "float"),
    degree("degree", "int"),
    coef0("coef0", "float"),
    tol("tol", "float"),
    probability("probability", "(lambda value: value.lower() == \"true\")");

    private final String name;
    private final String type;

    SklearnAdvancedSVCParameters(String name, String type) {
        this.name = name;
        this.type  = type;
    }

    public String getType() {
        return this.type;
    }

    public String getName() {
        return this.name;
    }
}
