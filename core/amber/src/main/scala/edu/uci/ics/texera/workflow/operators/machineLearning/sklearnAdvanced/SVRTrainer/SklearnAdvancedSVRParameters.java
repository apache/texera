package edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.SVRTrainer;

import edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.base.ParamClass;

public enum SklearnAdvancedSVRParameters implements ParamClass {
    C("C", "float"),
    kernel("kernel", "str"),
    gamma("gamma", "float"),
    degree("degree", "int"),
    coef0("coef0", "float"),
    tol("tol", "float"),
    probability("shrinking", "'True'=="),
    verbose("verbose", "'True'=="),
    epsilon("epsilon", "float"),
    cache_size("cache_size", "int"),
    max_iter("max_iter", "int")
    ;

    private final String name;
    private final String type;

    SklearnAdvancedSVRParameters(String name, String type) {
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
