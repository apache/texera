/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer;

import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.ParamClass;
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.ParamSpec;

public enum SklearnAdvancedSVCParameters implements ParamClass {
    // Bounds are scikit-learn's own, whose ranges are open at zero for the two below and
    // closed for degree.
    C(ParamSpec.of("C", "float", "1.0").withMinimum(">0")),
    kernel(ParamSpec.of("kernel", "str", "", "rbf", "linear", "poly", "sigmoid", "precomputed")),
    gamma(ParamSpec.of(
                    "gamma",
                    "(lambda value: value.strip() if value.strip() in (\"scale\", \"auto\") else float(value))",
                    "scale")
            .withPattern(ParamSpec.GammaPattern())),
    degree(ParamSpec.of("degree", "int", "3").withMinimum(">=0")),
    // coef0 is the one parameter here with no bound at either end.
    coef0(ParamSpec.of("coef0", "float", "0.0")),
    tol(ParamSpec.of("tol", "float", "0.001").withMinimum(">0")),
    probability(ParamSpec.of(
            "probability", "(lambda value: value.lower() == \"true\")", "", "false", "true"));

    private final ParamSpec paramSpec;

    SklearnAdvancedSVCParameters(ParamSpec paramSpec) {
        this.paramSpec = paramSpec;
    }

    @Override
    public ParamSpec paramSpec() {
        return this.paramSpec;
    }
}
