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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVRTrainer;

import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.ParamClass;
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.ParamSpec;

public enum SklearnAdvancedSVRParameters implements ParamClass {
    // Bounds are scikit-learn's own; see SVC for the shared ones.
    C(ParamSpec.of("C", "float", "1.0").withMinimum(">0")),
    kernel(ParamSpec.of("kernel", "str", "", "rbf", "linear", "poly", "sigmoid", "precomputed")),
    gamma(ParamSpec.of(
                    "gamma",
                    "(lambda value: value.strip() if value.strip() in (\"scale\", \"auto\") else float(value))",
                    "scale")
            .withPattern(ParamSpec.GammaPattern())),
    degree(ParamSpec.of("degree", "int", "3").withMinimum(">=0")),
    coef0(ParamSpec.of("coef0", "float", "0.0")),
    tol(ParamSpec.of("tol", "float", "0.001").withMinimum(">0")),
    shrinking(ParamSpec.of(
            "shrinking", "(lambda value: value.lower() == \"true\")", "", "true", "false")),
    verbose(ParamSpec.of(
            "verbose", "(lambda value: value.lower() == \"true\")", "", "false", "true")),
    epsilon(ParamSpec.of("epsilon", "float", "0.1").withMinimum(">=0")),
    cache_size(ParamSpec.of("cache_size", "int", "200").withMinimum(">0")),
    // -1 is SVR's own value for no iteration limit, which is also why the bound is -1 and not
    // zero: the sentinel has to remain reachable.
    max_iter(ParamSpec.of("max_iter", "int", "-1").withMinimum(">=-1"));

    private final ParamSpec paramSpec;

    SklearnAdvancedSVRParameters(ParamSpec paramSpec) {
        this.paramSpec = paramSpec;
    }

    @Override
    public ParamSpec paramSpec() {
        return this.paramSpec;
    }
}
