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

public enum SklearnAdvancedSVRParameters implements ParamClass {
    // Bounds are scikit-learn's own; see SVC for the shared ones.
    C("C", "float", "1.0") { @Override public String getMinimum() { return ">0"; } },
    kernel("kernel", "str", "", "rbf", "linear", "poly", "sigmoid", "precomputed"),
    // Same converter and shape as SVC's gamma -- see there for why each is spelled this way.
    gamma(
            "gamma",
            "(lambda value: value.strip() if value.strip() in (\"scale\", \"auto\") else float(value))",
            "scale") {
        @Override
        public String getPattern() {
            return "^\\s*(?:scale|auto|[-+]?(?:(?:[0-9]+(?:_[0-9]+)*)?\\.(?:[0-9]+(?:_[0-9]+)*)"
                    + "|(?:[0-9]+(?:_[0-9]+)*)\\.?)(?:[eE][-+]?[0-9]+(?:_[0-9]+)*)?)\\s*$";
        }
    },
    degree("degree", "int", "3") { @Override public String getMinimum() { return ">=0"; } },
    coef0("coef0", "float", "0.0"),
    tol("tol", "float", "0.001") { @Override public String getMinimum() { return ">0"; } },
    probability("shrinking", "(lambda value: value.lower() == \"true\")", "", "true", "false"),
    verbose("verbose", "(lambda value: value.lower() == \"true\")", "", "false", "true"),
    epsilon("epsilon", "float", "0.1") { @Override public String getMinimum() { return ">=0"; } },
    cache_size("cache_size", "int", "200") { @Override public String getMinimum() { return ">0"; } },
    // -1 is SVR's own value for no iteration limit, which is also why the bound is -1 and not
    // zero: the sentinel has to remain reachable.
    max_iter("max_iter", "int", "-1") { @Override public String getMinimum() { return ">=-1"; } };

    private final String name;
    private final String type;
    private final String sampleValue;
    private final String[] allowedValues;

    SklearnAdvancedSVRParameters(
            String name, String type, String sampleValue, String... allowedValues) {
        this.name = name;
        this.type = type;
        this.sampleValue = sampleValue;
        this.allowedValues = allowedValues;
    }

    public String getType() {
        return this.type;
    }

    public String getName() {
        return this.name;
    }

    public String getSampleValue() {
        return this.sampleValue;
    }

    public String[] getAllowedValues() {
        return this.allowedValues.clone();
    }
}
