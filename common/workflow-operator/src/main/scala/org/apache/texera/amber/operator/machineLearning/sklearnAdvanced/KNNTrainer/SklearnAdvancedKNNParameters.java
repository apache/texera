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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer;

import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.ParamClass;

public enum SklearnAdvancedKNNParameters implements ParamClass {
    n_neighbors("n_neighbors", "int", "5"),
    p("p", "int", "2"),
    weights("weights", "str", "", "uniform", "distance"),
    algorithm("algorithm", "str", "", "auto", "ball_tree", "kd_tree", "brute"),
    leaf_size("leaf_size", "int", "30"),
    // The last two have no example and no accepted set, because neither has a value worth
    // naming under the converter it declares: the metrics are words while int() takes only
    // numbers, and metric_params is a mapping that str() cannot produce.
    metric("metric", "int", ""),
    metric_params("metric_params", "str", "");

    private final String name;
    private final String type;
    private final String sampleValue;
    private final String[] allowedValues;

    SklearnAdvancedKNNParameters(
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
