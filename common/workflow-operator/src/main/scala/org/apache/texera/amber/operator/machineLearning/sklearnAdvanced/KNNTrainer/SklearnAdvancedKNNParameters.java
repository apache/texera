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
    // Bounds are scikit-learn's own: a neighbour count and a leaf size start at one, and the
    // Minkowski power is open at zero.
    n_neighbors("n_neighbors", "int", "5") { @Override public String getMinimum() { return ">=1"; } },
    p("p", "int", "2") { @Override public String getMinimum() { return ">0"; } },
    weights("weights", "str", "", "uniform", "distance"),
    algorithm("algorithm", "str", "", "auto", "ball_tree", "kd_tree", "brute"),
    leaf_size("leaf_size", "int", "30") { @Override public String getMinimum() { return ">=1"; } },
    // A metric is named, not measured: "minkowski" and the rest are words, so the
    // int() this used to declare rejected every value scikit-learn would take. The
    // set is the one every `algorithm` above accepts -- the tree algorithms take
    // fewer metrics than brute force, and naming a brute-only metric here would
    // break the moment the sibling knob is moved off `auto`.
    metric("metric", "str", "minkowski", "minkowski", "euclidean", "manhattan",
            "chebyshev", "cityblock", "l1", "l2"),
    // The only one that is not a scalar. scikit-learn wants a mapping of extra
    // keyword arguments for the metric, so the user's text is read as JSON.
    metric_params("metric_params", "json.loads", "{}");

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
