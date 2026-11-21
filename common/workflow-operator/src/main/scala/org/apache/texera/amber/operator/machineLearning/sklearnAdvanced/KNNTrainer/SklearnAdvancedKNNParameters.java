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
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.ParamSpec;

public enum SklearnAdvancedKNNParameters implements ParamClass {
    // Bounds are scikit-learn's own: a neighbour count and a leaf size start at one, and the
    // Minkowski power is open at zero.
    n_neighbors(ParamSpec.of("n_neighbors", "int", "5").withMinimum(">=1")),
    p(ParamSpec.of("p", "int", "2").withMinimum(">0")),
    weights(ParamSpec.of("weights", "str", "", "uniform", "distance")),
    algorithm(ParamSpec.of("algorithm", "str", "", "auto", "ball_tree", "kd_tree", "brute")),
    leaf_size(ParamSpec.of("leaf_size", "int", "30").withMinimum(">=1")),
    // A metric is named, not measured: "minkowski" and the rest are words, so the
    // int() this used to declare rejected every value scikit-learn would take.
    metric(ParamSpec.of("metric", "str", "minkowski", "minkowski", "euclidean", "manhattan",
            "chebyshev", "cityblock", "l1", "l2")),
    // The only one that is not a scalar. scikit-learn wants a mapping of extra
    // keyword arguments for the metric, so the user's text is read as JSON.
    metric_params(ParamSpec.of("metric_params", "json.loads", "{}"));

    private final ParamSpec paramSpec;

    SklearnAdvancedKNNParameters(ParamSpec paramSpec) {
        this.paramSpec = paramSpec;
    }

    @Override
    public ParamSpec paramSpec() {
        return this.paramSpec;
    }
}
