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
    n_neighbors("n_neighbors", "int"),
    p("p", "int"),
    weights("weights", "str"),
    algorithm("algorithm", "str"),
    leaf_size("leaf_size", "int"),
    // A metric is named, not measured: "minkowski" and the rest of the accepted
    // set are words, so int() rejects every value scikit-learn would take.
    metric("metric", "str"),
    // The only one that is not a scalar. scikit-learn wants a mapping of extra
    // keyword arguments for the metric, so the user's text is read as JSON.
    metric_params("metric_params", "json.loads");

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
