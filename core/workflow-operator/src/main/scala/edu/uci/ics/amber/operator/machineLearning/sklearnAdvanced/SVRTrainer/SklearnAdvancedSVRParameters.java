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

package edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.SVRTrainer;

import edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.base.ParamClass;

public enum SklearnAdvancedSVRParameters implements ParamClass {
    C("C", "float"),
    kernel("kernel", "str"),
    gamma("gamma", "float"),
    degree("degree", "int"),
    coef0("coef0", "float"),
    tol("tol", "float"),
    probability("shrinking", "(lambda value: value.lower() == \"true\")"),
    verbose("verbose", "(lambda value: value.lower() == \"true\")"),
    epsilon("epsilon", "float"),
    cache_size("cache_size", "int"),
    max_iter("max_iter", "int");

    private final String name;
    private final String type;

    SklearnAdvancedSVRParameters(String name, String type) {
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
