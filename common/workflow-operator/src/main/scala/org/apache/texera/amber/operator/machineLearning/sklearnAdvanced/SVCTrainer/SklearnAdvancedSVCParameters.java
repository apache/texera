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

public enum SklearnAdvancedSVCParameters implements ParamClass {
    // Bounds are scikit-learn's own, whose ranges are open at zero for the two below and
    // closed for degree.
    C("C", "float", "1.0") { @Override public String getMinimum() { return ">0"; } },
    kernel("kernel", "str", "", "rbf", "linear", "poly", "sigmoid", "precomputed"),
    // gamma takes either of two words or a number, so no converter of a name covers it. This
    // one hands the words through and puts everything else past float(), which is also what
    // decides that a value is not a number at all.
    //
    // The pattern below is what that converter takes. Digits are [0-9] rather than \d so the
    // three engines it runs through read it alike: Python's float() also takes non-ASCII
    // decimal digits, but JavaScript's \d does not match them either, so the browser turns
    // them away whichever spelling is used. It is loose in one direction, letting a negative
    // through for the estimator to refuse, because excluding the sign would also exclude -0.0,
    // which the estimator takes, and turning away a value that works is the worse mistake.
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
    // coef0 is the one parameter here with no bound at either end.
    coef0("coef0", "float", "0.0"),
    tol("tol", "float", "0.001") { @Override public String getMinimum() { return ">0"; } },
    probability("probability", "(lambda value: value.lower() == \"true\")", "", "false", "true");

    private final String name;
    private final String type;
    private final String sampleValue;
    private final String[] allowedValues;

    SklearnAdvancedSVCParameters(
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
