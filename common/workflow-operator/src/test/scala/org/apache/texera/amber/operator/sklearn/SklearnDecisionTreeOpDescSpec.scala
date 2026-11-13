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

package org.apache.texera.amber.operator.sklearn

import org.scalatest.flatspec.AnyFlatSpec

class SklearnDecisionTreeOpDescSpec extends AnyFlatSpec with SklearnModelOpDescBehaviors {

  sklearnClassifier(
    new SklearnDecisionTreeOpDesc,
    "SklearnDecisionTree",
    "Decision Tree",
    "from sklearn.tree import DecisionTreeClassifier"
  )

  // A split compares, and a comparison can ask whether the value is there before it
  // asks how large it is, so the row is worth keeping and the count is worth saying.
  "SklearnDecisionTreeOpDesc" should "keep a row with a blank feature and say how many it kept" in {
    val d = new SklearnDecisionTreeOpDesc
    d.target = "y"
    val code = d.generatePythonCode()
    code should not include "table.dropna() "
    code should include("dropna(subset=[")
    code should include("rows_with_gaps")
  }
}
