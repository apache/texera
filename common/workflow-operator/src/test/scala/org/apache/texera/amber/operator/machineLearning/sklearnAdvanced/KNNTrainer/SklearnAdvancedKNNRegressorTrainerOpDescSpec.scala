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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer

import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor
import org.scalatest.flatspec.AnyFlatSpec

class SklearnAdvancedKNNRegressorTrainerOpDescSpec extends AnyFlatSpec {

  "SklearnAdvancedKNNRegressorTrainerOpDesc.getImportStatements" should
    "return the canonical KNeighborsRegressor import" in {
    val d = new SklearnAdvancedKNNRegressorTrainerOpDesc
    assert(d.getImportStatements == "from sklearn.neighbors import KNeighborsRegressor")
  }

  "SklearnAdvancedKNNRegressorTrainerOpDesc.getOperatorInfo" should "return 'KNN Regressor'" in {
    val d = new SklearnAdvancedKNNRegressorTrainerOpDesc
    assert(d.getOperatorInfo == "KNN Regressor")
  }

  it should "be stable across two instances (no instance-state interaction)" in {
    val a = new SklearnAdvancedKNNRegressorTrainerOpDesc
    val b = new SklearnAdvancedKNNRegressorTrainerOpDesc
    assert(a.getImportStatements == b.getImportStatements)
    assert(a.getOperatorInfo == b.getOperatorInfo)
  }

  "SklearnAdvancedKNNRegressorTrainerOpDesc" should
    "extend SklearnMLOperatorDescriptor (compile-time enforced)" in {
    val d: SklearnMLOperatorDescriptor[SklearnAdvancedKNNParameters] =
      new SklearnAdvancedKNNRegressorTrainerOpDesc
    assert(d.getImportStatements.contains("KNeighborsRegressor"))
  }

  it should "be matchable via the SklearnMLOperatorDescriptor type-pattern" in {
    val any: AnyRef = new SklearnAdvancedKNNRegressorTrainerOpDesc
    val matched = any match {
      case _: SklearnMLOperatorDescriptor[_] => true
      case _                                 => false
    }
    assert(matched)
  }

  it should "differ from the Classifier sibling on both methods" in {
    // Catches a copy-paste regression where the Regressor accidentally
    // returned the Classifier's strings (or vice-versa).
    val regressor = new SklearnAdvancedKNNRegressorTrainerOpDesc
    val classifier = new SklearnAdvancedKNNClassifierTrainerOpDesc
    assert(regressor.getImportStatements != classifier.getImportStatements)
    assert(regressor.getOperatorInfo != classifier.getOperatorInfo)
  }
}
