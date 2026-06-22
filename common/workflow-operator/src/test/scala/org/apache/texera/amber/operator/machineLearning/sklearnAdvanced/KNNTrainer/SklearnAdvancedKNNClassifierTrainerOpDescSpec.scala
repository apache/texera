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

class SklearnAdvancedKNNClassifierTrainerOpDescSpec extends AnyFlatSpec {

  "SklearnAdvancedKNNClassifierTrainerOpDesc.getImportStatements" should
    "return the canonical KNeighborsClassifier import" in {
    val d = new SklearnAdvancedKNNClassifierTrainerOpDesc
    assert(d.getImportStatements == "from sklearn.neighbors import KNeighborsClassifier")
  }

  "SklearnAdvancedKNNClassifierTrainerOpDesc.getOperatorInfo" should
    "return 'KNN Classifier'" in {
    val d = new SklearnAdvancedKNNClassifierTrainerOpDesc
    assert(d.getOperatorInfo == "KNN Classifier")
  }

  it should "be stable across two instances (no instance-state interaction)" in {
    val a = new SklearnAdvancedKNNClassifierTrainerOpDesc
    val b = new SklearnAdvancedKNNClassifierTrainerOpDesc
    assert(a.getImportStatements == b.getImportStatements)
    assert(a.getOperatorInfo == b.getOperatorInfo)
  }

  "SklearnAdvancedKNNClassifierTrainerOpDesc" should
    "extend SklearnMLOperatorDescriptor (compile-time enforced)" in {
    val d: SklearnMLOperatorDescriptor[SklearnAdvancedKNNParameters] =
      new SklearnAdvancedKNNClassifierTrainerOpDesc
    assert(d.getImportStatements.contains("KNeighborsClassifier"))
  }

  it should "be matchable via the SklearnMLOperatorDescriptor type-pattern" in {
    val any: AnyRef = new SklearnAdvancedKNNClassifierTrainerOpDesc
    val matched = any match {
      case _: SklearnMLOperatorDescriptor[_] => true
      case _                                 => false
    }
    assert(matched)
  }
}
