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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVRTrainer

import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer.SklearnAdvancedSVCTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor
import org.scalatest.flatspec.AnyFlatSpec

class SklearnAdvancedSVRTrainerOpDescSpec extends AnyFlatSpec {

  "SklearnAdvancedSVRTrainerOpDesc.getImportStatements" should
    "return the canonical SVR import (from sklearn.svm)" in {
    val d = new SklearnAdvancedSVRTrainerOpDesc
    assert(d.getImportStatements == "from sklearn.svm import SVR")
  }

  "SklearnAdvancedSVRTrainerOpDesc.getOperatorInfo" should "return 'SVM Regressor'" in {
    val d = new SklearnAdvancedSVRTrainerOpDesc
    assert(d.getOperatorInfo == "SVM Regressor")
  }

  it should "be stable across two instances (no instance-state interaction)" in {
    val a = new SklearnAdvancedSVRTrainerOpDesc
    val b = new SklearnAdvancedSVRTrainerOpDesc
    assert(a.getImportStatements == b.getImportStatements)
    assert(a.getOperatorInfo == b.getOperatorInfo)
  }

  "SklearnAdvancedSVRTrainerOpDesc" should
    "extend SklearnMLOperatorDescriptor (compile-time enforced)" in {
    val d: SklearnMLOperatorDescriptor[SklearnAdvancedSVRParameters] =
      new SklearnAdvancedSVRTrainerOpDesc
    assert(d.getImportStatements.contains("SVR"))
  }

  it should "be matchable via the SklearnMLOperatorDescriptor type-pattern" in {
    val any: AnyRef = new SklearnAdvancedSVRTrainerOpDesc
    val matched = any match {
      case _: SklearnMLOperatorDescriptor[_] => true
      case _                                 => false
    }
    assert(matched)
  }

  it should "differ from the SVC sibling on both methods" in {
    val regressor = new SklearnAdvancedSVRTrainerOpDesc
    val classifier = new SklearnAdvancedSVCTrainerOpDesc
    assert(regressor.getImportStatements != classifier.getImportStatements)
    assert(regressor.getOperatorInfo != classifier.getOperatorInfo)
  }
}
