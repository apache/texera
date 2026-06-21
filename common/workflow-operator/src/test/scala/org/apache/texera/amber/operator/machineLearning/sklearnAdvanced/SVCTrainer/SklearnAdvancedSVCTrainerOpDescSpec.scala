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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer

import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor
import org.scalatest.flatspec.AnyFlatSpec

class SklearnAdvancedSVCTrainerOpDescSpec extends AnyFlatSpec {

  "SklearnAdvancedSVCTrainerOpDesc.getImportStatements" should
    "return the canonical SVC import (from sklearn.svm)" in {
    val d = new SklearnAdvancedSVCTrainerOpDesc
    assert(d.getImportStatements == "from sklearn.svm import SVC")
  }

  "SklearnAdvancedSVCTrainerOpDesc.getOperatorInfo" should "return 'SVM Classifier'" in {
    val d = new SklearnAdvancedSVCTrainerOpDesc
    assert(d.getOperatorInfo == "SVM Classifier")
  }

  it should "be stable across two instances (no instance-state interaction)" in {
    val a = new SklearnAdvancedSVCTrainerOpDesc
    val b = new SklearnAdvancedSVCTrainerOpDesc
    assert(a.getImportStatements == b.getImportStatements)
    assert(a.getOperatorInfo == b.getOperatorInfo)
  }

  "SklearnAdvancedSVCTrainerOpDesc" should
    "extend SklearnMLOperatorDescriptor (compile-time enforced)" in {
    val d: SklearnMLOperatorDescriptor[SklearnAdvancedSVCParameters] =
      new SklearnAdvancedSVCTrainerOpDesc
    assert(d.getImportStatements.contains("SVC"))
  }

  it should "be matchable via the SklearnMLOperatorDescriptor type-pattern" in {
    val any: AnyRef = new SklearnAdvancedSVCTrainerOpDesc
    val matched = any match {
      case _: SklearnMLOperatorDescriptor[_] => true
      case _                                 => false
    }
    assert(matched)
  }
}
