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

import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.{
  HyperParameters,
  SklearnMLOperatorDescriptor
}
import org.scalatest.flatspec.AnyFlatSpec

class SklearnAdvancedKNNClassifierTrainerOpDescSpec extends AnyFlatSpec {

  private def hyperParam(
      parameter: SklearnAdvancedKNNParameters,
      value: String
  ): HyperParameters[SklearnAdvancedKNNParameters] = {
    val hp = new HyperParameters[SklearnAdvancedKNNParameters]
    hp.parameter = parameter
    hp.parametersSource = false
    hp.value = value
    hp
  }

  private def codeFor(paraList: HyperParameters[SklearnAdvancedKNNParameters]*): String = {
    val d = new SklearnAdvancedKNNClassifierTrainerOpDesc
    d.paraList = paraList.toList
    d.selectedFeatures = List("f1")
    d.groundTruthAttribute = "label"
    d.generatePythonCode()
  }

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

  // The declared type is emitted as the callable that converts what the user
  // typed, so a hyperparameter is only usable when that callable can return
  // something scikit-learn accepts. These two could not: `metric` names a word
  // from a fixed set, and `metric_params` a mapping.
  "SklearnAdvancedKNNParameters.metric" should
    "convert with str, the accepted metrics being words" in {
    assert(
      codeFor(hyperParam(SklearnAdvancedKNNParameters.metric, "minkowski"))
        .contains("metric = str (")
    )
  }

  "SklearnAdvancedKNNParameters.metric_params" should
    "convert with json.loads, and the template must import json for it" in {
    val code = codeFor(hyperParam(SklearnAdvancedKNNParameters.metric_params, """{"p": 2}"""))
    assert(code.contains("metric_params = json.loads ("))
    assert(code.contains("import json"))
  }

  it should "apply both conversions in the model call, not only in the summary" in {
    val code = codeFor(
      hyperParam(SklearnAdvancedKNNParameters.metric, "minkowski"),
      hyperParam(SklearnAdvancedKNNParameters.metric_params, """{"p": 2}""")
    )
    // The values themselves travel base64-encoded, so what is checked here is
    // which callable each one is handed to.
    val modelCall = code.linesIterator.find(_.contains("model = KNeighborsClassifier(")).get
    assert(modelCall.contains("metric = str ("))
    assert(modelCall.contains("metric_params = json.loads ("))
  }
}
