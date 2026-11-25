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
  SklearnTrainerOpDescSpecBehaviors
}
import org.scalatest.flatspec.AnyFlatSpec

class SklearnAdvancedKNNClassifierTrainerOpDescSpec
    extends AnyFlatSpec
    with SklearnTrainerOpDescSpecBehaviors {

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

  trainerDescriptorBehaviors[
    SklearnAdvancedKNNParameters,
    SklearnAdvancedKNNClassifierTrainerOpDesc
  ](
    "SklearnAdvancedKNNClassifierTrainerOpDesc",
    () => new SklearnAdvancedKNNClassifierTrainerOpDesc,
    "from sklearn.neighbors import KNeighborsClassifier",
    "KNN Classifier",
    expectedImportToken = "KNeighborsClassifier"
  )

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
    val modelCall = code.linesIterator.find(_.contains("model = KNeighborsClassifier(")).get
    assert(modelCall.contains("metric = str ("))
    assert(modelCall.contains("metric_params = json.loads ("))
  }
}
