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

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SklearnLinearRegressionOpDescSpec extends AnyFlatSpec with Matchers {

  "SklearnLinearRegressionOpDesc.operatorInfo" should
    "advertise the model name, Sklearn group, and the training/testing port shape" in {
    val info = (new SklearnLinearRegressionOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Linear Regression"
    info.operatorDescription shouldBe "Sklearn Linear Regression Operator"
    info.operatorGroupName shouldBe OperatorGroupConstants.SKLEARN_GROUP
    info.inputPorts.map(_.displayName) shouldBe List("training", "testing")
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  "SklearnLinearRegressionOpDesc" should "default target to null and degree to 1" in {
    val d = new SklearnLinearRegressionOpDesc
    d.target shouldBe null
    d.degree shouldBe 1
  }

  "SklearnLinearRegressionOpDesc.getOutputSchemas" should
    "emit the model_name/model schema keyed by the declared output port" in {
    val d = new SklearnLinearRegressionOpDesc
    val schema = d.getOutputSchemas(Map.empty)(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("model_name").getType shouldBe AttributeType.STRING
    schema.getAttribute("model").getType shouldBe AttributeType.BINARY
  }

  "SklearnLinearRegressionOpDesc.generatePythonCode" should
    "build a polynomial LinearRegression pipeline" in {
    val d = new SklearnLinearRegressionOpDesc
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("from sklearn.linear_model import LinearRegression")
    code should include("from sklearn.preprocessing import PolynomialFeatures")
    code should include("make_pipeline")
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }

  // This operator builds its own pipeline rather than inheriting the classifier
  // base's, so the drop the rest of the family gained has to be stated here too.
  it should "drop rows with missing values and say how many" in {
    val d = new SklearnLinearRegressionOpDesc
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("table.dropna()")
    code should include("\"Skipped\"")
  }

  // The operator fits every column but the target, so a text column beside the
  // numbers would end the run from inside scikit-learn.
  it should "narrow the features to the columns an estimator can fit" in {
    val d = new SklearnLinearRegressionOpDesc
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("""_fittable = X.select_dtypes(include=["number", "bool"])""")
    code should include("""print("Ignoring columns an estimator cannot fit:", _ignored)""")
    code should include("X = _fittable")
  }

  // Narrowing a table whose every column but the target is text leaves a frame of
  // no columns, which numpy answers with `at least one array or dtype is required`.
  it should "say so when the narrowing leaves no column at all" in {
    val d = new SklearnLinearRegressionOpDesc
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("if _fittable.columns.empty:")
    code should include("No column left to fit on")
  }

  // Both frames, since the model is fitted on one and scored on the other: a
  // narrowing on only one side hands `predict` a different feature set.
  "SklearnLinearRegressionOpDesc.generateStandaloneCode" should
    "narrow both the training and the testing features" in {
    val d = new SklearnLinearRegressionOpDesc
    d.target = "y"
    val code = d.generateStandaloneCode()
    code should include("""_fittable = X_train.select_dtypes(include=["number", "bool"])""")
    code should include("X_train = _fittable")
    code should include("""_fittable = X_test.select_dtypes(include=["number", "bool"])""")
    code should include("X_test = _fittable")
  }

  "SklearnLinearRegressionOpDesc" should
    "round-trip its target through the polymorphic base" in {
    val d = new SklearnLinearRegressionOpDesc
    d.target = "label"
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"SklearnLinearRegression\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SklearnLinearRegressionOpDesc]
    restored.asInstanceOf[SklearnLinearRegressionOpDesc].target shouldBe "label"
  }
}
