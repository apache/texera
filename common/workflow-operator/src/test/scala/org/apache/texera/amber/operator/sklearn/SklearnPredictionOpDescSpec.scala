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

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SklearnPredictionOpDescSpec extends AnyFlatSpec with Matchers {

  "SklearnPredictionOpDesc.operatorInfo" should
    "advertise the name, Sklearn group, and a model/data 2-in 1-out shape" in {
    val info = (new SklearnPredictionOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Sklearn Prediction"
    info.operatorDescription shouldBe "Sklearn Prediction Operator"
    info.operatorGroupName shouldBe OperatorGroupConstants.SKLEARN_GROUP
    info.inputPorts should have length 2
    info.inputPorts.head.displayName shouldBe "model"
    info.outputPorts should have length 1
  }

  "SklearnPredictionOpDesc" should "default its attribute fields" in {
    val d = new SklearnPredictionOpDesc
    d.model shouldBe null
    d.resultAttribute shouldBe null
    d.groundTruthAttribute shouldBe ""
  }

  "SklearnPredictionOpDesc.getOutputSchemas" should
    "append the result attribute to the data (port 1) schema" in {
    val d = new SklearnPredictionOpDesc
    d.resultAttribute = "prediction"
    val data = Schema().add("feature", AttributeType.STRING)
    val out = d.getOutputSchemas(Map(PortIdentity(1) -> data))
    val schema = out(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("feature").getType shouldBe AttributeType.STRING
    schema.getAttribute("prediction").getType shouldBe AttributeType.STRING
  }

  it should "derive the result column type from the configured ground-truth column" in {
    val d = new SklearnPredictionOpDesc
    d.resultAttribute = "prediction"
    d.groundTruthAttribute = "label"
    val data = Schema()
      .add("feature", AttributeType.STRING)
      .add("label", AttributeType.INTEGER)
    val out = d.getOutputSchemas(Map(PortIdentity(1) -> data))
    out(d.operatorInfo.outputPorts.head.id)
      .getAttribute("prediction")
      .getType shouldBe AttributeType.INTEGER
  }

  it should "throw when the configured ground-truth attribute is absent from the input schema" in {
    val d = new SklearnPredictionOpDesc
    d.resultAttribute = "prediction"
    d.groundTruthAttribute = "missing"
    val data = Schema().add("feature", AttributeType.STRING)
    intercept[NoSuchElementException] {
      d.getOutputSchemas(Map(PortIdentity(1) -> data))
    }
  }

  "SklearnPredictionOpDesc.generatePythonCode" should "emit the model-applying tuple operator" in {
    val d = new SklearnPredictionOpDesc
    d.model = "model"
    d.resultAttribute = "prediction"
    val code = d.generatePythonCode()
    code should include("class ProcessTupleOperator(UDFOperatorV2)")
    code should include("from sklearn.pipeline import Pipeline")
    code should include(".predict(")
    code should include("yield tuple_")
  }

  "SklearnPredictionOpDesc" should
    "round-trip its config fields through the polymorphic base" in {
    val d = new SklearnPredictionOpDesc
    d.model = "m"
    d.resultAttribute = "p"
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"SklearnPrediction\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SklearnPredictionOpDesc]
    val r = restored.asInstanceOf[SklearnPredictionOpDesc]
    r.model shouldBe "m"
    r.resultAttribute shouldBe "p"
  }
}
