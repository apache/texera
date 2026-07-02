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

package org.apache.texera.amber.operator.sklearn.testing

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SklearnTestingOpDescSpec extends AnyFlatSpec with Matchers {

  "SklearnTestingOpDesc.operatorInfo" should
    "advertise the name, Sklearn group, and a model/data 2-in 1-out shape" in {
    val info = (new SklearnTestingOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Sklearn Testing"
    info.operatorDescription shouldBe "It will generate scorers for Sklearn model"
    info.operatorGroupName shouldBe OperatorGroupConstants.SKLEARN_GROUP
    info.inputPorts.map(_.displayName) shouldBe List("model", "data")
    info.outputPorts should have length 1
  }

  "SklearnTestingOpDesc" should "default isRegression false and the attribute fields to null" in {
    val d = new SklearnTestingOpDesc
    d.isRegression shouldBe false
    d.model shouldBe null
    d.target shouldBe null
  }

  "SklearnTestingOpDesc.getOutputSchemas" should
    "append the classification metric columns for the default (non-regression) case" in {
    val d = new SklearnTestingOpDesc
    val input = Schema().add("x", AttributeType.STRING)
    val schema =
      d.getOutputSchemas(Map(PortIdentity() -> input))(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("x").getType shouldBe AttributeType.STRING
    schema.getAttribute("accuracy").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("f1").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("precision").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("recall").getType shouldBe AttributeType.DOUBLE
  }

  it should "append the regression metric columns when isRegression is true" in {
    val d = new SklearnTestingOpDesc
    d.isRegression = true
    val input = Schema().add("x", AttributeType.STRING)
    val schema =
      d.getOutputSchemas(Map(PortIdentity() -> input))(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("R2").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("RMSE").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("MAE").getType shouldBe AttributeType.DOUBLE
  }

  "SklearnTestingOpDesc.generatePythonCode" should "emit the scorer tuple operator" in {
    val d = new SklearnTestingOpDesc
    d.model = "model"
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("class ProcessTupleOperator(UDFOperatorV2)")
    code should include("from sklearn.metrics import")
    code should include(".predict(")
  }

  "SklearnTestingOpDesc" should
    "round-trip its config fields through the polymorphic base" in {
    val d = new SklearnTestingOpDesc
    d.isRegression = true
    d.model = "m"
    d.target = "t"
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"SklearnTesting\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SklearnTestingOpDesc]
    val r = restored.asInstanceOf[SklearnTestingOpDesc]
    r.isRegression shouldBe true
    r.model shouldBe "m"
    r.target shouldBe "t"
  }
}
