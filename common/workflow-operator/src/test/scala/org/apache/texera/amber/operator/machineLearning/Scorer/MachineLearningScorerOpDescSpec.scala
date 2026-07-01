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

package org.apache.texera.amber.operator.machineLearning.Scorer

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class MachineLearningScorerOpDescSpec extends AnyFlatSpec with Matchers {

  "MachineLearningScorerOpDesc.operatorInfo" should
    "advertise the name and Machine Learning General group" in {
    val info = (new MachineLearningScorerOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Machine Learning Scorer"
    info.operatorDescription shouldBe "Scorer for machine learning models"
    info.operatorGroupName shouldBe OperatorGroupConstants.MACHINE_LEARNING_GENERAL_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "MachineLearningScorerOpDesc" should "default isRegression false and the column fields to empty" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression shouldBe false
    d.actualValueColumn shouldBe ""
    d.predictValueColumn shouldBe ""
    d.classificationMetrics shouldBe empty
    d.regressionMetrics shouldBe empty
  }

  "MachineLearningScorerOpDesc.getOutputSchemas" should
    "include a Class column for classification with no metrics" in {
    val d = new MachineLearningScorerOpDesc
    d.getOutputSchemas(Map.empty) shouldBe Map(
      d.operatorInfo.outputPorts.head.id -> Schema(
        List(new Attribute("Class", AttributeType.STRING))
      )
    )
  }

  it should "produce an empty schema for regression with no metrics" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    val out = d.getOutputSchemas(Map.empty)
    out.keySet shouldBe Set(d.operatorInfo.outputPorts.head.id)
    out(d.operatorInfo.outputPorts.head.id).getAttributes shouldBe empty
  }

  "MachineLearningScorerOpDesc.generatePythonCode" should "emit the scorer table operator" in {
    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("from sklearn.metrics import")
    // actualValueColumn/predictValueColumn are EncodableString: base64-encoded into the emitted code.
    code should include(Base64.getEncoder.encodeToString("y".getBytes(StandardCharsets.UTF_8)))
    code should include(Base64.getEncoder.encodeToString("yhat".getBytes(StandardCharsets.UTF_8)))
  }

  "MachineLearningScorerOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[MachineLearningScorerOpDesc]
    val s = restored.asInstanceOf[MachineLearningScorerOpDesc]
    s.isRegression shouldBe true
    s.actualValueColumn shouldBe "y"
    s.predictValueColumn shouldBe "yhat"
  }
}
