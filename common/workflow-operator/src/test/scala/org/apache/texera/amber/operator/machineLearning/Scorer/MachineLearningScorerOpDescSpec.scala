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

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class MachineLearningScorerOpDescSpec extends AnyFlatSpec with Matchers {

  /** An EncodableString field renders as a runtime decode site in the emitted code. */
  private val decodeSite = "self.decode_python_template"

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  /** The one emitted line that assigns metric_list, isolated from the template body. */
  private def metricListLine(code: String): String =
    code.linesIterator
      .find(_.contains("metric_list = ["))
      .getOrElse(fail(s"no metric_list assignment in:\n$code"))

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

  it should "append one DOUBLE column per selected classification metric, after Class" in {
    // The metric columns carry numeric scores, so their type is the one schema
    // decision this method makes. With empty metric lists the foldLeft body never
    // runs and that type stays unpinned -- a scorer could advertise Accuracy as
    // STRING and the suite would not notice.
    val d = new MachineLearningScorerOpDesc
    d.classificationMetrics =
      List(classificationMetricsFnc.accuracy, classificationMetricsFnc.f1Score)
    val port = d.operatorInfo.outputPorts.head.id
    d.getOutputSchemas(Map.empty)(port).getAttributes.map(a => (a.getName, a.getType)) shouldBe
      List(
        ("Class", AttributeType.STRING),
        ("Accuracy", AttributeType.DOUBLE),
        ("F1 Score", AttributeType.DOUBLE)
      )
  }

  it should "produce an empty schema for regression with no metrics" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    val out = d.getOutputSchemas(Map.empty)
    out.keySet shouldBe Set(d.operatorInfo.outputPorts.head.id)
    out(d.operatorInfo.outputPorts.head.id).getAttributes shouldBe empty
  }

  it should "emit one DOUBLE column per selected regression metric and no Class column" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    d.regressionMetrics = List(regressionMetricsFnc.mse, regressionMetricsFnc.r2)
    // the classification list must be ignored entirely once the task is regression
    d.classificationMetrics = List(classificationMetricsFnc.accuracy)
    val port = d.operatorInfo.outputPorts.head.id
    d.getOutputSchemas(Map.empty)(port).getAttributes.map(a => (a.getName, a.getType)) shouldBe
      List(("MSE", AttributeType.DOUBLE), ("R2", AttributeType.DOUBLE))
  }

  "MachineLearningScorerOpDesc.generatePythonCode" should "emit the scorer table operator" in {
    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("from sklearn.metrics import")
    // actualValueColumn/predictValueColumn are EncodableString: base64-encoded into
    // the emitted code. Assert WHICH variable each payload is bound to, not just that
    // both payloads appear somewhere: precision_score/recall_score are asymmetric in
    // (y_true, y_pred), so a swap would silently report recall as precision.
    code should include(s"y_true = table[$decodeSite('${b64("y")}')]")
    code should include(s"y_pred = table[$decodeSite('${b64("yhat")}')]")
    // isRegression defaults to false, so the emitted branch guard must read False;
    // without this the `else "False"` arm of the flag is unpinned in both tests.
    code should include("if False:")
  }

  it should "drop the rows missing either scored column, on both paths" in {
    // The metrics refuse an empty cell instead of passing over it, so the row has
    // to go before it reaches them. Both generators emit the same drop, and the
    // subset names both columns: dropping on either one alone would leave the two
    // series misaligned.
    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    d.generatePythonCode() should include(
      s"table = table.dropna(subset=[$decodeSite('${b64("y")}'), $decodeSite('${b64("yhat")}')])"
    )
    d.generateStandaloneCode() should include("""in1df = in1df.dropna(subset=["y", "yhat"])""")
  }

  it should "splice the selected metrics verbatim into a proper metric_list" in {
    // The metric fragment must be spliced verbatim, not re-encoded as one quoted
    // value (which would collapse the whole list into a single malformed element).
    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    d.classificationMetrics =
      List(classificationMetricsFnc.accuracy, classificationMetricsFnc.f1Score)
    val code = d.generatePythonCode()
    code should include("metric_list = ['Accuracy','F1 Score']")
    // The metric names must NOT be base64-re-encoded through the template builder.
    val encoded =
      Base64.getEncoder.encodeToString("'Accuracy','F1 Score'".getBytes(StandardCharsets.UTF_8))
    code should not include encoded
  }

  it should "select the regression metrics and the regression branch when isRegression is set" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    d.regressionMetrics = List(regressionMetricsFnc.mse, regressionMetricsFnc.r2)
    // the classification list must be ignored entirely once the task is regression
    d.classificationMetrics = List(classificationMetricsFnc.accuracy)
    val code = d.generatePythonCode()

    metricListLine(code) should include("['MSE','R2']")
    // isRegression also picks the branch process_table takes at runtime
    code should include("if True:")
    code should not include "if False:"
  }

  it should "reject an unrecognized entry in the metric list loudly" in {
    // A saved workflow can carry a null element in the metric array. Emitting
    // `metric_list = ['']` for it would generate silently broken Python, so the
    // descriptor must fail while the workflow is still being compiled.
    val node = objectMapper
      .readTree(objectMapper.writeValueAsString(new MachineLearningScorerOpDesc))
      .asInstanceOf[ObjectNode]
    node.putArray("classificationFlag").addNull()
    val d =
      objectMapper.treeToValue(node, classOf[LogicalOp]).asInstanceOf[MachineLearningScorerOpDesc]
    d.classificationMetrics shouldBe List(null)

    val ex = intercept[IllegalArgumentException](d.generatePythonCode())
    ex.getMessage shouldBe "Unknown metric type"
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
