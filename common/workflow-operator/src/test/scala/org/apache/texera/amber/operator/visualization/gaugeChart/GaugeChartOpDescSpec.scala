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

package org.apache.texera.amber.operator.visualization.gaugeChart

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GaugeChartOpDescSpec extends AnyFlatSpec with Matchers {

  "GaugeChartOpDesc.operatorInfo" should
    "advertise the name and Financial visualization group" in {
    val info = (new GaugeChartOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Gauge Chart"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "GaugeChartOpDesc" should
    "default value to empty, delta/threshold to unset and steps to an empty list" in {
    val d = new GaugeChartOpDesc
    d.value shouldBe ""
    d.delta shouldBe None
    d.threshold shouldBe None
    d.steps shouldBe empty
  }

  "GaugeChartOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new GaugeChartOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "GaugeChartOpDesc.generatePythonCode" should "emit a Plotly Indicator figure" in {
    val d = new GaugeChartOpDesc
    d.value = "score"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.graph_objects")
    code should include("go.Indicator(")
  }

  "GaugeChartOpDesc" should
    "round-trip value/delta/threshold and steps through the polymorphic base" in {
    val d = new GaugeChartOpDesc
    d.value = "v"
    d.delta = Some(40)
    d.threshold = Some(80)
    val step = new GaugeChartSteps
    step.start = Some(0)
    step.end = Some(50)
    d.steps = List(step)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[GaugeChartOpDesc]
    val g = restored.asInstanceOf[GaugeChartOpDesc]
    g.value shouldBe "v"
    g.delta shouldBe Some(40)
    g.threshold shouldBe Some(80)
    g.steps should have length 1
    g.steps.head.start shouldBe Some(0)
    g.steps.head.end shouldBe Some(50)
  }

  /** An unset field has to arrive as Python's `None` for the template's
    * `is not None` guards to read it as "not configured".
    */
  "GaugeChartOpDesc.generatePythonCode" should
    "assign delta and threshold as numbers, and None when they are unset" in {
    val d = new GaugeChartOpDesc
    d.value = "score"
    d.generatePythonCode() should include("delta_ref = None")
    d.generatePythonCode() should include("threshold_val = None")
    d.delta = Some(40)
    d.threshold = Some(80.5)
    val code = d.generatePythonCode()
    code should include("delta_ref = 40.0")
    code should include("threshold_val = 80.5")
  }

  it should "emit only the steps whose bounds are both filled in" in {
    val d = new GaugeChartOpDesc
    d.value = "score"
    val complete = new GaugeChartSteps
    complete.start = Some(0)
    complete.end = Some(50)
    val halfFilled = new GaugeChartSteps
    halfFilled.start = Some(50)
    d.steps = List(complete, halfFilled)
    val code = d.generatePythonCode()
    code should include("""valid_steps = [{"start": 0.0, "end": 50.0}]""")
  }
}
