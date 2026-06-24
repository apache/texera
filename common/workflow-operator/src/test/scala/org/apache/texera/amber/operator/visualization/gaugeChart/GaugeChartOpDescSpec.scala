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

  "GaugeChartOpDesc" should "default value/delta/threshold to empty and steps to an empty list" in {
    val d = new GaugeChartOpDesc
    d.value shouldBe ""
    d.delta shouldBe ""
    d.threshold shouldBe ""
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
    d.delta = "dl"
    d.threshold = "th"
    val step = new GaugeChartSteps
    step.start = "0"
    step.end = "50"
    d.steps = List(step)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[GaugeChartOpDesc]
    val g = restored.asInstanceOf[GaugeChartOpDesc]
    g.value shouldBe "v"
    g.delta shouldBe "dl"
    g.threshold shouldBe "th"
    g.steps should have length 1
    g.steps.head.start shouldBe "0"
    g.steps.head.end shouldBe "50"
  }
}
