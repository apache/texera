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

package org.apache.texera.amber.operator.visualization.dumbbellPlot

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DumbbellPlotOpDescSpec extends AnyFlatSpec with Matchers {

  "DumbbellPlotOpDesc.operatorInfo" should
    "advertise the name and Basic visualization group" in {
    val info = (new DumbbellPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Dumbbell Plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "DumbbellPlotOpDesc" should "default its column fields to empty and showLegends to false" in {
    val d = new DumbbellPlotOpDesc
    d.categoryColumnName shouldBe ""
    d.dumbbellStartValue shouldBe ""
    d.dumbbellEndValue shouldBe ""
    d.measurementColumnName shouldBe ""
    d.comparedColumnName shouldBe ""
    d.showLegends shouldBe false
  }

  "DumbbellPlotOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new DumbbellPlotOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "DumbbellPlotOpDesc.generatePythonCode" should "emit a Plotly Scatter (dumbbell) figure" in {
    val d = new DumbbellPlotOpDesc
    d.categoryColumnName = "entity"
    d.measurementColumnName = "metric"
    d.comparedColumnName = "phase"
    d.dumbbellStartValue = "before"
    d.dumbbellEndValue = "after"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("go.Scatter(")
  }

  "DumbbellPlotOpDesc.createPlotlyDumbbellLineFigure" should
    "select the showlegend flag from showLegends" in {
    val on = new DumbbellPlotOpDesc
    on.showLegends = true
    on.createPlotlyDumbbellLineFigure().plain should include("showlegend=True")

    val off = new DumbbellPlotOpDesc // showLegends defaults to false
    off.createPlotlyDumbbellLineFigure().plain should include("showlegend=False")
  }

  "DumbbellPlotOpDesc.addPlotlyDots" should
    "list the configured dot columns and default to an empty list" in {
    val d = new DumbbellPlotOpDesc
    val dot1 = new DumbbellDotConfig
    dot1.dotValue = "q1"
    val dot2 = new DumbbellDotConfig
    dot2.dotValue = "q2"
    d.dots = java.util.Arrays.asList(dot1, dot2)
    val withDots = d.addPlotlyDots().plain
    // both configured dots must be emitted: the rendered list has two comma-separated
    // (base64-encoded) entries, e.g. dotColumnNames = [<enc-q1>,<enc-q2>]
    val dotsLine = withDots.linesIterator.find(_.contains("dotColumnNames = [")).getOrElse("")
    dotsLine should not include "dotColumnNames = []"
    dotsLine.split(",") should have length 2

    (new DumbbellPlotOpDesc).addPlotlyDots().plain should include("dotColumnNames = []")
  }

  "DumbbellPlotOpDesc" should "round-trip its column fields through the polymorphic base" in {
    val d = new DumbbellPlotOpDesc
    d.categoryColumnName = "entity"
    d.measurementColumnName = "metric"
    d.comparedColumnName = "phase"
    d.dumbbellStartValue = "before"
    d.dumbbellEndValue = "after"
    d.showLegends = true
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[DumbbellPlotOpDesc]
    val dp = restored.asInstanceOf[DumbbellPlotOpDesc]
    dp.categoryColumnName shouldBe "entity"
    dp.measurementColumnName shouldBe "metric"
    dp.comparedColumnName shouldBe "phase"
    dp.dumbbellStartValue shouldBe "before"
    dp.dumbbellEndValue shouldBe "after"
    dp.showLegends shouldBe true
  }
}
