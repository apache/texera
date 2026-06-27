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

package org.apache.texera.amber.operator.visualization.polarChart

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PolarChartOpDescSpec extends AnyFlatSpec with Matchers {

  "PolarChartOpDesc.operatorInfo" should
    "advertise the name and Scientific visualization group" in {
    val info = (new PolarChartOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Polar Chart"
    info.operatorDescription shouldBe "Displays data points in a polar scatter plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "PolarChartOpDesc" should "default r and theta to the empty string" in {
    val d = new PolarChartOpDesc
    d.r shouldBe ""
    d.theta shouldBe ""
  }

  "PolarChartOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new PolarChartOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "PolarChartOpDesc.generatePythonCode" should "emit a Plotly Scatterpolargl figure" in {
    val d = new PolarChartOpDesc
    d.r = "radius"
    d.theta = "angle"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.graph_objects")
    code should include("go.Scatterpolargl(")
  }

  "PolarChartOpDesc" should "round-trip r and theta through the polymorphic base" in {
    val d = new PolarChartOpDesc
    d.r = "radius"
    d.theta = "angle"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[PolarChartOpDesc]
    val p = restored.asInstanceOf[PolarChartOpDesc]
    p.r shouldBe "radius"
    p.theta shouldBe "angle"
  }
}
