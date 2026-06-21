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

package org.apache.texera.amber.operator.visualization.carpetPlot

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CarpetPlotOpDescSpec extends AnyFlatSpec with Matchers {

  "CarpetPlotOpDesc.operatorInfo" should
    "advertise the name and Scientific visualization group" in {
    val info = (new CarpetPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Carpet Plot"
    info.operatorDescription shouldBe "Visualize data in a Carpet Plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "CarpetPlotOpDesc" should "default a / b / y to the empty string" in {
    val d = new CarpetPlotOpDesc
    d.a shouldBe ""
    d.b shouldBe ""
    d.y shouldBe ""
  }

  "CarpetPlotOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new CarpetPlotOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "CarpetPlotOpDesc.generatePythonCode" should "emit a Plotly Carpet figure" in {
    val d = new CarpetPlotOpDesc
    d.a = "ax"
    d.b = "bx"
    d.y = "yx"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("go.Carpet(")
  }

  "CarpetPlotOpDesc" should "round-trip a / b / y through the polymorphic base" in {
    val d = new CarpetPlotOpDesc
    d.a = "ax"
    d.b = "bx"
    d.y = "yx"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[CarpetPlotOpDesc]
    val c = restored.asInstanceOf[CarpetPlotOpDesc]
    c.a shouldBe "ax"
    c.b shouldBe "bx"
    c.y shouldBe "yx"
  }
}
