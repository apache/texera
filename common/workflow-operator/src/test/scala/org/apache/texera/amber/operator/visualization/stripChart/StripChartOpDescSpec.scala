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

package org.apache.texera.amber.operator.visualization.stripChart

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StripChartOpDescSpec extends AnyFlatSpec with Matchers {

  "StripChartOpDesc.operatorInfo" should
    "advertise the name and Statistical visualization group" in {
    val info = (new StripChartOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Strip Chart"
    info.operatorDescription shouldBe "Visualize distribution of data points as a strip plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "StripChartOpDesc" should "default x / y / colorBy / facetColumn to the empty string" in {
    val d = new StripChartOpDesc
    d.x shouldBe ""
    d.y shouldBe ""
    d.colorBy shouldBe ""
    d.facetColumn shouldBe ""
  }

  "StripChartOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new StripChartOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "StripChartOpDesc.generatePythonCode" should "emit a Plotly px.strip figure" in {
    val d = new StripChartOpDesc
    d.x = "category"
    d.y = "value"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.express")
    code should include("px.strip(")
  }

  "StripChartOpDesc" should "round-trip all four column fields through the polymorphic base" in {
    val d = new StripChartOpDesc
    d.x = "cat"
    d.y = "val"
    d.colorBy = "grp"
    d.facetColumn = "panel"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[StripChartOpDesc]
    val s = restored.asInstanceOf[StripChartOpDesc]
    s.x shouldBe "cat"
    s.y shouldBe "val"
    s.colorBy shouldBe "grp"
    s.facetColumn shouldBe "panel"
  }
}
