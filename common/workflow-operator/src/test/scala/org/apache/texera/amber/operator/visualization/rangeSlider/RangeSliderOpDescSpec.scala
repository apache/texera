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

package org.apache.texera.amber.operator.visualization.rangeSlider

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RangeSliderOpDescSpec extends AnyFlatSpec with Matchers {

  "RangeSliderOpDesc.operatorInfo" should
    "advertise the name and Basic visualization group" in {
    val info = (new RangeSliderOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Range Slider"
    info.operatorDescription shouldBe "Visualize data in a Range Slider"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "RangeSliderOpDesc" should "default xAxis and yAxis to the empty string" in {
    val d = new RangeSliderOpDesc
    d.xAxis shouldBe ""
    d.yAxis shouldBe ""
  }

  "RangeSliderOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new RangeSliderOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "RangeSliderOpDesc.generatePythonCode" should "emit a Plotly Scatter figure" in {
    val d = new RangeSliderOpDesc
    d.xAxis = "x"
    d.yAxis = "y"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("go.Scatter(")
  }

  "RangeSliderOpDesc" should
    "round-trip xAxis, yAxis, and duplicateType through the polymorphic base" in {
    val d = new RangeSliderOpDesc
    d.xAxis = "month"
    d.yAxis = "sales"
    d.duplicateType = RangeSliderHandleDuplicateFunction.MEAN
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[RangeSliderOpDesc]
    val r = restored.asInstanceOf[RangeSliderOpDesc]
    r.xAxis shouldBe "month"
    r.yAxis shouldBe "sales"
    r.duplicateType shouldBe RangeSliderHandleDuplicateFunction.MEAN
  }
}
