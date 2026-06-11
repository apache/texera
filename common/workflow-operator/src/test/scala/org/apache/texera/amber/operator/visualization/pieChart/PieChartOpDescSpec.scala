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

package org.apache.texera.amber.operator.visualization.pieChart

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class PieChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {
  var opDesc: PieChartOpDesc = _
  before {
    opDesc = new PieChartOpDesc()
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  "PieChartOpDesc.operatorInfo" should "advertise the user-friendly name and Basic group" in {
    val info = opDesc.operatorInfo
    info.userFriendlyName shouldBe "Pie Chart"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    info.operatorDescription should include("Pie Chart")
  }

  it should "expose exactly one output port wired through forVisualization" in {
    opDesc.operatorInfo.outputPorts should have length 1
  }

  "PieChartOpDesc.getOutputSchemas" should "return a single-port schema with an html-content STRING column" in {
    opDesc.value = "amount"
    opDesc.name = "label"
    val schemas = opDesc.getOutputSchemas(Map.empty)
    schemas should have size 1
    val (portId, schema) = schemas.head
    portId shouldBe opDesc.operatorInfo.outputPorts.head.id
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "html-content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  "PieChartOpDesc.generatePythonCode" should "render Python source with runtime decode sites for value and name" in {
    // Use distinct sentinels and assert on the exact base64-wrapped decode
    // expressions so a regression that leaves `name` as a raw literal
    // cannot satisfy a generic `decodeOccurrences >= 2` (since `value` is
    // referenced multiple times in the generated template anyway).
    opDesc.value = "VAL_SENT"
    opDesc.name = "NAME_SENT"
    val code = opDesc.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.express")

    def b64(s: String): String =
      Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

    code should include(s"self.decode_python_template('${b64("VAL_SENT")}')")
    code should include(s"self.decode_python_template('${b64("NAME_SENT")}')")
    code should not include "VAL_SENT"
    code should not include "NAME_SENT"
  }

  it should "fail-fast when value is unset even if name is set (only `value` is asserted)" in {
    // Pin: PieChartOpDesc.manipulateTable and createPlotlyFigure both assert
    // nonEmpty on `value`, but neither asserts on `name`. Setting just `name`
    // is therefore not enough to satisfy the guards.
    opDesc.name = "label"
    assertThrows[AssertionError](opDesc.generatePythonCode())
  }

  it should "render successfully when only name is empty (asymmetric guard, current behavior)" in {
    // Pin: name has no assert guard. With value set and name empty, the
    // generated Python still renders — only the runtime call site receives
    // an empty decode. This asymmetry between value (asserted) and name
    // (not asserted) is documented here.
    opDesc.value = "amount"
    opDesc.name = ""
    val code = opDesc.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }
}
