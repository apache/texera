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

package org.apache.texera.amber.operator.visualization.barChart

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class BarChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: BarChartOpDesc = _

  before {
    opDesc = new BarChartOpDesc()
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "list titles of axes in the python code" in {
    // The plain (un-encoded) template body still carries the literal column
    // names; only the encoded `generatePythonCode` output runs them through
    // base64 + decode_python_template wrapping.
    opDesc.fields = "geo.state_name"
    opDesc.value = "person.count"
    val temp = opDesc.manipulateTable().plain
    assert(temp.contains("geo.state_name"))
    assert(temp.contains("person.count"))
  }

  it should "throw assertion error if chart is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  "BarChartOpDesc.operatorInfo" should "advertise the user-friendly name and Basic group" in {
    val info = opDesc.operatorInfo
    info.userFriendlyName shouldBe "Bar Chart"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    info.operatorDescription should include("Bar Chart")
  }

  it should "expose exactly one output port wired through forVisualization" in {
    opDesc.operatorInfo.outputPorts should have length 1
  }

  "BarChartOpDesc.getOutputSchemas" should "return a single-port schema with an html-content STRING column" in {
    opDesc.value = "v"
    opDesc.fields = "f"
    val schemas = opDesc.getOutputSchemas(Map.empty)
    schemas should have size 1
    val (portId, schema) = schemas.head
    portId shouldBe opDesc.operatorInfo.outputPorts.head.id
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "html-content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  "BarChartOpDesc.generatePythonCode" should "render a UDFTableOperator source with runtime decode sites for value AND fields" in {
    // Use distinct sentinels and assert on the exact base64-wrapped decode
    // expressions so the test actually proves both `value` *and* `fields`
    // were wrapped through wrapWithPythonDecoderExpr. A generic
    // `decodeOccurrences >= 2` could be satisfied by `value` alone since
    // both fields appear in multiple template positions.
    opDesc.value = "VAL_SENT"
    opDesc.fields = "FIELDS_SENT"
    val code = opDesc.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.express")

    def b64(s: String): String =
      Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

    code should include(s"self.decode_python_template('${b64("VAL_SENT")}')")
    code should include(s"self.decode_python_template('${b64("FIELDS_SENT")}')")
    code should not include "VAL_SENT"
    code should not include "FIELDS_SENT"
  }

  it should "fail-fast when value or fields is unset (asserts inside manipulateTable)" in {
    // manipulateTable asserts nonEmpty on value AND fields with explicit
    // messages ("Value column cannot be empty" / "Fields cannot be empty").
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should (include("Value column") or include("Fields"))
  }

}
