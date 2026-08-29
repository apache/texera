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
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
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

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

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

    code should include(s"self.decode_python_template('${b64("VAL_SENT")}')")
    code should include(s"self.decode_python_template('${b64("FIELDS_SENT")}')")
    code should not include "VAL_SENT"
    code should not include "FIELDS_SENT"
  }

  it should "name the Value column when only value is unset" in {
    // manipulateTable asserts nonEmpty on value AND fields with distinct messages.
    // Asserting `include("Value column") or include("Fields")` would hold even if the
    // two messages were swapped, i.e. even if a user who left Value blank were told
    // "Fields cannot be empty". Each case therefore leaves exactly one field unset and
    // pins the one message that belongs to it.
    opDesc.fields = "f"
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should include("Value column")
    ex.getMessage should not include "Fields"
  }

  it should "name Fields when only fields is unset" in {
    opDesc.value = "v"
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should include("Fields")
    ex.getMessage should not include "Value column"
  }

  "BarChartOpDesc.generatePythonCode" should "treat an unset categoryColumn as no category (color guarded to None)" in {
    // An empty categoryColumn (its Scala default) must guard color to None, not
    // emit `... if True else None` with an empty column name for px.bar(color=).
    opDesc.value = "score"
    opDesc.fields = "name"
    val code = opDesc.generatePythonCode()
    code should include("color=self.decode_python_template('') if False else None")
    code should not include "color=self.decode_python_template('') if True else None"
  }

  it should "colour-code by the chosen category column" in {
    opDesc.value = "score"
    opDesc.fields = "name"
    opDesc.categoryColumn = "cat"
    val code = opDesc.generatePythonCode()
    code should include(s"color=self.decode_python_template('${b64("cat")}') if True else None")
  }

  it should "treat the literal 'No Selection' as no category" in {
    // "No Selection" is the placeholder the UI shows for the optional category column
    // (it is this field's declared JSON defaultValue). It is a sentinel, not a column
    // name: passing it through to px.bar(color=...) would look up a column that does
    // not exist. The non-emptiness check alone does not stop it.
    opDesc.value = "score"
    opDesc.fields = "name"
    opDesc.categoryColumn = "No Selection"
    val code = opDesc.generatePythonCode()
    code should include(
      s"color=self.decode_python_template('${b64("No Selection")}') if False else None"
    )
    code should not include s"self.decode_python_template('${b64("No Selection")}') if True"
  }

  it should "enable the horizontal branch of the chart only when horizontalOrientation is set" in {
    // Both px.bar calls -- the `orientation = 'h'` one and the vertical one -- are
    // literal text in every generated program; which one runs is decided by the Python
    // guard this Scala flag splices in. So asserting on "orientation = 'h'" alone would
    // pass no matter what the flag says, and asserting only on the True/False literal
    // would pass even if the two branch bodies were swapped. Pin the ADJACENCY: the
    // guard line, the body directly under it, the `else:`, and the else body -- and the
    // axis assignment inside each, since a horizontal bar chart must put the category
    // on y and the numeric value on x (the vertical one is the mirror image).
    opDesc.value = "score"
    opDesc.fields = "name"
    val valueRef = s"self.decode_python_template('${b64("score")}')"
    val fieldsRef = s"self.decode_python_template('${b64("name")}')"
    val horizontalCall = s"px.bar(table, y=$fieldsRef, x=$valueRef,"
    val verticalCall = s"px.bar(table, y=$valueRef, x=$fieldsRef,"

    opDesc.horizontalOrientation = true
    val horizontal = opDesc.generatePythonCode()
    horizontal should not include "if False:"
    val hLines = horizontal.linesIterator.toVector
    val hGuard = hLines.indexWhere(_.trim == "if True:")
    withClue(s"no `if True:` guard line in:\n$horizontal") { hGuard should be >= 0 }
    hLines(hGuard + 2).trim shouldBe "else:"
    hLines(hGuard + 1) should include(horizontalCall)
    hLines(hGuard + 1) should include("orientation = 'h'")
    hLines(hGuard + 3) should include(verticalCall)
    hLines(hGuard + 3) should not include "orientation = 'h'"

    opDesc.horizontalOrientation = false
    val vertical = opDesc.generatePythonCode()
    vertical should not include "if True:"
    val vLines = vertical.linesIterator.toVector
    val vGuard = vLines.indexWhere(_.trim == "if False:")
    withClue(s"no `if False:` guard line in:\n$vertical") { vGuard should be >= 0 }
    vLines(vGuard + 2).trim shouldBe "else:"
    vLines(vGuard + 1) should include(horizontalCall)
    vLines(vGuard + 1) should include("orientation = 'h'")
    vLines(vGuard + 3) should include(verticalCall)
    vLines(vGuard + 3) should not include "orientation = 'h'"
  }

  it should "request a pattern shape only when a pattern column is chosen" in {
    opDesc.value = "score"
    opDesc.fields = "name"

    val withoutPattern = opDesc.generatePythonCode()
    withoutPattern should include(
      "pattern_shape=self.decode_python_template('') if False else None"
    )

    opDesc.pattern = "texture"
    val withPattern = opDesc.generatePythonCode()
    withPattern should include(
      s"pattern_shape=self.decode_python_template('${b64("texture")}') if True else None"
    )
  }

  "BarChartOpDesc" should "round-trip its config fields through the polymorphic base" in {
    opDesc.value = "score"
    opDesc.fields = "name"
    opDesc.categoryColumn = "cat"
    opDesc.pattern = "texture"
    opDesc.horizontalOrientation = true

    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(opDesc), classOf[LogicalOp])
    restored shouldBe a[BarChartOpDesc]
    val b = restored.asInstanceOf[BarChartOpDesc]
    b.value shouldBe "score"
    b.fields shouldBe "name"
    b.categoryColumn shouldBe "cat"
    b.pattern shouldBe "texture"
    b.horizontalOrientation shouldBe true
  }

}
