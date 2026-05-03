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

package org.apache.texera.amber.operator.visualization.lineChart

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64

class LineChartOpDescSpec extends AnyFlatSpec with Matchers {

  private def lineConfig(x: String, y: String): LineConfig = {
    val c = new LineConfig
    c.xValue = x
    c.yValue = y
    c
  }

  private def configured: LineChartOpDesc = {
    val op = new LineChartOpDesc
    op.xLabel = "x_col"
    op.yLabel = "y_col"
    val ls = new util.ArrayList[LineConfig]()
    ls.add(lineConfig("x_col", "y_col"))
    op.lines = ls
    op
  }

  "LineChartOpDesc.operatorInfo" should "advertise the user-friendly name and Basic group" in {
    val info = (new LineChartOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Line Chart"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    info.operatorDescription should include("line chart")
  }

  it should "expose exactly one output port wired through forVisualization" in {
    (new LineChartOpDesc).operatorInfo.outputPorts should have length 1
  }

  "LineChartOpDesc.getOutputSchemas" should "return a single-port schema with an html-content STRING column" in {
    val op = configured
    val schemas = op.getOutputSchemas(Map.empty)
    schemas should have size 1
    val (portId, schema) = schemas.head
    portId shouldBe op.operatorInfo.outputPorts.head.id
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "html-content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  "LineChartOpDesc.generatePythonCode" should "render Python source with runtime decode sites for both labels" in {
    // Use distinct sentinels for the two LABELS *and* the LineConfig values
    // so the spec actually exercises both label fields. Asserting on the
    // exact base64 payloads proves each field was wrapped through
    // wrapWithPythonDecoderExpr individually — `decodeOccurrences >= 2`
    // could otherwise be satisfied by xValue/yValue alone.
    val op = new LineChartOpDesc
    op.xLabel = "X_LBL_SENT"
    op.yLabel = "Y_LBL_SENT"
    val ls = new util.ArrayList[LineConfig]()
    ls.add(lineConfig("X_VAL_SENT", "Y_VAL_SENT"))
    op.lines = ls

    val code = op.generatePythonCode()
    code should include("plotly")

    def b64(s: String): String =
      Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

    code should include(s"self.decode_python_template('${b64("X_LBL_SENT")}')")
    code should include(s"self.decode_python_template('${b64("Y_LBL_SENT")}')")
    // Raw sentinels must be absent from the encoded output — their presence
    // would mean the field was never run through the decoder wrapper.
    code should not include "X_LBL_SENT"
    code should not include "Y_LBL_SENT"
  }

  it should "raise NullPointerException when lines is left at its null default" in {
    // Pin: `var lines: util.List[LineConfig] = _` defaults to null, and
    // `createPlotlyFigure` calls `lines.asScala.map(...)` without a null
    // check. Calling `generatePythonCode` on a default-constructed LineChart
    // therefore throws NPE rather than rendering an empty chart or raising
    // an AssertionError. Documenting so a future fix that null-guards lines
    // breaks this spec deliberately.
    val op = new LineChartOpDesc
    assertThrows[NullPointerException](op.generatePythonCode())
  }

  it should "render code with an empty lines list (no NPE, no assertion)" in {
    val op = configured
    op.lines = new util.ArrayList[LineConfig]()
    val code = op.generatePythonCode()
    code should include("plotly")
    code should include("fig = go.Figure()")
  }
}
