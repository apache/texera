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

package org.apache.texera.amber.operator.visualization.radarPlot

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class RadarPlotOpDescSpec extends AnyFlatSpec with Matchers {

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // EncodableString axes are always base64-wrapped in .encode mode
  // (self.decode_python_template('<base64>')), so assert on the base64 form only rather than
  // the raw column name, which could appear in the generated Python for unrelated reasons.
  private def carries(output: String, name: String): Boolean =
    output.contains(b64(name))

  "RadarPlotOpDesc.operatorInfo" should
    "advertise the name and Scientific visualization group with a 1-in/1-out shape" in {
    val info = (new RadarPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Radar Plot"
    info.operatorDescription shouldBe "View the result in a radar plot."
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "RadarPlotOpDesc" should "default the boolean flags to true and the optional columns to empty" in {
    val d = new RadarPlotOpDesc
    d.maxNormalize shouldBe true
    d.fillTrace shouldBe true
    d.showMarkers shouldBe true
    d.showLegend shouldBe true
    d.traceNameAttribute shouldBe ""
    d.traceColorAttribute shouldBe ""
    d.selectedAttributes shouldBe null
    d.linePattern shouldBe null
  }

  "RadarPlotOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new RadarPlotOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "RadarPlotOpDesc.generatePythonCode" should
    "reject a missing line pattern with a clear error" in {
    val d = new RadarPlotOpDesc
    d.selectedAttributes = List("m1", "m2")
    val ex = intercept[IllegalArgumentException](d.generatePythonCode())
    ex.getMessage should include("Line pattern must be specified")
  }

  it should "emit a Plotly Scatterpolar figure carrying the configured axes" in {
    val d = new RadarPlotOpDesc
    d.selectedAttributes = List("m1", "m2")
    d.linePattern = RadarPlotLinePattern.DASH
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("go.Scatterpolar")
    carries(code, "m1") shouldBe true
    carries(code, "m2") shouldBe true
  }

  "RadarPlotOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new RadarPlotOpDesc
    d.selectedAttributes = List("m1", "m2")
    d.traceNameAttribute = "name"
    d.traceColorAttribute = "color"
    d.linePattern = RadarPlotLinePattern.DASH
    d.maxNormalize = false
    d.fillTrace = false
    d.showMarkers = false
    d.showLegend = false
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[RadarPlotOpDesc]
    val r = restored.asInstanceOf[RadarPlotOpDesc]
    r.selectedAttributes shouldBe List("m1", "m2")
    r.traceNameAttribute shouldBe "name"
    r.traceColorAttribute shouldBe "color"
    r.linePattern shouldBe RadarPlotLinePattern.DASH
    r.maxNormalize shouldBe false
    r.fillTrace shouldBe false
    r.showMarkers shouldBe false
    r.showLegend shouldBe false
  }
}
