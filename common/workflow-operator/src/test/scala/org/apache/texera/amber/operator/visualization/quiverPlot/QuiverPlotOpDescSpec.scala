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

package org.apache.texera.amber.operator.visualization.quiverPlot

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class QuiverPlotOpDescSpec extends AnyFlatSpec with Matchers {

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // EncodableString columns are always base64-wrapped in .encode mode
  // (self.decode_python_template('<base64>')), so assert on the base64 form only rather than
  // the raw column name, which could appear in the generated Python for unrelated reasons.
  private def carries(output: String, name: String): Boolean =
    output.contains(b64(name))

  "QuiverPlotOpDesc.operatorInfo" should
    "advertise the name and Scientific visualization group with a 1-in/1-out shape" in {
    val info = (new QuiverPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Quiver Plot"
    info.operatorDescription shouldBe "Visualize vector data in a Quiver Plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "QuiverPlotOpDesc" should "default the x/y/u/v columns to empty strings" in {
    val d = new QuiverPlotOpDesc
    d.x shouldBe ""
    d.y shouldBe ""
    d.u shouldBe ""
    d.v shouldBe ""
  }

  "QuiverPlotOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new QuiverPlotOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "QuiverPlotOpDesc.generatePythonCode" should
    "emit a Plotly figure-factory quiver carrying the configured columns" in {
    val d = new QuiverPlotOpDesc
    d.x = "vx"
    d.y = "vy"
    d.u = "vu"
    d.v = "vv"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("ff.create_quiver(")
    carries(code, "vx") shouldBe true
    carries(code, "vy") shouldBe true
    carries(code, "vu") shouldBe true
    carries(code, "vv") shouldBe true
  }

  "QuiverPlotOpDesc" should "round-trip its columns through the polymorphic base" in {
    val d = new QuiverPlotOpDesc
    d.x = "vx"
    d.y = "vy"
    d.u = "vu"
    d.v = "vv"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[QuiverPlotOpDesc]
    val q = restored.asInstanceOf[QuiverPlotOpDesc]
    q.x shouldBe "vx"
    q.y shouldBe "vy"
    q.u shouldBe "vu"
    q.v shouldBe "vv"
  }
}
