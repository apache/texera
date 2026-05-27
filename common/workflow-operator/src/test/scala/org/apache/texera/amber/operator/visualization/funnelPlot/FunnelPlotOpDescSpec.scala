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

package org.apache.texera.amber.operator.visualization.funnelPlot

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FunnelPlotOpDescSpec extends AnyFlatSpec with Matchers {

  private def configured: FunnelPlotOpDesc = {
    val op = new FunnelPlotOpDesc
    op.x = "stage"
    op.y = "count"
    op
  }

  "FunnelPlotOpDesc.operatorInfo" should "advertise the user-friendly name and Financial group" in {
    val info = (new FunnelPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Funnel Plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP
    info.operatorDescription should include("Funnel")
  }

  it should "expose exactly one output port wired through forVisualization" in {
    (new FunnelPlotOpDesc).operatorInfo.outputPorts should have length 1
  }

  "FunnelPlotOpDesc.getOutputSchemas" should "return a single-port schema with an html-content STRING column" in {
    val op = configured
    val schemas = op.getOutputSchemas(Map.empty)
    schemas should have size 1
    val (portId, schema) = schemas.head
    portId shouldBe op.operatorInfo.outputPorts.head.id
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "html-content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  "FunnelPlotOpDesc.generatePythonCode" should "render a UDFTableOperator source with runtime decode sites for x and y" in {
    // EncodableString fields are NOT emitted as literal strings — the pyb
    // macro wraps them in `self.decode_python_template.decode("<base64>")`
    // calls. Each configured column becomes one decode site, so x + y must
    // produce at least two distinct decodes in the rendered source.
    val code = configured.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.express")
    val decodeOccurrences = "decode_python_template".r.findAllIn(code).length
    decodeOccurrences should be >= 2
  }

  it should "render the optional color argument only when color is configured" in {
    val without = configured.generatePythonCode()
    val withColor = {
      val op = configured
      op.color = "category"
      op.generatePythonCode()
    }
    without should not include "color="
    withColor should include("color=")
    // With color set, the rendered source has one extra decode site beyond
    // the two for x and y.
    val withDecodes = "decode_python_template".r.findAllIn(withColor).length
    val withoutDecodes = "decode_python_template".r.findAllIn(without).length
    withDecodes shouldBe withoutDecodes + 1
  }

  it should "fail-fast when required x/y are unset (the assert guards inside createPlotlyFigure)" in {
    // Pin: createPlotlyFigure asserts nonEmpty on both x and y. The fields
    // are initialized to "" so the assert path is reached (not the NPE path
    // that ImageVisualizerOpDesc hits).
    val op = new FunnelPlotOpDesc
    op.x = ""
    op.y = ""
    assertThrows[AssertionError](op.generatePythonCode())
  }
}
