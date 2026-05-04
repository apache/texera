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

package org.apache.texera.amber.operator.visualization.bulletChart

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.{List => JList}

class BulletChartOpDescSpec extends AnyFlatSpec with Matchers {

  private def configured: BulletChartOpDesc = {
    val op = new BulletChartOpDesc
    op.value = "actualValue"
    op.deltaReference = "100"
    op
  }

  "BulletChartOpDesc.operatorInfo" should "advertise the user-friendly name and Financial group" in {
    val info = (new BulletChartOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Bullet Chart"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP
    info.operatorDescription should include("Bullet Chart")
  }

  it should "expose exactly one output port wired through forVisualization" in {
    (new BulletChartOpDesc).operatorInfo.outputPorts should have length 1
  }

  "BulletChartOpDesc.getOutputSchemas" should "return a single-port schema with an html-content STRING column" in {
    val op = configured
    val schemas = op.getOutputSchemas(Map.empty)
    schemas should have size 1
    val (portId, schema) = schemas.head
    portId shouldBe op.operatorInfo.outputPorts.head.id
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "html-content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  "BulletChartOpDesc.generatePythonCode" should "render Python source with a runtime decode site for the value column" in {
    // EncodableString fields are NOT emitted as literal strings — the pyb
    // macro wraps them in `self.decode_python_template.decode("<base64>")`
    // calls. The rendered source must reference the decoder symbol at least
    // for `value` and `deltaReference`.
    val code = configured.generatePythonCode()
    code should include("plotly.graph_objects")
    val decodeOccurrences = "decode_python_template".r.findAllIn(code).length
    decodeOccurrences should be >= 2
  }

  it should "default to an empty steps list when none are configured" in {
    // The bullet-chart template ships with several unrelated `[]` literals
    // (`colors`, `valid_steps`, `step_errors`, `steps_list`, `html_chunks`),
    // so a bare `code should include("[]")` is too weak. Anchor on the
    // generated `steps_data = ...` literal directly so a regression that
    // makes it non-empty would actually fail the assertion.
    val code = configured.generatePythonCode()
    code should include regex """steps_data\s*=\s*\[\]"""
  }

  it should "include each configured step's start/end JSON keys with extra decode sites" in {
    val op = configured
    val steps: JList[BulletChartStepDefinition] = new util.ArrayList[BulletChartStepDefinition]()
    steps.add(new BulletChartStepDefinition("0", "50"))
    steps.add(new BulletChartStepDefinition("50", "100"))
    op.steps = steps
    val code = op.generatePythonCode()
    code should include("\"start\":")
    code should include("\"end\":")
    // Two steps × 2 EncodableString fields each = 4 extra decode sites on
    // top of the value/deltaReference decodes from the base configuration.
    val baseDecodes = "decode_python_template".r.findAllIn(configured.generatePythonCode()).length
    val withSteps = "decode_python_template".r.findAllIn(code).length
    withSteps shouldBe baseDecodes + 4
  }

  it should "currently render a code block even with the default empty configuration (no assert guard)" in {
    // Documents the present behavior: BulletChartOpDesc has no assert
    // guards inside generatePythonCode, so empty defaults still produce
    // syntactically valid Python source. The intended contract lives in
    // the pendingUntilFixed test below.
    val op = new BulletChartOpDesc
    val code = op.generatePythonCode()
    code should include("plotly.graph_objects")
  }

  it should "eventually reject empty required value/deltaReference like FunnelPlot/ImageVisualizer (pendingUntilFixed)" in pendingUntilFixed {
    // Intended contract: `value` and `deltaReference` are marked required
    // on `BulletChartOpDesc`, so generatePythonCode on a default-constructed
    // instance should raise instead of rendering empty-string column refs.
    // Using pendingUntilFixed so a future validation fix flips this test
    // from Pending to a deliberate failure and forces removal of the marker.
    val op = new BulletChartOpDesc
    intercept[RuntimeException] {
      op.generatePythonCode()
    }
  }
}
