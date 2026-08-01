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
    op.deltaReference = Some(100)
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
    // The column name is an EncodableString and is NOT emitted as a literal
    // string — the pyb macro wraps it in a
    // `self.decode_python_template.decode("<base64>")` call. The numeric settings
    // carry no user text and are spliced as numbers, so they add no decode site.
    val code = configured.generatePythonCode()
    code should include("plotly.graph_objects")
    val decodeOccurrences = "decode_python_template".r.findAllIn(code).length
    decodeOccurrences should be >= 1
  }

  it should "assign the delta reference as a number, falling back to 0 when unset" in {
    configured.generatePythonCode() should include("delta_ref = 100.0")
    val unset = new BulletChartOpDesc
    unset.value = "actualValue"
    unset.generatePythonCode() should include("delta_ref = 0.0")
  }

  it should "assign None for a threshold that is not configured" in {
    configured.generatePythonCode() should include("threshold_val = None")
    val withThreshold = configured
    withThreshold.thresholdValue = Some(75.5)
    withThreshold.generatePythonCode() should include("threshold_val = 75.5")
  }

  it should "default to an empty steps list when none are configured" in {
    // The bullet-chart template ships with several unrelated `[]` literals
    // (`colors`, `valid_steps`, `step_errors`, `steps_list`, `html_chunks`), so a
    // bare `code should include("[]")` is too weak. Anchor on the argument the
    // generated code passes to generate_valid_steps so a regression that makes it
    // non-empty would actually fail the assertion.
    val code = configured.generatePythonCode()
    code should include regex """generate_valid_steps\(\[\]\)"""
  }

  it should "emit each configured step's bounds as numbers, dropping a half-filled step" in {
    val op = configured
    val steps: JList[BulletChartStepDefinition] = new util.ArrayList[BulletChartStepDefinition]()
    steps.add(new BulletChartStepDefinition(Some(0), Some(50)))
    steps.add(new BulletChartStepDefinition(Some(50), Some(100)))
    steps.add(new BulletChartStepDefinition(Some(100), None))
    op.steps = steps
    val code = op.generatePythonCode()
    code should include(
      """generate_valid_steps([{"start": 0.0, "end": 50.0}, {"start": 50.0, "end": 100.0}])"""
    )
    // The bounds are numbers now, so a step adds no runtime decode site.
    val baseDecodes = "decode_python_template".r.findAllIn(configured.generatePythonCode()).length
    "decode_python_template".r.findAllIn(code).length shouldBe baseDecodes
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

  "BulletChartOpDesc.generateStandaloneCode" should "reject empty required fields" in {
    val op = new BulletChartOpDesc
    assertThrows[AssertionError] {
      op.generateStandaloneCode()
    }
  }

  "BulletChartStepDefinition" should "initialize with start and end unset" in {
    val step = new BulletChartStepDefinition()
    step.start shouldBe None
    step.end shouldBe None
  }
}
