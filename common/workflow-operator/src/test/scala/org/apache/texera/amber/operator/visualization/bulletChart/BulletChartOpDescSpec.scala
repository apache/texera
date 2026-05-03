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
    val code = configured.generatePythonCode()
    code should include("[]")
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

  it should "render a code block even with the default empty configuration (no assert guard)" in {
    // Pin: BulletChartOpDesc, unlike FunnelPlot / ImageVisualizer, has no
    // assert guards inside generatePythonCode. Empty defaults still produce
    // valid Python source. Pinning so a future tightening that adds asserts
    // breaks this spec deliberately.
    val op = new BulletChartOpDesc
    val code = op.generatePythonCode()
    code should include("plotly.graph_objects")
  }
}
