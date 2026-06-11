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

package org.apache.texera.amber.operator.visualization.heatMap

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HeatMapOpDescSpec extends AnyFlatSpec with Matchers {

  private def configured: HeatMapOpDesc = {
    val op = new HeatMapOpDesc
    op.x = "ax"
    op.y = "ay"
    op.value = "v"
    op
  }

  "HeatMapOpDesc.operatorInfo" should "advertise the user-friendly name and Scientific group" in {
    val info = (new HeatMapOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Heatmap"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.operatorDescription should include("HeatMap")
  }

  it should "expose exactly one output port wired through forVisualization" in {
    (new HeatMapOpDesc).operatorInfo.outputPorts should have length 1
  }

  "HeatMapOpDesc.getOutputSchemas" should "return a single-port schema with an html-content STRING column" in {
    val op = configured
    val schemas = op.getOutputSchemas(Map.empty)
    schemas should have size 1
    val (portId, schema) = schemas.head
    portId shouldBe op.operatorInfo.outputPorts.head.id
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "html-content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  "HeatMapOpDesc.generatePythonCode" should "render a UDFTableOperator source with three runtime decode sites for x/y/value" in {
    // EncodableString fields are wrapped in `self.decode_python_template(...)`
    // calls by the pyb macro; pin a structural count instead of literal names.
    val code = configured.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.graph_objects")
    val decodeOccurrences = "decode_python_template".r.findAllIn(code).length
    decodeOccurrences should be >= 3
  }

  it should "fail-fast when any required field is unset (asserts inside createHeatMap)" in {
    // createHeatMap asserts nonEmpty on x, y, AND value. Empty defaults
    // ("") hit the assert path and surface as AssertionError.
    val op = new HeatMapOpDesc
    assertThrows[AssertionError](op.generatePythonCode())
  }
}
