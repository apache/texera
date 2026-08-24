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

package org.apache.texera.amber.operator.visualization.contourPlot

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ContourPlotOpDescSpec extends AnyFlatSpec with Matchers {

  "ContourPlotOpDesc.operatorInfo" should
    "advertise the name and Scientific visualization group" in {
    val info = (new ContourPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Contour Plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "ContourPlotOpDesc" should
    "default the x/y/z columns to empty, gridSize to unset and connectGaps to false" in {
    val d = new ContourPlotOpDesc
    d.x shouldBe ""
    d.y shouldBe ""
    d.z shouldBe ""
    d.gridSize shouldBe None
    d.connectGaps shouldBe false
  }

  "ContourPlotOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new ContourPlotOpDesc
    // getOutputSchemas ignores its input; pass empty to prove that.
    val out = op.getOutputSchemas(Map.empty)
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "ContourPlotOpDesc" should "round-trip its column fields through the polymorphic base" in {
    val d = new ContourPlotOpDesc
    d.x = "lon"
    d.y = "lat"
    d.z = "elev"
    d.gridSize = Some(20)
    d.connectGaps = true
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[ContourPlotOpDesc]
    val c = restored.asInstanceOf[ContourPlotOpDesc]
    c.x shouldBe "lon"
    c.y shouldBe "lat"
    c.z shouldBe "elev"
    c.gridSize shouldBe Some(20)
    c.connectGaps shouldBe true
  }

  /** Reads the shapes a stored workflow can hold; a round trip cannot cover them,
    * since it writes a number back. See GaugeChartStepsSpec for why `contentAs` is
    * what these pin.
    */
  private def readGridSize(json: String): Option[Int] =
    objectMapper
      .readValue(s"""{"operatorType":"ContourPlot"$json}""", classOf[LogicalOp])
      .asInstanceOf[ContourPlotOpDesc]
      .gridSize

  "ContourPlotOpDesc.gridSize" should "deserialize a JSON number" in {
    readGridSize(""","gridSize":20""") shouldBe Some(20)
  }

  it should "deserialize the numeric string a workflow saved before the field was numeric" in {
    readGridSize(""","gridSize":"20"""") shouldBe Some(20)
  }

  it should "read an absent, null or blank value as unset rather than as zero" in {
    readGridSize("") shouldBe None
    readGridSize(""","gridSize":null""") shouldBe None
    readGridSize(""","gridSize":""""") shouldBe None
  }

  it should "hold an Int, not the raw JSON value" in {
    // The ClassCastException surfaces here, at the first use, not at read time.
    readGridSize(""","gridSize":"20"""").map(_ + 1) shouldBe Some(21)
  }

  "ContourPlotOpDesc.generatePythonCode" should
    "assign the grid size as a number and fall back to the form's default" in {
    val d = new ContourPlotOpDesc
    d.x = "lon"
    d.y = "lat"
    d.z = "elev"
    d.coloringMethod = ContourPlotColoringFunction.HEATMAP
    d.generatePythonCode() should include("grid_size = 10")
    d.gridSize = Some(25)
    d.generatePythonCode() should include("grid_size = 25")
  }
}
