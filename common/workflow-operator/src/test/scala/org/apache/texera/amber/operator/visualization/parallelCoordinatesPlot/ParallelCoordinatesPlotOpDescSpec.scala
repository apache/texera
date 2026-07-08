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

package org.apache.texera.amber.operator.visualization.parallelCoordinatesPlot

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParallelCoordinatesPlotOpDescSpec extends AnyFlatSpec with Matchers {

  "ParallelCoordinatesPlotOpDesc.operatorInfo" should
    "advertise the name and Scientific visualization group" in {
    val info = (new ParallelCoordinatesPlotOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Parallel Coordinates Plot"
    info.operatorDescription shouldBe "Visualize multivariate data using parallel coordinate axes"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "ParallelCoordinatesPlotOpDesc" should "default dimensions to an empty list" in {
    (new ParallelCoordinatesPlotOpDesc).dimensions shouldBe empty
  }

  "ParallelCoordinatesPlotOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new ParallelCoordinatesPlotOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "ParallelCoordinatesPlotOpDesc.generatePythonCode" should
    "emit a Plotly parallel_coordinates figure (even with no dimensions / null color)" in {
    // color defaults to null and dimensions to empty; both are guarded in codegen.
    val code = (new ParallelCoordinatesPlotOpDesc).generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("px.parallel_coordinates(")
  }

  "ParallelCoordinatesPlotOpDesc" should
    "round-trip dimensions and color through the polymorphic base" in {
    val d = new ParallelCoordinatesPlotOpDesc
    d.dimensions = List("d1", "d2")
    d.color = "grp"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[ParallelCoordinatesPlotOpDesc]
    val p = restored.asInstanceOf[ParallelCoordinatesPlotOpDesc]
    p.dimensions shouldBe List("d1", "d2")
    p.color shouldBe "grp"
  }
}
