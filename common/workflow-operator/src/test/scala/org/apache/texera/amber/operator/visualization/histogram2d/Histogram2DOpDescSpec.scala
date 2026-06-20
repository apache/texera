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

package org.apache.texera.amber.operator.visualization.histogram2d

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Histogram2DOpDescSpec extends AnyFlatSpec with Matchers {

  private def configured(): Histogram2DOpDesc = {
    val d = new Histogram2DOpDesc
    d.xColumn = "x"
    d.yColumn = "y"
    d
  }

  "Histogram2DOpDesc.operatorInfo" should
    "advertise the name and Statistical visualization group" in {
    val info = (new Histogram2DOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Histogram2D"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "Histogram2DOpDesc" should "default bins to 10 and normalization to DENSITY" in {
    val d = new Histogram2DOpDesc
    d.xBins shouldBe 10
    d.yBins shouldBe 10
    d.normalize shouldBe NormalizationType.DENSITY
    d.xColumn shouldBe ""
    d.yColumn shouldBe ""
  }

  "Histogram2DOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new Histogram2DOpDesc
    val out = op.getOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> Schema()))
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "Histogram2DOpDesc.generatePythonCode" should "reject a non-positive bin count" in {
    val d = configured()
    d.xBins = 0
    intercept[AssertionError] {
      d.generatePythonCode()
    }
  }

  it should "emit a Plotly density-heatmap figure for a valid config" in {
    val code = configured().generatePythonCode()
    code should include("px.density_heatmap(")
    code should include("html-content")
  }

  "Histogram2DOpDesc" should "round-trip its fields through the polymorphic base" in {
    val d = configured()
    d.xBins = 20
    d.yBins = 5
    d.normalize = NormalizationType.PROBABILITY
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[Histogram2DOpDesc]
    val h = restored.asInstanceOf[Histogram2DOpDesc]
    h.xColumn shouldBe "x"
    h.yColumn shouldBe "y"
    h.xBins shouldBe 20
    h.yBins shouldBe 5
    h.normalize shouldBe NormalizationType.PROBABILITY
  }
}
