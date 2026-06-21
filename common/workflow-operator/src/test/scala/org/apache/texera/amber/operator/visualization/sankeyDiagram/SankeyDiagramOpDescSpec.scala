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

package org.apache.texera.amber.operator.visualization.sankeyDiagram

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SankeyDiagramOpDescSpec extends AnyFlatSpec with Matchers {

  "SankeyDiagramOpDesc.operatorInfo" should
    "advertise the name and Basic visualization group" in {
    val info = (new SankeyDiagramOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Sankey Diagram"
    info.operatorDescription shouldBe "Visualize data using a Sankey diagram"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "SankeyDiagramOpDesc" should
    "default sourceAttribute / targetAttribute / valueAttribute to the empty string" in {
    val d = new SankeyDiagramOpDesc
    d.sourceAttribute shouldBe ""
    d.targetAttribute shouldBe ""
    d.valueAttribute shouldBe ""
  }

  "SankeyDiagramOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new SankeyDiagramOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "SankeyDiagramOpDesc.generatePythonCode" should "emit a Plotly Sankey figure" in {
    val d = new SankeyDiagramOpDesc
    d.sourceAttribute = "src"
    d.targetAttribute = "dst"
    d.valueAttribute = "amount"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("go.Sankey(")
  }

  "SankeyDiagramOpDesc" should "round-trip its three attributes through the polymorphic base" in {
    val d = new SankeyDiagramOpDesc
    d.sourceAttribute = "src"
    d.targetAttribute = "dst"
    d.valueAttribute = "amount"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[SankeyDiagramOpDesc]
    val s = restored.asInstanceOf[SankeyDiagramOpDesc]
    s.sourceAttribute shouldBe "src"
    s.targetAttribute shouldBe "dst"
    s.valueAttribute shouldBe "amount"
  }
}
