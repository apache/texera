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

package org.apache.texera.amber.operator.visualization.htmlviz

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HtmlVizOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "HtmlVizOpDesc.operatorInfo" should "advertise the name and Media visualization group" in {
    val info = (new HtmlVizOpDesc).operatorInfo
    info.userFriendlyName shouldBe "HTML Visualizer"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_MEDIA_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "HtmlVizOpDesc.htmlContentAttrName" should "default to the empty string" in {
    (new HtmlVizOpDesc).htmlContentAttrName shouldBe ""
  }

  "HtmlVizOpDesc.getPhysicalOp" should "wire HtmlVizOpExec and carry port identities" in {
    val op = new HtmlVizOpDesc
    op.htmlContentAttrName = "html"
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.visualization.htmlviz.HtmlVizOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "HtmlVizOpDesc schema propagation" should
    "emit a single html-content STRING column keyed by the declared output port" in {
    val op = new HtmlVizOpDesc
    op.htmlContentAttrName = "html"
    val input = Schema().add(new Attribute("html", AttributeType.STRING))
    val out = op.getExternalOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }
}
