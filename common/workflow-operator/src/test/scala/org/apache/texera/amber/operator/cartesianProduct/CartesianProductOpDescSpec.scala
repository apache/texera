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

package org.apache.texera.amber.operator.cartesianProduct

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CartesianProductOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "CartesianProductOpDesc.operatorInfo" should
    "advertise the Cartesian Product in the Join group with a left/right 2-in 1-out shape" in {
    val info = (new CartesianProductOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Cartesian Product"
    info.operatorDescription shouldBe
      "Append fields together to get the cartesian product of two inputs"
    info.operatorGroupName shouldBe OperatorGroupConstants.JOIN_GROUP
    info.inputPorts.map(_.displayName) shouldBe List("left", "right")
    info.outputPorts should have length 1
  }

  "CartesianProductOpDesc.getExternalOutputSchemas" should
    "concatenate the two input schemas, renaming right-side duplicates" in {
    val d = new CartesianProductOpDesc
    val left = Schema()
      .add(new Attribute("a", AttributeType.STRING))
      .add(new Attribute("k", AttributeType.LONG))
    val right = Schema()
      .add(new Attribute("b", AttributeType.STRING))
      .add(new Attribute("k", AttributeType.LONG))
    val out = d.getExternalOutputSchemas(
      Map(PortIdentity() -> left, PortIdentity(1) -> right)
    )
    out(d.operatorInfo.outputPorts.head.id).getAttributeNames shouldBe List("a", "k", "b", "k#@1")
  }

  it should "keep both schemas intact when there are no name clashes" in {
    val d = new CartesianProductOpDesc
    val left = Schema().add(new Attribute("a", AttributeType.STRING))
    val right = Schema().add(new Attribute("b", AttributeType.STRING))
    val out = d.getExternalOutputSchemas(
      Map(PortIdentity() -> left, PortIdentity(1) -> right)
    )
    out(d.operatorInfo.outputPorts.head.id).getAttributeNames shouldBe List("a", "b")
  }

  "CartesianProductOpDesc.getPhysicalOp" should
    "wire the Cartesian Product exec with two input ports and one output port" in {
    val d = new CartesianProductOpDesc
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.cartesianProduct.CartesianProductOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "CartesianProductOpDesc" should "round-trip through the polymorphic base" in {
    val d = new CartesianProductOpDesc
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"CartesianProduct\"")
    objectMapper.readValue(json, classOf[LogicalOp]) shouldBe a[CartesianProductOpDesc]
  }
}
