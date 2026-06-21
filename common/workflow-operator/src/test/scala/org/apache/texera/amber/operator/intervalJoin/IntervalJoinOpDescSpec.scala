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

package org.apache.texera.amber.operator.intervalJoin

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{HashPartition, PortIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IntervalJoinOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // Set the join keys + a concrete (non-null) Option for timeIntervalType before any
  // path that serializes `this` (getPhysicalOp / round-trip).
  private def configured(): IntervalJoinOpDesc = {
    val d = new IntervalJoinOpDesc
    d.leftAttributeName = "lk"
    d.rightAttributeName = "rk"
    d.timeIntervalType = None
    d
  }

  "IntervalJoinOpDesc.operatorInfo" should
    "advertise two ordered inputs (left then right) in the Join group" in {
    val info = (new IntervalJoinOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Interval Join"
    info.operatorGroupName shouldBe OperatorGroupConstants.JOIN_GROUP
    info.inputPorts should have length 2
    info.inputPorts.head.id shouldBe PortIdentity()
    info.inputPorts.head.displayName shouldBe "left table"
    info.inputPorts.last.id shouldBe PortIdentity(1)
    info.inputPorts.last.displayName shouldBe "right table"
    info.inputPorts.last.dependencies shouldBe List(PortIdentity(0))
    info.outputPorts should have length 1
  }

  "IntervalJoinOpDesc" should
    "default the join-key attributes to null and the bounds/constant to their defaults" in {
    val d = new IntervalJoinOpDesc
    d.leftAttributeName shouldBe null
    d.rightAttributeName shouldBe null
    d.constant shouldBe 10L
    d.includeLeftBound shouldBe true
    d.includeRightBound shouldBe true
  }

  "IntervalJoinOpDesc.getPhysicalOp" should
    "wire IntervalJoinOpExec, carry port identities, and require HashPartition on each join key" in {
    val op = configured()
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.intervalJoin.IntervalJoinOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    physical.partitionRequirement shouldBe List(
      Option(HashPartition(List("lk"))),
      Option(HashPartition(List("rk")))
    )
  }

  "IntervalJoinOpDesc schema propagation" should
    "merge the left and right schemas, suffixing a conflicting attribute with #@1" in {
    val op = configured()
    val physical = op.getPhysicalOp(workflowId, executionId)
    val leftSchema = Schema()
      .add(new Attribute("a", AttributeType.STRING))
      .add(new Attribute("k", AttributeType.LONG))
    val rightSchema = Schema()
      .add(new Attribute("b", AttributeType.STRING))
      .add(new Attribute("k", AttributeType.LONG))
    val out = physical.propagateSchema.func(
      Map(PortIdentity() -> leftSchema, PortIdentity(1) -> rightSchema)
    )
    out.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    out(op.operatorInfo.outputPorts.head.id).getAttributes.map(_.getName) shouldBe
      List("a", "k", "b", "k#@1")
  }

  "IntervalJoinOpDesc" should "round-trip its fields through the polymorphic base" in {
    val d = configured()
    d.constant = 42L
    d.includeLeftBound = false
    d.includeRightBound = false
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[IntervalJoinOpDesc]
    val ij = restored.asInstanceOf[IntervalJoinOpDesc]
    ij.leftAttributeName shouldBe "lk"
    ij.rightAttributeName shouldBe "rk"
    ij.constant shouldBe 42L
    ij.includeLeftBound shouldBe false
    ij.includeRightBound shouldBe false
  }
}
