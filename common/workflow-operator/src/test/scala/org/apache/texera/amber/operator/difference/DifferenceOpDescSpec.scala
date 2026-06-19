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

package org.apache.texera.amber.operator.difference

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  HashPartition,
  PortIdentity,
  SinglePartition,
  UnknownPartition
}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DifferenceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private val schemaA: Schema =
    Schema().add(new Attribute("col", AttributeType.STRING))
  private val schemaB: Schema =
    Schema().add(new Attribute("col", AttributeType.STRING))
  private val schemaDifferent: Schema =
    Schema().add(new Attribute("other", AttributeType.INTEGER))

  // ---------------------------------------------------------------------------
  // operatorInfo
  // ---------------------------------------------------------------------------

  "DifferenceOpDesc.operatorInfo" should "advertise the Set group + difference description" in {
    val info = (new DifferenceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Difference"
    info.operatorGroupName shouldBe OperatorGroupConstants.SET_GROUP
    info.operatorDescription.toLowerCase should include("difference")
  }

  it should
    "expose two input ports (left at PortIdentity 0, right at PortIdentity 1) and one blocking output" in {
    val info = (new DifferenceOpDesc).operatorInfo
    info.inputPorts should have length 2
    info.inputPorts.map(_.id) shouldBe List(PortIdentity(), PortIdentity(1))
    info.inputPorts.map(_.displayName) shouldBe List("left", "right")
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  // ---------------------------------------------------------------------------
  // getPhysicalOp — wiring + partitioning + schema propagation
  // ---------------------------------------------------------------------------

  "DifferenceOpDesc.getPhysicalOp" should
    "wire the DifferenceOpExec class name into the OpExecInitInfo" in {
    val physical = (new DifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.difference.DifferenceOpExec"
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "require HashPartition on BOTH input ports" in {
    // Set-difference semantics require both inputs to be hash-aligned so
    // matching keys can be compared on the same worker.
    val physical = (new DifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    physical.partitionRequirement shouldBe List(
      Option(HashPartition()),
      Option(HashPartition())
    )
  }

  it should "derive HashPartition for the output regardless of input partition kinds" in {
    val physical = (new DifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    physical.derivePartition(List(SinglePartition(), UnknownPartition())) shouldBe HashPartition()
    physical.derivePartition(
      List(HashPartition(List("a")), HashPartition(List("b")))
    ) shouldBe HashPartition()
  }

  // ---------------------------------------------------------------------------
  // Schema propagation
  // ---------------------------------------------------------------------------

  "DifferenceOpDesc schema propagation" should
    "produce a single output schema equal to the (shared) input schema" in {
    // When both inputs report the same schema, propagation succeeds and
    // every output port receives that schema.
    val op = new DifferenceOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    val propagateFn = physical.propagateSchema
    val inputs = Map(PortIdentity() -> schemaA, PortIdentity(1) -> schemaB)
    val outputs = propagateFn.func(inputs)
    outputs.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    outputs.values.toSet shouldBe Set(schemaA)
  }

  it should
    "throw IllegalArgumentException when the two inputs do not share one schema" in {
    val physical = (new DifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    val propagateFn = physical.propagateSchema
    val mismatched =
      Map(PortIdentity() -> schemaA, PortIdentity(1) -> schemaDifferent)
    intercept[IllegalArgumentException] {
      propagateFn.func(mismatched)
    }
  }

  // ---------------------------------------------------------------------------
  // Independent instances
  // ---------------------------------------------------------------------------

  "DifferenceOpDesc" should
    "assign a fresh operatorIdentifier per instance (UUID-based id is not shared)" in {
    // `LogicalOp` initializes `operatorId` from `UUID.randomUUID()` in
    // its constructor body, so two `new DifferenceOpDesc` allocations
    // must hold different identifiers. A regression to a static /
    // shared id would surface here as the two ids being equal.
    val a = new DifferenceOpDesc
    val b = new DifferenceOpDesc
    a.operatorIdentifier should not equal b.operatorIdentifier
  }
}
