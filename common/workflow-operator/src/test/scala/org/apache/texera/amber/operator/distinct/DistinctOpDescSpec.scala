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

package org.apache.texera.amber.operator.distinct

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{HashPartition, SinglePartition, UnknownPartition}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DistinctOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // ---------------------------------------------------------------------------
  // operatorInfo — descriptor metadata
  // ---------------------------------------------------------------------------

  "DistinctOpDesc.operatorInfo" should
    "advertise the user-friendly name and Cleaning group" in {
    val info = (new DistinctOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Distinct"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.operatorDescription.toLowerCase should include("duplicate")
  }

  it should "expose one input port and one blocking output port" in {
    val info = (new DistinctOpDesc).operatorInfo
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  // ---------------------------------------------------------------------------
  // getPhysicalOp — wiring to DistinctOpExec + partitioning contract
  // ---------------------------------------------------------------------------

  "DistinctOpDesc.getPhysicalOp" should
    "wire the DistinctOpExec class name into the OpExecInitInfo" in {
    val op = new DistinctOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.distinct.DistinctOpExec"
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "require HashPartition on the single input port" in {
    val physical = (new DistinctOpDesc).getPhysicalOp(workflowId, executionId)
    physical.partitionRequirement shouldBe List(Option(HashPartition()))
  }

  it should "always derive HashPartition for the output regardless of input partitions" in {
    // Distinct's dedup semantics depend on hash-alignment, so the
    // derived output partition stays hash even when upstream inputs
    // report differing partition kinds.
    val physical = (new DistinctOpDesc).getPhysicalOp(workflowId, executionId)
    physical.derivePartition(List(SinglePartition())) shouldBe HashPartition()
    physical.derivePartition(List(UnknownPartition())) shouldBe HashPartition()
    physical.derivePartition(List(HashPartition(List("col-a")))) shouldBe HashPartition()
  }

  it should "preserve the one input / one blocking output port shape from operatorInfo" in {
    val op = new DistinctOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.inputPorts should have size 1
    physical.outputPorts should have size 1
    // PhysicalOp.outputPorts is a Map[PortIdentity, (OutputPort, …, …)],
    // so the blocking flag is on the first tuple element of the value.
    val (outputPort, _, _) = physical.outputPorts.values.head
    outputPort.blocking shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Independent instances
  // ---------------------------------------------------------------------------

  "DistinctOpDesc" should
    "assign a fresh operatorIdentifier per instance (UUID-based id is not shared)" in {
    // `LogicalOp` initializes `operatorId` from `UUID.randomUUID()` in
    // its constructor body, so two `new DistinctOpDesc` allocations
    // must hold different identifiers. A regression to a static /
    // shared id would surface here as the two ids being equal.
    val a = new DistinctOpDesc
    val b = new DistinctOpDesc
    a.operatorIdentifier should not equal b.operatorIdentifier
  }
}
