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

package org.apache.texera.amber.operator.union

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UnionOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // ---------------------------------------------------------------------------
  // operatorInfo — descriptor metadata
  // ---------------------------------------------------------------------------

  "UnionOpDesc.operatorInfo" should "advertise the user-friendly name and Set group" in {
    val info = (new UnionOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Union"
    info.operatorGroupName shouldBe OperatorGroupConstants.SET_GROUP
    info.operatorDescription should include("Union")
  }

  it should "expose exactly one input port and one (non-blocking) output port" in {
    val info = (new UnionOpDesc).operatorInfo
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe false
  }

  // ---------------------------------------------------------------------------
  // getPhysicalOp — wiring to UnionOpExec
  // ---------------------------------------------------------------------------

  "UnionOpDesc.getPhysicalOp" should
    "wire the UnionOpExec class name into the OpExecInitInfo" in {
    val op = new UnionOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.union.UnionOpExec"
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "expose the same input/output port shape as operatorInfo" in {
    val op = new UnionOpDesc
    val info = op.operatorInfo
    val physical = op.getPhysicalOp(workflowId, executionId)
    // `physical.inputPorts` / `outputPorts` are `Map`s — compare `size`
    // (Int) directly; the descriptor's `operatorInfo.*.size` is also an
    // Int, so no Long coercion is needed.
    assert(physical.inputPorts.size == info.inputPorts.size)
    assert(physical.outputPorts.size == info.outputPorts.size)
  }

  it should "leave the partition requirement empty (no hash-alignment forced)" in {
    // Unlike Distinct / Difference / Intersect in the same SET group,
    // Union does NOT require its inputs to be hash-partitioned — the
    // pass-through executor preserves whatever the upstream produced.
    //
    // Assert on the list itself (not just `.flatten`) so a regression
    // that introduced a `None` entry (`List(None)` — same "no
    // requirement" semantics but a different list shape) is caught here.
    val physical = (new UnionOpDesc).getPhysicalOp(workflowId, executionId)
    physical.partitionRequirement shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // Independent instances
  // ---------------------------------------------------------------------------

  "UnionOpDesc" should
    "assign a fresh operatorIdentifier per instance (UUID-based id is not shared)" in {
    // `LogicalOp` initializes `operatorId` from `UUID.randomUUID()` in
    // its constructor body, so two `new UnionOpDesc` allocations must
    // hold different identifiers. A regression to a static / shared id
    // would surface here as the two ids being equal.
    val a = new UnionOpDesc
    val b = new UnionOpDesc
    a.operatorIdentifier should not equal b.operatorIdentifier
  }
}
