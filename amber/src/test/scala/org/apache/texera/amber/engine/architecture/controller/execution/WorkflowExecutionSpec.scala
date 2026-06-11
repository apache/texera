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

package org.apache.texera.amber.engine.architecture.controller.execution

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowExecutionSpec extends AnyFlatSpec {

  private def physicalOpId(opId: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(opId), "main")

  private def op(opId: String): PhysicalOp =
    PhysicalOp(
      physicalOpId(opId),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    )

  /** A region with no ports — its `RegionExecution.getState` defaults to COMPLETED. */
  private def region(regionId: Long, opId: String): Region =
    Region(RegionIdentity(regionId), Set(op(opId)), Set.empty)

  "WorkflowExecution.initRegionExecution" should "create a new RegionExecution for the given region" in {
    val we = WorkflowExecution()
    val r = region(1, "a")

    val regionExecution = we.initRegionExecution(r)

    assert(regionExecution.region == r)
    assert(we.getRegionExecution(r.id) eq regionExecution)
  }

  it should "throw when called twice for the same region id" in {
    val we = WorkflowExecution()
    val r = region(1, "a")
    we.initRegionExecution(r)

    assertThrows[AssertionError] {
      we.initRegionExecution(r)
    }
  }

  "WorkflowExecution.hasRegionExecution" should "be false before init and true after" in {
    val we = WorkflowExecution()
    val r = region(1, "a")

    assert(!we.hasRegionExecution(r.id))
    we.initRegionExecution(r)
    assert(we.hasRegionExecution(r.id))
  }

  "WorkflowExecution.getRegionExecution" should "throw NoSuchElementException for an unknown region id" in {
    val we = WorkflowExecution()
    assertThrows[NoSuchElementException] {
      we.getRegionExecution(RegionIdentity(99))
    }
  }

  "WorkflowExecution.getAllRegionExecutions" should "preserve the insertion order of region executions" in {
    val we = WorkflowExecution()
    val r0 = region(0, "a")
    val r1 = region(1, "b")
    val r2 = region(2, "c")

    val e0 = we.initRegionExecution(r0)
    val e1 = we.initRegionExecution(r1)
    val e2 = we.initRegionExecution(r2)

    assert(we.getAllRegionExecutions.toList == List(e0, e1, e2))
  }

  "WorkflowExecution.restartRegionExecution" should "behave like a fresh init when no prior region execution exists" in {
    val we = WorkflowExecution()
    val r = region(1, "a")

    val regionExecution = we.restartRegionExecution(r)

    assert(we.hasRegionExecution(r.id))
    assert(we.getRegionExecution(r.id) eq regionExecution)
  }

  it should "replace an existing completed region execution with a fresh one" in {
    val we = WorkflowExecution()
    val r = region(1, "a")
    val original = we.initRegionExecution(r)
    assert(original.isCompleted)

    val replacement = we.restartRegionExecution(r)

    assert(replacement ne original)
    assert(we.getRegionExecution(r.id) eq replacement)
  }

  "WorkflowExecution.getRunningRegionExecutions" should "exclude completed region executions" in {
    val we = WorkflowExecution()
    val r = region(1, "a")
    val regionExecution = we.initRegionExecution(r)
    assert(regionExecution.isCompleted)

    assert(we.getRunningRegionExecutions.toList.isEmpty)
  }

  "WorkflowExecution.getState" should "return UNINITIALIZED when no regions have been initialized" in {
    val we = WorkflowExecution()
    assert(we.getState == WorkflowAggregatedState.UNINITIALIZED)
    assert(!we.isCompleted)
  }

  it should "return COMPLETED when every initialized region is completed" in {
    val we = WorkflowExecution()
    we.initRegionExecution(region(0, "a"))
    we.initRegionExecution(region(1, "b"))

    assert(we.getState == WorkflowAggregatedState.COMPLETED)
    assert(we.isCompleted)
  }

  "WorkflowExecution.getLatestOperatorExecutionOption" should "return None when no operator execution exists for the id" in {
    val we = WorkflowExecution()
    we.initRegionExecution(region(0, "a"))

    assert(we.getLatestOperatorExecutionOption(physicalOpId("never-initialized")).isEmpty)
  }

  it should "return the latest matching operator execution across regions" in {
    val we = WorkflowExecution()
    val regionA = we.initRegionExecution(region(0, "a"))
    val regionB = we.initRegionExecution(region(1, "b"))

    val olderExecution = regionA.initOperatorExecution(physicalOpId("a"))
    val newerExecution = regionB.initOperatorExecution(physicalOpId("a"))

    val result = we.getLatestOperatorExecutionOption(physicalOpId("a"))
    // Use reference identity: OperatorExecution is a no-field case class so
    // instances are structurally equal; only `eq` distinguishes them.
    assert(result.exists(_ eq newerExecution))
    assert(!result.exists(_ eq olderExecution))
  }
}
