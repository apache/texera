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

package org.apache.texera.amber.engine.architecture.coordinator.execution

import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.scalatest.flatspec.AnyFlatSpec

class RegionExecutionSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private def physicalOpId(layered: String, layer: String = "main"): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(layered), layer)

  private def physicalLink(
      from: PhysicalOpIdentity,
      to: PhysicalOpIdentity,
      fromPort: Int = 0,
      toPort: Int = 0
  ): PhysicalLink =
    PhysicalLink(from, PortIdentity(fromPort), to, PortIdentity(toPort))

  private def channelId(from: String, to: String): ChannelIdentity =
    ChannelIdentity(ActorVirtualIdentity(from), ActorVirtualIdentity(to), isControl = false)

  /**
    * Empty `Region` — no operators, no links, no ports. Pinpointing the
    * `RegionExecution` behavior that does NOT touch `region.getPorts`
    * (init / get / has / getAll for operators & links, plus `getStats`)
    * does not need the heavy `PhysicalOp` fixture. `getState` /
    * `isCompleted` exercises the empty-ports path explicitly.
    */
  private def emptyRegion(id: Long = 0L): Region =
    Region(
      id = RegionIdentity(id),
      physicalOps = Set.empty,
      physicalLinks = Set.empty,
      ports = Set.empty
    )

  // ---------------------------------------------------------------------------
  // initOperatorExecution + getOperatorExecution + hasOperatorExecution
  // ---------------------------------------------------------------------------

  "RegionExecution.initOperatorExecution" should
    "register a fresh OperatorExecution under the requested opId and return it" in {
    val region = RegionExecution(emptyRegion())
    val opId = physicalOpId("op-a")
    val opExec = region.initOperatorExecution(opId)
    assert(opExec != null)
    assert(region.getOperatorExecution(opId) eq opExec)
    assert(region.hasOperatorExecution(opId))
  }

  it should "throw AssertionError if called twice for the same opId" in {
    val region = RegionExecution(emptyRegion())
    val opId = physicalOpId("op-a")
    region.initOperatorExecution(opId)
    assertThrows[AssertionError] {
      region.initOperatorExecution(opId)
    }
  }

  it should "deep-clone the inherited OperatorExecution (state mutations on inherited do NOT leak)" in {
    // Build an inherited OperatorExecution with one registered worker;
    // pass it as inheritOperatorExecution. The clone must be a distinct
    // instance — registering a NEW worker on the inherited copy must
    // not show up in the cloned region's OperatorExecution.
    val inherited = OperatorExecution()
    inherited.initWorkerExecution(ActorVirtualIdentity("inherited-worker"))

    val region = RegionExecution(emptyRegion())
    val opId = physicalOpId("op-a")
    val cloned = region.initOperatorExecution(opId, Some(inherited))

    assert(cloned ne inherited, "deep clone must be a distinct instance")
    assert(cloned.getWorkerIds == inherited.getWorkerIds)

    // Mutate the original — clone must not see the change.
    inherited.initWorkerExecution(ActorVirtualIdentity("post-clone-worker"))
    assert(!cloned.getWorkerIds.contains(ActorVirtualIdentity("post-clone-worker")))
  }

  "RegionExecution.hasOperatorExecution" should "return false for an unknown opId" in {
    val region = RegionExecution(emptyRegion())
    assert(!region.hasOperatorExecution(physicalOpId("missing")))
  }

  "RegionExecution.getAllOperatorExecutions" should
    "return every registered (opId, OperatorExecution) pair" in {
    val region = RegionExecution(emptyRegion())
    val opA = physicalOpId("op-a")
    val opB = physicalOpId("op-b")
    val execA = region.initOperatorExecution(opA)
    val execB = region.initOperatorExecution(opB)
    val all = region.getAllOperatorExecutions.toMap
    assert(all.keySet == Set(opA, opB))
    assert(all(opA) eq execA)
    assert(all(opB) eq execB)
  }

  // ---------------------------------------------------------------------------
  // initLinkExecution + getAllLinkExecutions
  // ---------------------------------------------------------------------------

  "RegionExecution.initLinkExecution" should
    "register a fresh LinkExecution for the requested link and return it" in {
    val region = RegionExecution(emptyRegion())
    val link = physicalLink(physicalOpId("op-a"), physicalOpId("op-b"))
    val linkExec = region.initLinkExecution(link)
    assert(linkExec != null)
    val all = region.getAllLinkExecutions.toMap
    assert(all.contains(link))
    assert(all(link) eq linkExec)
  }

  it should "throw AssertionError if called twice for the same link" in {
    val region = RegionExecution(emptyRegion())
    val link = physicalLink(physicalOpId("op-a"), physicalOpId("op-b"))
    region.initLinkExecution(link)
    assertThrows[AssertionError] {
      region.initLinkExecution(link)
    }
  }

  it should "track multiple distinct links independently" in {
    val region = RegionExecution(emptyRegion())
    val l1 = physicalLink(physicalOpId("op-a"), physicalOpId("op-b"))
    val l2 = physicalLink(physicalOpId("op-a"), physicalOpId("op-c"))
    val l3 = physicalLink(physicalOpId("op-b"), physicalOpId("op-c"))
    region.initLinkExecution(l1)
    region.initLinkExecution(l2)
    region.initLinkExecution(l3)
    assert(region.getAllLinkExecutions.toMap.keySet == Set(l1, l2, l3))
  }

  it should
    "produce LinkExecutions that can be mutated independently (channel init on one does not show in the other)" in {
    val region = RegionExecution(emptyRegion())
    val l1 = physicalLink(physicalOpId("op-a"), physicalOpId("op-b"))
    val l2 = physicalLink(physicalOpId("op-a"), physicalOpId("op-c"))
    val link1 = region.initLinkExecution(l1)
    val link2 = region.initLinkExecution(l2)
    link1.initChannelExecution(channelId("a", "b"))
    assert(link1.getAllChannelExecutions.size == 1)
    assert(link2.getAllChannelExecutions.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // getStats
  // ---------------------------------------------------------------------------

  "RegionExecution.getStats" should
    "return one OperatorMetrics per registered OperatorExecution, keyed by opId" in {
    val region = RegionExecution(emptyRegion())
    val opA = physicalOpId("op-a")
    val opB = physicalOpId("op-b")
    region.initOperatorExecution(opA)
    region.initOperatorExecution(opB)
    val stats = region.getStats
    assert(stats.keySet == Set(opA, opB))
    // With no workers under either OperatorExecution, the aggregated
    // state of each is UNINITIALIZED (per OperatorExecution.getState's
    // empty-iterable fallthrough).
    assert(stats(opA).operatorState == WorkflowAggregatedState.UNINITIALIZED)
    assert(stats(opB).operatorState == WorkflowAggregatedState.UNINITIALIZED)
  }

  it should "be empty when no OperatorExecution has been registered" in {
    val region = RegionExecution(emptyRegion())
    assert(region.getStats.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // getState / isCompleted
  // ---------------------------------------------------------------------------

  "RegionExecution.getState" should
    "return COMPLETED for a region with no ports (vacuous forall on getPorts)" in {
    // RegionExecution.getState iterates over region.getPorts; an empty
    // set vacuously satisfies the forall, so the region is COMPLETED.
    val region = RegionExecution(emptyRegion())
    assert(region.getState == WorkflowAggregatedState.COMPLETED)
  }

  "RegionExecution.isCompleted" should "be true when getState == COMPLETED" in {
    val region = RegionExecution(emptyRegion())
    assert(region.isCompleted)
  }

  // ---------------------------------------------------------------------------
  // Instance independence
  // ---------------------------------------------------------------------------
  //
  // Two RegionExecutions constructed from equal Regions hold their own
  // mutable operator / link maps. Registering an operator on one must not
  // be observable on the other — a regression here (e.g. accidentally
  // sharing the map via a `val` on the wrapping Region) would corrupt
  // coordinator bookkeeping across regions.

  "Two RegionExecutions wrapping equal Regions" should
    "hold independent operator-execution maps" in {
    val r1 = RegionExecution(emptyRegion(7L))
    val r2 = RegionExecution(emptyRegion(7L))
    r1.initOperatorExecution(physicalOpId("op-a"))
    assert(r1.hasOperatorExecution(physicalOpId("op-a")))
    assert(!r2.hasOperatorExecution(physicalOpId("op-a")))
  }
}
