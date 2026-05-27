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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalOp,
  PortIdentity
}
import org.scalatest.flatspec.AnyFlatSpec

class RegionPlanSpec extends AnyFlatSpec {

  private def physicalOpId(opId: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(opId), "main")

  private def op(opId: String): PhysicalOp =
    PhysicalOp(
      physicalOpId(opId),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    )

  private def link(fromOp: String, toOp: String): PhysicalLink =
    PhysicalLink(physicalOpId(fromOp), PortIdentity(0), physicalOpId(toOp), PortIdentity(0))

  private def globalPort(opId: String): GlobalPortIdentity =
    GlobalPortIdentity(physicalOpId(opId), PortIdentity(0), input = true)

  "RegionPlan.getRegion" should "return the region with the given id" in {
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty)
    val r1 = Region(RegionIdentity(1), Set(op("b")), Set.empty)
    val plan = RegionPlan(Set(r0, r1), Set.empty)

    assert(plan.getRegion(RegionIdentity(0)) == r0)
    assert(plan.getRegion(RegionIdentity(1)) == r1)
  }

  it should "throw NoSuchElementException for an unknown region id" in {
    val plan = RegionPlan(Set(Region(RegionIdentity(0), Set(op("a")), Set.empty)), Set.empty)
    assertThrows[NoSuchElementException] {
      plan.getRegion(RegionIdentity(99))
    }
  }

  "RegionPlan.getRegionOfLink" should "return the region whose physicalLinks include the link" in {
    val ab = link("a", "b")
    val r0 = Region(RegionIdentity(0), Set(op("a"), op("b")), Set(ab))
    val r1 = Region(RegionIdentity(1), Set(op("c")), Set.empty)
    val plan = RegionPlan(Set(r0, r1), Set.empty)

    assert(plan.getRegionOfLink(ab) == r0)
  }

  it should "throw NoSuchElementException when no region claims the link" in {
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty)
    val plan = RegionPlan(Set(r0), Set.empty)
    assertThrows[NoSuchElementException] {
      plan.getRegionOfLink(link("a", "missing"))
    }
  }

  "RegionPlan.getRegionOfPortId" should "find the region whose ports contain the global port id" in {
    val portA = globalPort("a")
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty, ports = Set(portA))
    val r1 = Region(RegionIdentity(1), Set(op("b")), Set.empty)
    val plan = RegionPlan(Set(r0, r1), Set.empty)

    assert(plan.getRegionOfPortId(portA).contains(r0))
  }

  it should "return None when no region claims the port" in {
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty)
    val plan = RegionPlan(Set(r0), Set.empty)

    assert(plan.getRegionOfPortId(globalPort("missing")).isEmpty)
  }

  "RegionPlan.topologicalIterator" should "yield region ids in topological order based on regionLinks" in {
    val r0 = Region(RegionIdentity(0), Set(op("a")), Set.empty)
    val r1 = Region(RegionIdentity(1), Set(op("b")), Set.empty)
    val r2 = Region(RegionIdentity(2), Set(op("c")), Set.empty)
    val plan = RegionPlan(
      regions = Set(r0, r1, r2),
      regionLinks = Set(
        RegionLink(r0.id, r1.id),
        RegionLink(r1.id, r2.id)
      )
    )

    assert(plan.topologicalIterator().toList == List(r0.id, r1.id, r2.id))
  }

  // ---------------------------------------------------------------------------
  // Larger / more complex region plan exercises
  // ---------------------------------------------------------------------------

  /**
    * Build a "diamond" of regions:
    *
    *         src
    *        /   \
    *      mid1  mid2  mid3 (all parallel siblings of src)
    *        \   /
    *         sink
    *
    * src fans out to three middle regions; all three middle regions feed a
    * single sink. Each region carries multiple operators and multiple links.
    */
  private def buildDiamondPlan(): RegionPlan = {
    val src = Region(
      RegionIdentity(0),
      physicalOps = Set(op("src-1"), op("src-2"), op("src-3")),
      physicalLinks = Set(link("src-1", "src-2"), link("src-2", "src-3"))
    )
    val mid1 = Region(
      RegionIdentity(1),
      physicalOps = Set(op("mid1-1"), op("mid1-2")),
      physicalLinks = Set(link("mid1-1", "mid1-2")),
      ports = Set(globalPort("mid1-1"))
    )
    val mid2 = Region(
      RegionIdentity(2),
      physicalOps = Set(op("mid2-1")),
      physicalLinks = Set.empty,
      ports = Set(globalPort("mid2-1"))
    )
    val mid3 = Region(
      RegionIdentity(3),
      physicalOps = Set(op("mid3-1"), op("mid3-2"), op("mid3-3"), op("mid3-4")),
      physicalLinks = Set(
        link("mid3-1", "mid3-2"),
        link("mid3-2", "mid3-3"),
        link("mid3-3", "mid3-4")
      )
    )
    val sink = Region(
      RegionIdentity(4),
      physicalOps = Set(op("sink-1"), op("sink-2")),
      physicalLinks = Set(link("sink-1", "sink-2")),
      ports = Set(globalPort("sink-1"))
    )
    RegionPlan(
      regions = Set(src, mid1, mid2, mid3, sink),
      regionLinks = Set(
        RegionLink(src.id, mid1.id),
        RegionLink(src.id, mid2.id),
        RegionLink(src.id, mid3.id),
        RegionLink(mid1.id, sink.id),
        RegionLink(mid2.id, sink.id),
        RegionLink(mid3.id, sink.id)
      )
    )
  }

  "RegionPlan (diamond fan-out / fan-in)" should "look up every region by id" in {
    val plan = buildDiamondPlan()
    val ids = (0L to 4L).map(RegionIdentity).toList
    ids.foreach(id => assert(plan.getRegion(id).id == id))
  }

  it should "find the region containing each physical link across multiple regions" in {
    val plan = buildDiamondPlan()
    // src has 2 internal links, mid1 has 1, mid3 has 3, sink has 1 → 7 internal links total.
    val internalLinks = Seq(
      ("src-1", "src-2", RegionIdentity(0)),
      ("src-2", "src-3", RegionIdentity(0)),
      ("mid1-1", "mid1-2", RegionIdentity(1)),
      ("mid3-1", "mid3-2", RegionIdentity(3)),
      ("mid3-2", "mid3-3", RegionIdentity(3)),
      ("mid3-3", "mid3-4", RegionIdentity(3)),
      ("sink-1", "sink-2", RegionIdentity(4))
    )
    internalLinks.foreach {
      case (from, to, expectedRegion) =>
        assert(plan.getRegionOfLink(link(from, to)).id == expectedRegion)
    }
  }

  it should "find each port-bearing region by its global port id" in {
    val plan = buildDiamondPlan()
    assert(plan.getRegionOfPortId(globalPort("mid1-1")).map(_.id).contains(RegionIdentity(1)))
    assert(plan.getRegionOfPortId(globalPort("mid2-1")).map(_.id).contains(RegionIdentity(2)))
    assert(plan.getRegionOfPortId(globalPort("sink-1")).map(_.id).contains(RegionIdentity(4)))
    // Unknown port → None.
    assert(plan.getRegionOfPortId(globalPort("not-in-plan")).isEmpty)
  }

  it should "produce a topological ordering with src first, sink last, and middles in the middle" in {
    val plan = buildDiamondPlan()
    val order = plan.topologicalIterator().toList
    assert(order.size == 5)
    assert(order.head == RegionIdentity(0), "src must come first")
    assert(order.last == RegionIdentity(4), "sink must come last")
    assert(order.slice(1, 4).toSet == Set(RegionIdentity(1), RegionIdentity(2), RegionIdentity(3)))
  }

  "RegionPlan.topologicalIterator" should
    "respect a wide DAG with multiple parallel branches and joins" in {
    // Construct a 9-region DAG:
    //
    //     0 ──┬──► 1 ──┬──► 4 ──┐
    //         │        │        │
    //         │        ├──► 5 ──┤
    //         │        │        ├──► 7 ──► 8
    //         ├──► 2 ──┤        │
    //         │        ├──► 6 ──┘
    //         └──► 3 ──┘
    //
    // 0 is the only source, 8 is the only sink. Multiple intermediate
    // joins/forks make the test more meaningful than a linked list.
    val ids = (0L to 8L).map(RegionIdentity)
    val regs = ids.map(rid => Region(rid, Set(op(s"r${rid.id}-x")), Set.empty)).toSet
    val edges = Set(
      RegionLink(ids(0), ids(1)),
      RegionLink(ids(0), ids(2)),
      RegionLink(ids(0), ids(3)),
      RegionLink(ids(1), ids(4)),
      RegionLink(ids(1), ids(5)),
      RegionLink(ids(2), ids(5)),
      RegionLink(ids(2), ids(6)),
      RegionLink(ids(3), ids(6)),
      RegionLink(ids(4), ids(7)),
      RegionLink(ids(5), ids(7)),
      RegionLink(ids(6), ids(7)),
      RegionLink(ids(7), ids(8))
    )
    val plan = RegionPlan(regs, edges)
    val order = plan.topologicalIterator().toList
    val pos = order.zipWithIndex.toMap
    edges.foreach { e =>
      assert(
        pos(e.fromRegionId) < pos(e.toRegionId),
        s"topological order violates edge $e: " +
          s"${e.fromRegionId}@${pos(e.fromRegionId)} should come before " +
          s"${e.toRegionId}@${pos(e.toRegionId)}"
      )
    }
    assert(order.head == ids(0))
    assert(order.last == ids(8))
  }
}
