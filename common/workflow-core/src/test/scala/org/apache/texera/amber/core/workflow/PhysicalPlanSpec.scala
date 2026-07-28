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

package org.apache.texera.amber.core.workflow

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec

class PhysicalPlanSpec extends AnyFlatSpec {

  private val workflowId = WorkflowIdentity(0L)
  private val executionId = ExecutionIdentity(0L)
  private def opId(name: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(name), "main")
  private val intSchema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))
  private def newPhysicalOp(name: String): PhysicalOp =
    PhysicalOp.oneToOnePhysicalOp(opId(name), workflowId, executionId, OpExecInitInfo.Empty)
  private def physicalOp(name: String): PhysicalOp =
    newPhysicalOp(name)
      .withInputPorts(List(InputPort(PortIdentity(0))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
  private def link(from: String, to: String): PhysicalLink =
    PhysicalLink(opId(from), PortIdentity(0), opId(to), PortIdentity(0))

  // ----- addOperator / addLink -----

  "PhysicalPlan.addLink" should "register the link on both endpoint operators" in {
    val emptyPlan = PhysicalPlan(Set.empty, Set.empty)
    val plan =
      emptyPlan.addOperator(physicalOp("a")).addOperator(physicalOp("b")).addLink(link("a", "b"))
    assert(plan.links == Set(link("a", "b")))
    assert(plan.getOperator(opId("a")).getOutputLinks(PortIdentity(0)) == List(link("a", "b")))
    assert(plan.getOperator(opId("b")).getInputLinks(Some(PortIdentity(0))) == List(link("a", "b")))
    assert(emptyPlan.operators.isEmpty && emptyPlan.links.isEmpty) // immutability
  }

  it should "propagate a known upstream output schema to the downstream input port" in {
    val src = newPhysicalOp("src")
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(PortIdentity(0) -> intSchema)))
      .propagateSchema()
    val downstream = physicalOp("b")
    val plan = PhysicalPlan(Set(src, downstream), Set.empty)
      .addLink(PhysicalLink(src.id, PortIdentity(0), downstream.id, PortIdentity(0)))
    assert(plan.getOperator(downstream.id).inputPorts(PortIdentity(0))._3 == Right(intSchema))
  }

  it should "reject a link from an undeclared output port" in {
    val plan = PhysicalPlan(Set(physicalOp("a"), physicalOp("b")), Set.empty)
    assertThrows[NoSuchElementException] {
      plan.addLink(PhysicalLink(opId("a"), PortIdentity(99), opId("b"), PortIdentity(0)))
    }
  }

  // ----- removeLink / setOperator -----

  "PhysicalPlan.removeLink" should "unregister the link from both endpoint operators" in {
    val plan =
      PhysicalPlan(Set(physicalOp("a"), physicalOp("b")), Set.empty).addLink(link("a", "b"))
    val removed = plan.removeLink(link("a", "b"))
    assert(removed.links.isEmpty)
    assert(removed.getOperator(opId("a")).getOutputLinks(PortIdentity(0)).isEmpty)
    assert(removed.getOperator(opId("b")).getInputLinks().isEmpty)
  }

  "PhysicalPlan.setOperator" should "replace an existing operator and insert a new one" in {
    val plan = PhysicalPlan(Set(physicalOp("a")), Set.empty)
    val replacement = physicalOp("a").withParallelizable(false)
    val updated = plan.setOperator(replacement)
    assert(!updated.getOperator(opId("a")).parallelizable)
    assert(updated.operators.size == 1)
    assert(plan.setOperator(physicalOp("b")).operators.size == 2)
  }

  // ----- lookups -----

  "PhysicalPlan.getPhysicalOpByWorkerId" should "resolve the operator from a worker identity" in {
    val op = physicalOp("a")
    val plan = PhysicalPlan(Set(op), Set.empty)
    val workerId = VirtualIdentityUtils.createWorkerIdentity(WorkflowIdentity(0L), "a", "main", 0)
    assert(plan.getPhysicalOpByWorkerId(workerId) == op)
  }

  "PhysicalPlan.getLinksBetween" should "return only links in the requested direction" in {
    val plan = PhysicalPlan(
      Set(physicalOp("a"), physicalOp("b"), physicalOp("c")),
      Set(link("a", "b"), link("a", "c"))
    )
    assert(plan.getLinksBetween(opId("a"), opId("b")) == Set(link("a", "b")))
    assert(plan.getLinksBetween(opId("b"), opId("a")).isEmpty)
  }

  // ----- getOutputPartitionInfo -----

  "PhysicalPlan.getOutputPartitionInfo" should
    "keep the upstream partition when it satisfies the requirement and worker counts match" in {
    val plan =
      PhysicalPlan(Set(physicalOp("a"), physicalOp("b")), Set.empty).addLink(link("a", "b"))
    val result = plan.getOutputPartitionInfo(link("a", "b"), HashPartition(List("k")), Map.empty)
    assert(result == HashPartition(List("k")))
  }

  it should "fall back to the required partition when the upstream does not satisfy it" in {
    val demanding = physicalOp("b").withPartitionRequirement(List(Option(SinglePartition())))
    val plan = PhysicalPlan(Set(physicalOp("a"), demanding), Set.empty).addLink(link("a", "b"))
    val result = plan.getOutputPartitionInfo(link("a", "b"), HashPartition(List("k")), Map.empty)
    assert(result == SinglePartition())
  }

  it should "fall back to the required partition when worker counts differ" in {
    val plan =
      PhysicalPlan(Set(physicalOp("a"), physicalOp("b")), Set.empty).addLink(link("a", "b"))
    val result = plan.getOutputPartitionInfo(
      link("a", "b"),
      HashPartition(List("k")),
      Map(opId("a") -> 2, opId("b") -> 1)
    )
    assert(result == UnknownPartition())
  }

  // ----- blocking and dependee links -----

  "PhysicalPlan.getBlockingAndDependeeLinks" should "detect links out of blocking ports" in {
    val blockingSource =
      newPhysicalOp("a").withOutputPorts(List(OutputPort(PortIdentity(0), blocking = true)))
    val plan =
      PhysicalPlan(Set(blockingSource, physicalOp("b")), Set.empty).addLink(link("a", "b"))
    assert(plan.getBlockingAndDependeeLinks == Set(link("a", "b")))
    val plainPlan =
      PhysicalPlan(Set(physicalOp("a"), physicalOp("b")), Set.empty).addLink(link("a", "b"))
    assert(plainPlan.getBlockingAndDependeeLinks.isEmpty)
  }

  private def dependeePlan(): (PhysicalPlan, PhysicalLink, PhysicalLink) = {
    val join = newPhysicalOp("j")
      .withInputPorts(
        List(
          InputPort(PortIdentity(0)),
          InputPort(PortIdentity(1), dependencies = Seq(PortIdentity(0)))
        )
      )
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
    val s1 = physicalOp("s1")
    val s2 = physicalOp("s2")
    val dependeeLink = PhysicalLink(s1.id, PortIdentity(0), join.id, PortIdentity(0))
    val dependerLink = PhysicalLink(s2.id, PortIdentity(0), join.id, PortIdentity(1))
    val plan =
      PhysicalPlan(Set(s1, s2, join), Set.empty).addLink(dependeeLink).addLink(dependerLink)
    (plan, dependeeLink, dependerLink)
  }

  "PhysicalPlan.getDependeeLinks" should "detect links into dependee input ports" in {
    val (plan, dependeeLink, _) = dependeePlan()
    assert(plan.getDependeeLinks == Set(dependeeLink))
    assert(plan.getBlockingAndDependeeLinks == Set(dependeeLink))
  }

  "PhysicalPlan.getDependeeLinksRemovedDAG" should "drop only the dependee links" in {
    val (plan, _, dependerLink) = dependeePlan()
    val trimmed = plan.getDependeeLinksRemovedDAG
    assert(trimmed.links == Set(dependerLink))
    assert(trimmed.operators == plan.operators)
  }

  // ----- getNonBridgeNonBlockingLinks -----

  "PhysicalPlan.getNonBridgeNonBlockingLinks" should "keep all links of a diamond (no bridges)" in {
    val diamondLinks =
      Set(link("a", "b"), link("a", "c"), link("b", "d"), link("c", "d"))
    val plan = PhysicalPlan(
      Set(physicalOp("a"), physicalOp("b"), physicalOp("c"), physicalOp("d")),
      diamondLinks
    )
    assert(plan.getNonBridgeNonBlockingLinks == diamondLinks)
  }

  it should "drop bridge links of a straight chain" in {
    val plan = PhysicalPlan(Set(physicalOp("a"), physicalOp("b")), Set(link("a", "b")))
    assert(plan.getNonBridgeNonBlockingLinks.isEmpty)
  }

  // ----- maxChains -----

  "PhysicalPlan.maxChains" should "keep only the maximal chain of a straight pipeline" in {
    val plan = PhysicalPlan(
      Set(physicalOp("a"), physicalOp("b"), physicalOp("c"), physicalOp("d")),
      Set(link("a", "b"), link("b", "c"), link("c", "d"))
    )
    assert(plan.maxChains == Set(Set(link("a", "b"), link("b", "c"), link("c", "d"))))
  }

  it should "be empty when no path is longer than one link" in {
    val plan = PhysicalPlan(Set(physicalOp("a"), physicalOp("b")), Set(link("a", "b")))
    assert(plan.maxChains.isEmpty)
  }

  // ----- layeredReversedTopologicalOrder -----

  "PhysicalPlan.layeredReversedTopologicalOrder" should "layer a diamond sink-to-source" in {
    val plan = PhysicalPlan(
      Set(physicalOp("a"), physicalOp("b"), physicalOp("c"), physicalOp("d")),
      Set(link("a", "b"), link("a", "c"), link("b", "d"), link("c", "d"))
    )
    assert(
      plan.layeredReversedTopologicalOrder ==
        Seq(Set(opId("d")), Set(opId("b"), opId("c")), Set(opId("a")))
    )
  }

  it should "not promote an operator until all its parallel edges are consumed" in {
    val upstream = newPhysicalOp("a")
      .withOutputPorts(List(OutputPort(PortIdentity(0)), OutputPort(PortIdentity(1))))
    val downstream = newPhysicalOp("b")
      .withInputPorts(List(InputPort(PortIdentity(0)), InputPort(PortIdentity(1))))
    val plan = PhysicalPlan(
      Set(upstream, downstream),
      Set(
        PhysicalLink(upstream.id, PortIdentity(0), downstream.id, PortIdentity(0)),
        PhysicalLink(upstream.id, PortIdentity(1), downstream.id, PortIdentity(1))
      )
    )
    assert(plan.layeredReversedTopologicalOrder == Seq(Set(downstream.id), Set(upstream.id)))
  }

  it should "be empty for an empty plan" in {
    assert(PhysicalPlan(Set.empty, Set.empty).layeredReversedTopologicalOrder == Seq.empty)
  }

  // ----- plan-level propagateSchema -----

  "PhysicalPlan.propagateSchema" should "flow schemas through the plan's links" in {
    val src = newPhysicalOp("src")
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(PortIdentity(0) -> intSchema)))
    val mid = physicalOp("mid").withPropagateSchema(
      SchemaPropagationFunc(inputs => Map(PortIdentity(0) -> inputs(PortIdentity(0))))
    )
    val planLink = PhysicalLink(src.id, PortIdentity(0), mid.id, PortIdentity(0))
    val result = PhysicalPlan(Set(src, mid), Set(planLink)).propagateSchema(Map.empty)
    assert(result.operators.map(_.id) == Set(src.id, mid.id))
    assert(result.links == Set(planLink))
    assert(result.getOperator(src.id).outputPorts(PortIdentity(0))._3.toOption.contains(intSchema))
    assert(result.getOperator(mid.id).outputPorts(PortIdentity(0))._3.toOption.contains(intSchema))
  }

  it should "seed input ports from the provided schema map" in {
    val op = physicalOp("mid").withPropagateSchema(
      SchemaPropagationFunc(inputs => Map(PortIdentity(0) -> inputs(PortIdentity(0))))
    )
    val result =
      PhysicalPlan(Set(op), Set.empty).propagateSchema(Map(PortIdentity(0) -> intSchema))
    assert(result.getOperator(op.id).outputPorts(PortIdentity(0))._3.toOption.contains(intSchema))
  }

  // ----- structural equality -----

  "PhysicalPlan" should "compare structurally" in {
    assert(PhysicalPlan(Set.empty, Set.empty) == PhysicalPlan(Set.empty, Set.empty))
  }
}
