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
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowCoreTypesSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // LocationPreference
  // ---------------------------------------------------------------------------

  "LocationPreference" should "have PreferCoordinator and RoundRobinPreference as singleton subtypes" in {
    val a: LocationPreference = PreferCoordinator
    val b: LocationPreference = RoundRobinPreference
    assert(a eq PreferCoordinator)
    assert(b eq RoundRobinPreference)
    assert(a != b)
  }

  it should "be Serializable on every subtype" in {
    val all: Seq[LocationPreference] = Seq(PreferCoordinator, RoundRobinPreference)
    all.foreach(p => assert(p.isInstanceOf[Serializable]))
  }

  // ---------------------------------------------------------------------------
  // WorkflowSettings
  // ---------------------------------------------------------------------------

  "WorkflowSettings" should "default dataTransferBatchSize to 400 and outputPortsNeedingStorage to empty" in {
    val s = WorkflowSettings()
    assert(s.dataTransferBatchSize == 400)
    assert(s.outputPortsNeedingStorage.isEmpty)
  }

  it should "carry custom values constructed via case-class apply" in {
    val portId = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("op"), "main"),
      PortIdentity(0),
      input = false
    )
    val s = WorkflowSettings(
      dataTransferBatchSize = 50,
      outputPortsNeedingStorage = Set(portId)
    )
    assert(s.dataTransferBatchSize == 50)
    assert(s.outputPortsNeedingStorage == Set(portId))
  }

  // ---------------------------------------------------------------------------
  // WorkflowContext
  // ---------------------------------------------------------------------------

  "WorkflowContext" should "default to DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID, and DEFAULT_WORKFLOW_SETTINGS" in {
    val ctx = new WorkflowContext()
    assert(ctx.workflowId == WorkflowContext.DEFAULT_WORKFLOW_ID)
    assert(ctx.executionId == WorkflowContext.DEFAULT_EXECUTION_ID)
    assert(ctx.workflowSettings == WorkflowContext.DEFAULT_WORKFLOW_SETTINGS)
  }

  it should "expose the documented default constants" in {
    assert(WorkflowContext.DEFAULT_WORKFLOW_ID == WorkflowIdentity(1L))
    assert(WorkflowContext.DEFAULT_EXECUTION_ID == ExecutionIdentity(1L))
  }

  it should "allow workflowId / executionId / workflowSettings to be reassigned" in {
    val ctx = new WorkflowContext()
    ctx.workflowId = WorkflowIdentity(7L)
    ctx.executionId = ExecutionIdentity(11L)
    val custom = WorkflowSettings(dataTransferBatchSize = 1)
    ctx.workflowSettings = custom
    assert(ctx.workflowId == WorkflowIdentity(7L))
    assert(ctx.executionId == ExecutionIdentity(11L))
    assert(ctx.workflowSettings eq custom)
  }

  // ---------------------------------------------------------------------------
  // PhysicalOp helpers
  // ---------------------------------------------------------------------------

  private val workflowId = WorkflowIdentity(0L)
  private val executionId = ExecutionIdentity(0L)
  private def opId(name: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(name), "main")
  private val intSchema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def newPhysicalOp(name: String, parallelizable: Boolean = true): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        opId(name),
        workflowId,
        executionId,
        OpExecInitInfo.Empty
      )
      .copy(parallelizable = parallelizable)

  "PhysicalOp.isSourceOperator" should "be true when there are no input ports" in {
    val op = newPhysicalOp("a")
    assert(op.inputPorts.isEmpty)
    assert(op.isSourceOperator)
  }

  it should "be false once an input port is added" in {
    val op = newPhysicalOp("a").withInputPorts(List(InputPort(PortIdentity(0))))
    assert(!op.isSourceOperator)
  }

  "PhysicalOp.withLocationPreference" should "store the location preference" in {
    val op = newPhysicalOp("a").withLocationPreference(Some(PreferCoordinator))
    assert(op.locationPreference.contains(PreferCoordinator))
  }

  "PhysicalOp.withParallelizable" should "set the parallelizable flag and round-trip through copy" in {
    val op = newPhysicalOp("a", parallelizable = true)
    val flipped = op.withParallelizable(false)
    assert(!flipped.parallelizable)
    assert(op.parallelizable, "the original instance is immutable")
  }

  "PhysicalOp.withRequiresMaterializedExecution" should "default to false and round-trip through copy" in {
    val op = newPhysicalOp("a")
    assert(!op.requiresMaterializedExecution, "defaults to false")
    val flipped = op.withRequiresMaterializedExecution(true)
    assert(flipped.requiresMaterializedExecution)
    assert(!op.requiresMaterializedExecution, "the original instance is immutable")
  }

  "PhysicalOp.withSuggestedWorkerNum" should "set the suggested worker count" in {
    val op = newPhysicalOp("a").withSuggestedWorkerNum(7)
    assert(op.suggestedWorkerNum.contains(7))
  }

  "PhysicalOp.addInputLink" should "append the link to the matching input port" in {
    val op = newPhysicalOp("a").withInputPorts(List(InputPort(PortIdentity(0))))
    val link = PhysicalLink(opId("up"), PortIdentity(0), opId("a"), PortIdentity(0))
    val updated = op.addInputLink(link)
    assert(updated.getInputLinks(Some(PortIdentity(0))) == List(link))
    assert(updated.getInputLinks() == List(link))
  }

  it should "fail the assertion when the link does not target this op id" in {
    val op = newPhysicalOp("a").withInputPorts(List(InputPort(PortIdentity(0))))
    val mismatched = PhysicalLink(opId("up"), PortIdentity(0), opId("other"), PortIdentity(0))
    assertThrows[AssertionError] {
      op.addInputLink(mismatched)
    }
  }

  it should "fail the assertion when the target port is not declared" in {
    val op = newPhysicalOp("a").withInputPorts(List(InputPort(PortIdentity(0))))
    val unknownPort = PhysicalLink(opId("up"), PortIdentity(0), opId("a"), PortIdentity(99))
    assertThrows[AssertionError] {
      op.addInputLink(unknownPort)
    }
  }

  "PhysicalOp.addOutputLink" should "append the link to the matching output port" in {
    val op = newPhysicalOp("a").withOutputPorts(List(OutputPort(PortIdentity(0))))
    val link = PhysicalLink(opId("a"), PortIdentity(0), opId("dn"), PortIdentity(0))
    val updated = op.addOutputLink(link)
    assert(updated.getOutputLinks(PortIdentity(0)) == List(link))
  }

  "PhysicalOp.removeInputLink" should "drop the matching link, leaving others intact" in {
    val op = newPhysicalOp("a").withInputPorts(List(InputPort(PortIdentity(0))))
    val l1 = PhysicalLink(opId("u1"), PortIdentity(0), opId("a"), PortIdentity(0))
    val l2 = PhysicalLink(opId("u2"), PortIdentity(0), opId("a"), PortIdentity(0))
    val updated = op.addInputLink(l1).addInputLink(l2).removeInputLink(l1)
    assert(updated.getInputLinks() == List(l2))
  }

  "PhysicalOp.removeOutputLink" should "drop the matching link, leaving others intact" in {
    val op = newPhysicalOp("a").withOutputPorts(List(OutputPort(PortIdentity(0))))
    val l1 = PhysicalLink(opId("a"), PortIdentity(0), opId("d1"), PortIdentity(0))
    val l2 = PhysicalLink(opId("a"), PortIdentity(0), opId("d2"), PortIdentity(0))
    val updated = op.addOutputLink(l1).addOutputLink(l2).removeOutputLink(l1)
    assert(updated.getOutputLinks(PortIdentity(0)) == List(l2))
  }

  "PhysicalOp.propagateSchema" should "fill in output schemas once every input schema is known" in {
    val out = OutputPort(PortIdentity(0))
    val in = InputPort(PortIdentity(0))
    val op = newPhysicalOp("a")
      .withInputPorts(List(in))
      .withOutputPorts(List(out))
      .withPropagateSchema(SchemaPropagationFunc(inputs => Map(out.id -> inputs(in.id))))
    val updated = op.propagateSchema(Some((in.id, intSchema)))
    val outSchema = updated.outputPorts(out.id)._3
    assert(outSchema.toOption.contains(intSchema))
  }

  it should "raise IllegalArgumentException when a conflicting schema arrives on an already-known port" in {
    val out = OutputPort(PortIdentity(0))
    val in = InputPort(PortIdentity(0))
    val op = newPhysicalOp("a")
      .withInputPorts(List(in))
      .withOutputPorts(List(out))
      .withPropagateSchema(SchemaPropagationFunc(inputs => Map(out.id -> inputs(in.id))))
      .propagateSchema(Some((in.id, intSchema)))
    val different = Schema().add(new Attribute("w", AttributeType.STRING))
    assertThrows[IllegalArgumentException] {
      op.propagateSchema(Some((in.id, different)))
    }
  }

  it should "leave output schemas as a Left when the propagation function throws" in {
    val out = OutputPort(PortIdentity(0))
    val in = InputPort(PortIdentity(0))
    val op = newPhysicalOp("a")
      .withInputPorts(List(in))
      .withOutputPorts(List(out))
      .withPropagateSchema(SchemaPropagationFunc(_ => throw new RuntimeException("boom")))
    val updated = op.propagateSchema(Some((in.id, intSchema)))
    assert(updated.outputPorts(out.id)._3.isLeft)
  }

  "PhysicalOp.isOutputLinkBlocking" should "reflect the configured blocking flag on the source port" in {
    val opBlocking =
      newPhysicalOp("a").withOutputPorts(List(OutputPort(PortIdentity(0), blocking = true)))
    val opOpen =
      newPhysicalOp("b").withOutputPorts(List(OutputPort(PortIdentity(0), blocking = false)))
    // Each link's `fromOpId` is set to the operator under test, so the test
    // remains correct if `isOutputLinkBlocking` is later tightened to
    // validate `fromOpId == this.id`.
    val blockingLink =
      PhysicalLink(opId("a"), PortIdentity(0), opId("downstream"), PortIdentity(0))
    val openLink =
      PhysicalLink(opId("b"), PortIdentity(0), opId("downstream"), PortIdentity(0))
    assert(opBlocking.isOutputLinkBlocking(blockingLink))
    assert(!opOpen.isOutputLinkBlocking(openLink))
  }

  // ---------------------------------------------------------------------------
  // PhysicalPlan
  // ---------------------------------------------------------------------------

  private def physicalOp(name: String): PhysicalOp =
    newPhysicalOp(name)
      .withInputPorts(List(InputPort(PortIdentity(0))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))

  private def link(from: String, to: String): PhysicalLink =
    PhysicalLink(opId(from), PortIdentity(0), opId(to), PortIdentity(0))

  "PhysicalPlan.getOperator" should "look up by physical id" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val plan = PhysicalPlan(Set(a, b), Set.empty)
    assert(plan.getOperator(a.id) == a)
  }

  "PhysicalPlan.getSourceOperatorIds" should "return operators with no incoming links in the DAG" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val c = physicalOp("c")
    val plan = PhysicalPlan(Set(a, b, c), Set(link("a", "b"), link("b", "c")))
    assert(plan.getSourceOperatorIds == Set(a.id))
  }

  it should "return all operators when there are no links" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val plan = PhysicalPlan(Set(a, b), Set.empty)
    assert(plan.getSourceOperatorIds == Set(a.id, b.id))
  }

  "PhysicalPlan.topologicalIterator" should "produce a topological ordering across the DAG" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val c = physicalOp("c")
    val plan = PhysicalPlan(Set(a, b, c), Set(link("a", "b"), link("b", "c")))
    assert(plan.topologicalIterator().toList == List(a.id, b.id, c.id))
  }

  "PhysicalPlan.getUpstreamPhysicalOpIds" should "return direct predecessors only" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val c = physicalOp("c")
    val plan = PhysicalPlan(Set(a, b, c), Set(link("a", "b"), link("a", "c"), link("b", "c")))
    assert(plan.getUpstreamPhysicalOpIds(c.id) == Set(a.id, b.id))
    assert(plan.getUpstreamPhysicalOpIds(a.id).isEmpty)
  }

  "PhysicalPlan.getUpstreamPhysicalLinks" should "return only links targeting the operator" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val c = physicalOp("c")
    val l1 = link("a", "c")
    val l2 = link("b", "c")
    val plan = PhysicalPlan(Set(a, b, c), Set(l1, l2))
    assert(plan.getUpstreamPhysicalLinks(c.id) == Set(l1, l2))
    assert(plan.getUpstreamPhysicalLinks(a.id).isEmpty)
  }

  "PhysicalPlan.getDownstreamPhysicalLinks" should "return only links sourcing from the operator" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val c = physicalOp("c")
    val l1 = link("a", "b")
    val l2 = link("a", "c")
    val plan = PhysicalPlan(Set(a, b, c), Set(l1, l2))
    assert(plan.getDownstreamPhysicalLinks(a.id) == Set(l1, l2))
    assert(plan.getDownstreamPhysicalLinks(c.id).isEmpty)
  }

  "PhysicalPlan.getSubPlan" should "include only the requested operators and the links between them" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val c = physicalOp("c")
    val plan = PhysicalPlan(Set(a, b, c), Set(link("a", "b"), link("b", "c"), link("a", "c")))
    val sub = plan.getSubPlan(Set(a.id, b.id))
    assert(sub.operators.map(_.id) == Set(a.id, b.id))
    assert(sub.links == Set(link("a", "b")))
  }

  "PhysicalPlan.getTransitiveUpstreamSubPlan" should "include the operator and all its transitive upstream, with the links between them" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val c = physicalOp("c")
    val d = physicalOp("d")
    // a -> b -> c -> d and a -> c; the upstream sub-DAG of c is {a, b, c} (d is downstream)
    val plan =
      PhysicalPlan(
        Set(a, b, c, d),
        Set(link("a", "b"), link("b", "c"), link("c", "d"), link("a", "c"))
      )
    val sub = plan.getTransitiveUpstreamSubPlan(c.id)
    assert(sub.operators.map(_.id) == Set(a.id, b.id, c.id))
    assert(sub.links == Set(link("a", "b"), link("b", "c"), link("a", "c")))
  }

  it should "return only the operator itself when it has no upstream" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val plan = PhysicalPlan(Set(a, b), Set(link("a", "b")))
    val sub = plan.getTransitiveUpstreamSubPlan(a.id)
    assert(sub.operators.map(_.id) == Set(a.id))
    assert(sub.links.isEmpty)
  }

  it should "include all branches when an operator has multiple inputs (join or union)" in {
    val s1 = physicalOp("s1")
    val s2 = physicalOp("s2")
    val j = newPhysicalOp("j")
      .withInputPorts(List(InputPort(PortIdentity(0)), InputPort(PortIdentity(1))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
    val l1 = PhysicalLink(s1.id, PortIdentity(0), j.id, PortIdentity(0))
    val l2 = PhysicalLink(s2.id, PortIdentity(0), j.id, PortIdentity(1))
    val plan = PhysicalPlan(Set(s1, s2, j), Set(l1, l2))
    val sub = plan.getTransitiveUpstreamSubPlan(j.id)
    assert(sub.operators.map(_.id) == Set(s1.id, s2.id, j.id))
    assert(sub.links == Set(l1, l2))
  }

  it should "follow only the target's upstream with multiple sources and sinks" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val c = newPhysicalOp("c")
      .withInputPorts(List(InputPort(PortIdentity(0)), InputPort(PortIdentity(1))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
    val d = physicalOp("d")
    val e = physicalOp("e")
    // sources a, b converge at c; c fans out to sinks d and e
    val plan = PhysicalPlan(
      Set(a, b, c, d, e),
      Set(
        PhysicalLink(a.id, PortIdentity(0), c.id, PortIdentity(0)),
        PhysicalLink(b.id, PortIdentity(0), c.id, PortIdentity(1)),
        link("c", "d"),
        link("c", "e")
      )
    )
    // the sub-DAG of sink d is {a, b, c, d}; the other sink e is excluded
    val sub = plan.getTransitiveUpstreamSubPlan(d.id)
    assert(sub.operators.map(_.id) == Set(a.id, b.id, c.id, d.id))
    assert(!sub.operators.map(_.id).contains(e.id))
  }

  "PhysicalPlan.getPhysicalOpsOfLogicalOp" should "return every physical op sharing a logical id, in topological order" in {
    val a = physicalOp("a")
    val b = physicalOp("b")
    val plan = PhysicalPlan(Set(a, b), Set(link("a", "b")))
    val onlyB = plan.getPhysicalOpsOfLogicalOp(OperatorIdentity("b"))
    assert(onlyB.map(_.id) == List(b.id))
  }
}
