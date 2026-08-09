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

package org.apache.texera.common.compiler

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.trycatch.{CatchGateConfig, FinallyOpDesc, TryCatchOpDesc}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TryCatchFramePassSpec extends AnyFlatSpec with Matchers {

  private val wid = WorkflowIdentity(1L)
  private val eid = ExecutionIdentity(1L)
  private val schema: Schema = Schema().add("field1", AttributeType.INTEGER)

  /** an ordinary single-input single-output pass-through physical op */
  private def simpleOp(name: String): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        PhysicalOpIdentity(OperatorIdentity(name), "main"),
        wid,
        eid,
        OpExecWithClassName(s"test.$name", "")
      )
      .withInputPorts(List(InputPort(PortIdentity())))
      .withOutputPorts(List(OutputPort(PortIdentity())))
      .withPropagateSchema(
        SchemaPropagationFunc(in => Map(PortIdentity() -> in(PortIdentity())))
      )

  private def sourceOp(name: String): PhysicalOp =
    PhysicalOp
      .sourcePhysicalOp(
        PhysicalOpIdentity(OperatorIdentity(name), "main"),
        wid,
        eid,
        OpExecWithClassName(s"test.$name", "")
      )
      .withInputPorts(List.empty)
      .withOutputPorts(List(OutputPort(PortIdentity())))
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(PortIdentity() -> schema)))

  /**
    * Consumes anything, emits the standard test schema — stands in for a
    * user op that turns Error Info rows back into branch-shaped data (a
    * classifier/formatter), so tests can wire the gate's Error Info port
    * into data subgraphs without tripping schema propagation.
    */
  private def fixedSchemaOp(name: String): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        PhysicalOpIdentity(OperatorIdentity(name), "main"),
        wid,
        eid,
        OpExecWithClassName(s"test.$name", "")
      )
      .withInputPorts(List(InputPort(PortIdentity())))
      .withOutputPorts(List(OutputPort(PortIdentity())))
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(PortIdentity() -> schema)))

  private def opId(name: String, layer: String = "main"): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(name), layer)

  private def gateConfig(gate: PhysicalOp): CatchGateConfig = {
    val descString = gate.opExecInitInfo match {
      case OpExecWithClassName(_, desc) => desc
    }
    objectMapper.readValue(descString, classOf[CatchGateConfig])
  }

  private def link(
      from: PhysicalOpIdentity,
      fromPort: PortIdentity,
      to: PhysicalOpIdentity,
      toPort: PortIdentity
  ): PhysicalLink = PhysicalLink(from, fromPort, to, toPort)

  private val out0 = PortIdentity()
  private val in0 = PortIdentity()
  private val catchPort = PortIdentity(1)

  /**
    * source -> TryCatch(Data); Try -> tryOp -> Finally.FromTry;
    * Catch -> catchOp -> Finally.FromCatch; Finally -> downstream
    */
  private def buildFramePlan(): PhysicalPlan = {
    val tcDesc = new TryCatchOpDesc()
    tcDesc.setOperatorId("tc")
    val fDesc = new FinallyOpDesc()
    fDesc.setOperatorId("fin")

    val tcPlan = tcDesc.getPhysicalPlan(wid, eid)
    val merger = fDesc.getPhysicalOp(wid, eid)

    val src = sourceOp("src")
    val tryOp = simpleOp("tryOp")
    val catchOp = simpleOp("catchOp")
    val down = simpleOp("down")

    val splitterId = opId("tc", TryCatchOpDesc.SPLITTER_LAYER)
    val gateId = opId("tc", TryCatchOpDesc.GATE_LAYER)

    var plan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    (Set(src, tryOp, catchOp, down, merger) ++ tcPlan.operators).foreach { op =>
      plan = plan.addOperator(op.propagateSchema())
    }
    List(
      link(opId("src"), out0, splitterId, in0),
      tcPlan.links.head, // splitter -> gate snapshot
      link(splitterId, out0, opId("tryOp"), in0),
      link(gateId, catchPort, opId("catchOp"), in0),
      link(opId("tryOp"), out0, merger.id, PortIdentity()),
      link(opId("catchOp"), out0, merger.id, PortIdentity(1)),
      link(merger.id, out0, opId("down"), in0)
    ).foreach(l => plan = plan.addLink(l))
    plan
  }

  "TryCatchFramePass" should "leave plans without frames untouched" in {
    var plan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    plan = plan.addOperator(sourceOp("src").propagateSchema())
    plan = plan.addOperator(simpleOp("down").propagateSchema())
    plan = plan.addLink(link(opId("src"), out0, opId("down"), in0))
    val result = TryCatchFramePass.run(plan, None)
    result should be theSameInstanceAs plan
  }

  it should "synthesize signal edges from try-cone tails into the gate with dependee snapshot" in {
    val result = TryCatchFramePass.run(buildFramePlan(), None)
    val gate = result.getOperator(opId("tc", TryCatchOpDesc.GATE_LAYER))

    // one signal port (tryOp's output feeds the merger => it is a tail)
    val signalPorts = gate.inputPorts.keys.filter(p => p.internal && p.id > 0)
    signalPorts should have size 1
    val signalPort = signalPorts.head

    // snapshot depends on all signal ports
    val snapshotInputPort = gate.inputPorts(TryCatchOpDesc.SNAPSHOT_IN)._1
    snapshotInputPort.dependencies should contain theSameElementsAs signalPorts

    // the signal link originates at the tail's output port
    val signalLinks = result.links.filter(l => l.toOpId == gate.id && l.toPortId == signalPort)
    signalLinks.map(_.fromOpId) shouldBe Set(opId("tryOp"))

    // signal ports carry the SignalPartition requirement (indexed by port id)
    gate.partitionRequirement.lift(signalPort.id).flatten shouldBe Some(SignalPartition())

    // cone attribution baked into the gate's executor config
    val descString = gate.opExecInitInfo match {
      case OpExecWithClassName(_, desc) => desc
    }
    val config = objectMapper.readValue(descString, classOf[CatchGateConfig])
    config.ownConeOpIds should contain("tryOp/main")
    config.ownConeOpIds should contain("catchOp/main")
    config.ownConeOpIds should not contain "src/main"
    config.ownConeOpIds should not contain "down/main"

    // frame semantics only inside the frame: cones + apparatus get the
    // guarded (fail-by-draining) flag; everything outside keeps the
    // default report-and-pause behavior
    result.getOperator(opId("tryOp")).isGuarded shouldBe true
    result.getOperator(opId("catchOp")).isGuarded shouldBe true
    result.getOperator(opId("tc", TryCatchOpDesc.SPLITTER_LAYER)).isGuarded shouldBe true
    gate.isGuarded shouldBe true
    result.getOperator(opId("fin")).isGuarded shouldBe true
    result.getOperator(opId("src")).isGuarded shouldBe false
    result.getOperator(opId("down")).isGuarded shouldBe false
  }

  it should "collect exactly one signal port when the try cone has a single tail wired to Finally" in {
    // The common shape: one linear try branch ending at Finally. Only the
    // user's chosen tail becomes a signal source — intermediate ports (whose
    // output flows on to another cone operator) must not.
    var plan = buildFramePlan()
    // extend the try branch: tryOp -> mid -> Finally (tryOp becomes intermediate)
    val mid = simpleOp("mid")
    plan = plan.addOperator(mid.propagateSchema())
    plan = plan.removeLink(
      link(opId("tryOp"), out0, opId("fin"), PortIdentity())
    )
    plan = plan.addLink(link(opId("tryOp"), out0, opId("mid"), in0))
    plan = plan.addLink(link(opId("mid"), out0, opId("fin"), PortIdentity()))

    val result = TryCatchFramePass.run(plan, None)
    val gate = result.getOperator(opId("tc", TryCatchOpDesc.GATE_LAYER))
    val signalPorts = gate.inputPorts.keys.filter(p => p.internal && p.id > 0)
    signalPorts should have size 1
    val signalLinks = result.links.filter(l => l.toOpId == gate.id && l.toPortId.id > 0)
    signalLinks.map(_.fromOpId) shouldBe Set(opId("mid"))
  }

  it should "collect every ending of a forked try cone, not just the one wired to Finally" in {
    // A fork where one tail goes to Finally and the other ends in its own
    // result table: BOTH must signal the gate, or a failure in the second fork
    // would be invisible and the frame could declare success while it is still
    // running.
    var plan = buildFramePlan()
    val sideTail = simpleOp("sideTail")
    plan = plan.addOperator(sideTail.propagateSchema())
    // the splitter's Try port also feeds a second branch that ends nowhere
    plan = plan.addLink(
      link(opId("tc", TryCatchOpDesc.SPLITTER_LAYER), out0, opId("sideTail"), in0)
    )

    val result = TryCatchFramePass.run(plan, None)
    val gate = result.getOperator(opId("tc", TryCatchOpDesc.GATE_LAYER))
    val signalPorts = gate.inputPorts.keys.filter(p => p.internal && p.id > 0)
    signalPorts should have size 2
    val signalLinks = result.links.filter(l => l.toOpId == gate.id && l.toPortId.id > 0)
    signalLinks.map(_.fromOpId) shouldBe Set(opId("tryOp"), opId("sideTail"))
    // every signal port carries the tuple-dropping requirement
    signalPorts.foreach(p =>
      gate.partitionRequirement.lift(p.id).flatten shouldBe Some(SignalPartition())
    )
  }

  it should "not signal-partition the user's data link into Finally" in {
    // Partitioning is PER-LINK: the tail wired to Finally keeps two links out
    // of the same output port -- the user's data link (normal partitioning, so
    // results actually reach Finally) and the synthesized signal link
    // (tuple-dropping). Signal-partitioning the data link would silently empty
    // the frame's output.
    val result = TryCatchFramePass.run(buildFramePlan(), None)
    val merger = result.getOperator(opId("fin"))
    val gate = result.getOperator(opId("tc", TryCatchOpDesc.GATE_LAYER))

    // the tail feeds BOTH the merger's From Try and a gate signal port
    val fromTail = result.links.filter(l => l.fromOpId == opId("tryOp"))
    fromTail.map(l => (l.toOpId, l.toPortId)) shouldBe Set(
      (merger.id, PortIdentity()),
      (gate.id, PortIdentity(1, internal = true))
    )

    // the merger declares no signal requirement on any input port, so its
    // incoming data links resolve to ordinary partitioning
    merger.inputPorts.keys.foreach(portId =>
      merger.partitionRequirement.lift(portId.id).flatten should not be Some(
        SignalPartition()
      )
    )
  }

  it should "reject a cross-cone edge (try and catch subgraphs must be disjoint)" in {
    var plan = buildFramePlan()
    // wire tryOp also into catchOp: catch cone now overlaps the try cone
    plan = plan.addLink(link(opId("tryOp"), out0, opId("catchOp"), in0))
    assertThrows[IllegalArgumentException] {
      TryCatchFramePass.run(plan, None)
    }
  }

  it should "reject a try-cone edge that joins the region after the Finally (Merger bypass)" in {
    // The Merger is the frame's only exit. tryOp -> down while merger -> down
    // would join the raw (possibly failed) attempt with the released winner —
    // and close a cycle through the synthesized signal edges. Must be rejected
    // with the rule's own words, not surface later as a schema error.
    var plan = buildFramePlan()
    plan = plan.addLink(link(opId("tryOp"), out0, opId("down"), in0))
    val err = intercept[IllegalArgumentException] {
      TryCatchFramePass.run(plan, None)
    }
    err.getMessage should include("only through the Finally")
  }

  it should "reject Error Info feeding back into the frame's own try subgraph" in {
    // reporter -> cone op -> tails -> signal edges -> gate is a structural
    // cycle and a temporal paradox (the report exists only once the attempt
    // resolved) — validated with a clear message.
    var plan = buildFramePlan()
    val err = fixedSchemaOp("err")
    plan = plan.addOperator(err.propagateSchema())
    plan = plan.addLink(
      link(opId("tc", TryCatchOpDesc.GATE_LAYER), TryCatchOpDesc.ERROR_INFO_PORT, opId("err"), in0)
    )
    plan = plan.addLink(link(opId("err"), out0, opId("tryOp"), in0))
    val rejected = intercept[IllegalArgumentException] {
      TryCatchFramePass.run(plan, None)
    }
    rejected.getMessage should include("Error Info")
  }

  it should "allow Error Info to feed the catch subgraph (catch(SpecificError) wiring)" in {
    // The error is known exactly when the snapshot releases, so joining the
    // report with catch data is causally sound; the consumer stays an external
    // upstream of the catch cone, not a frame member.
    var plan = buildFramePlan()
    val err = fixedSchemaOp("err")
    plan = plan.addOperator(err.propagateSchema())
    plan = plan.addLink(
      link(opId("tc", TryCatchOpDesc.GATE_LAYER), TryCatchOpDesc.ERROR_INFO_PORT, opId("err"), in0)
    )
    plan = plan.addLink(link(opId("err"), out0, opId("catchOp"), in0))

    val result = TryCatchFramePass.run(plan, None)
    val config = gateConfig(result.getOperator(opId("tc", TryCatchOpDesc.GATE_LAYER)))
    config.ownConeOpIds should contain("catchOp/main")
    config.ownConeOpIds should not contain "err/main"
  }

  it should "allow Error Info consumers downstream of the frame (audit lane)" in {
    // The green 6->8 edge: Error Info flows past the Finally and may join the
    // post-frame region — without becoming a signal source or a cone member.
    var plan = buildFramePlan()
    val audit = fixedSchemaOp("audit")
    plan = plan.addOperator(audit.propagateSchema())
    plan = plan.addLink(
      link(
        opId("tc", TryCatchOpDesc.GATE_LAYER),
        TryCatchOpDesc.ERROR_INFO_PORT,
        opId("audit"),
        in0
      )
    )
    plan = plan.addLink(link(opId("audit"), out0, opId("down"), in0))

    val result = TryCatchFramePass.run(plan, None)
    val gate = result.getOperator(opId("tc", TryCatchOpDesc.GATE_LAYER))
    val signalPorts = gate.inputPorts.keys.filter(p => p.internal && p.id > 0)
    signalPorts should have size 1 // audit's tail did NOT become a signal source
    gateConfig(gate).ownConeOpIds should not contain "audit/main"
  }

  it should "allow external upstreams to join either cone without joining the frame" in {
    // The green edges into the cones: attribution follows the FAILING
    // operator, not the data's destination — a side input is not guarded by
    // the frame it happens to feed.
    var plan = buildFramePlan()
    val side = sourceOp("side")
    plan = plan.addOperator(side.propagateSchema())
    plan = plan.addLink(link(opId("side"), out0, opId("tryOp"), in0))
    plan = plan.addLink(link(opId("side"), out0, opId("catchOp"), in0))

    val result = TryCatchFramePass.run(plan, None)
    val config = gateConfig(result.getOperator(opId("tc", TryCatchOpDesc.GATE_LAYER)))
    config.ownConeOpIds should contain("tryOp/main")
    config.ownConeOpIds should contain("catchOp/main")
    config.ownConeOpIds should not contain "side/main"
  }

  it should "signal a nested Finally-less frame's terminal catch leaf exactly once" in {
    // A terminal catch leaf of a nested (Finally-less) frame qualifies as an
    // enclosing-frame-owned tail AND as an escalation tap. It must get ONE
    // signal edge, not two: signal ports are dependees, dependee edges
    // materialize their source port, and a duplicate would race to create
    // the same storage table (iceberg commit conflict).
    val tc2Desc = new TryCatchOpDesc()
    tc2Desc.setOperatorId("tc2")
    val tc2Plan = tc2Desc.getPhysicalPlan(wid, eid)
    val splitter2Id = opId("tc2", TryCatchOpDesc.SPLITTER_LAYER)
    val gate2Id = opId("tc2", TryCatchOpDesc.GATE_LAYER)

    var plan = buildFramePlan()
    (Set(simpleOp("tryOp2"), simpleOp("catchOp2")) ++ tc2Plan.operators).foreach { op =>
      plan = plan.addOperator(op.propagateSchema())
    }
    List(
      // the outer Try also feeds a nested frame whose branches are terminal
      link(opId("tc", TryCatchOpDesc.SPLITTER_LAYER), out0, splitter2Id, in0),
      tc2Plan.links.head, // splitter2 -> gate2 snapshot
      link(splitter2Id, out0, opId("tryOp2"), in0),
      link(gate2Id, catchPort, opId("catchOp2"), in0)
    ).foreach(l => plan = plan.addLink(l))

    val result = TryCatchFramePass.run(plan, None)
    val outerGate = result.getOperator(opId("tc", TryCatchOpDesc.GATE_LAYER))
    val fromCatchLeaf =
      result.links.filter(l => l.fromOpId == opId("catchOp2") && l.toOpId == outerGate.id)
    fromCatchLeaf should have size 1
    // inner try tail signals the inner gate, not the outer one
    val innerGate = result.getOperator(gate2Id)
    result.links.count(l => l.fromOpId == opId("tryOp2") && l.toOpId == innerGate.id) shouldBe 1
    result.links.count(l => l.fromOpId == opId("tryOp2") && l.toOpId == outerGate.id) shouldBe 0
  }

  it should "reject a Finally-less frame flowing into an enclosing frame's Finally (close inside-out)" in {
    // Try1 -> Try2 -> Finally(of 1): the inner frame never closes — its
    // unbounded cone would swallow the outer Merger, starving its own gate
    // of signal edges (failures in its try branch would be invisible) and
    // mis-owning everything past the Finally. Must be rejected, telling the
    // user to close the inner frame first.
    val tc1Desc = new TryCatchOpDesc()
    tc1Desc.setOperatorId("tc1")
    val tc2Desc = new TryCatchOpDesc()
    tc2Desc.setOperatorId("tc2")
    val finDesc = new FinallyOpDesc()
    finDesc.setOperatorId("fin1")
    val tc1Plan = tc1Desc.getPhysicalPlan(wid, eid)
    val tc2Plan = tc2Desc.getPhysicalPlan(wid, eid)
    val merger = finDesc.getPhysicalOp(wid, eid)
    val splitter1Id = opId("tc1", TryCatchOpDesc.SPLITTER_LAYER)
    val gate1Id = opId("tc1", TryCatchOpDesc.GATE_LAYER)
    val splitter2Id = opId("tc2", TryCatchOpDesc.SPLITTER_LAYER)
    val gate2Id = opId("tc2", TryCatchOpDesc.GATE_LAYER)

    var plan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    (Set(
      sourceOp("src"),
      simpleOp("tryOp"),
      simpleOp("catchOp2"),
      simpleOp("catchOp"),
      merger
    ) ++ tc1Plan.operators ++ tc2Plan.operators).foreach { op =>
      plan = plan.addOperator(op.propagateSchema())
    }
    List(
      link(opId("src"), out0, splitter1Id, in0),
      tc1Plan.links.head,
      tc2Plan.links.head,
      link(splitter1Id, out0, splitter2Id, in0), // Try1 -> Try2
      link(splitter2Id, out0, opId("tryOp"), in0), // Try2's try branch...
      link(opId("tryOp"), out0, merger.id, PortIdentity()), // ...into Finally(of 1)!
      link(gate2Id, catchPort, opId("catchOp2"), in0), // Try2's catch (terminal)
      link(gate1Id, catchPort, opId("catchOp"), in0), // Try1's catch
      link(opId("catchOp"), out0, merger.id, PortIdentity(1))
    ).foreach(l => plan = plan.addLink(l))

    val rejected = intercept[IllegalArgumentException] {
      TryCatchFramePass.run(plan, None)
    }
    rejected.getMessage should include("inside-out")
    rejected.getMessage should include("tc2")
  }

  it should "reject a Finally whose From Try comes from outside the frame" in {
    val tcDesc = new TryCatchOpDesc()
    tcDesc.setOperatorId("tc")
    val fDesc = new FinallyOpDesc()
    fDesc.setOperatorId("fin")
    val tcPlan = tcDesc.getPhysicalPlan(wid, eid)
    val merger = fDesc.getPhysicalOp(wid, eid)
    val src = sourceOp("src")
    val stranger = sourceOp("stranger")
    val tryOp = simpleOp("tryOp")
    val catchOp = simpleOp("catchOp")
    val splitterId = opId("tc", TryCatchOpDesc.SPLITTER_LAYER)
    val gateId = opId("tc", TryCatchOpDesc.GATE_LAYER)

    var plan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    (Set(src, stranger, tryOp, catchOp, merger) ++ tcPlan.operators).foreach { op =>
      plan = plan.addOperator(op.propagateSchema())
    }
    List(
      link(opId("src"), out0, splitterId, in0),
      tcPlan.links.head,
      link(splitterId, out0, opId("tryOp"), in0),
      link(gateId, catchPort, opId("catchOp"), in0),
      link(opId("tryOp"), out0, merger.id, PortIdentity()),
      link(opId("stranger"), out0, merger.id, PortIdentity()), // outside the frame!
      link(opId("catchOp"), out0, merger.id, PortIdentity(1))
    ).foreach(l => plan = plan.addLink(l))

    assertThrows[IllegalArgumentException] {
      TryCatchFramePass.run(plan, None)
    }
  }
}
