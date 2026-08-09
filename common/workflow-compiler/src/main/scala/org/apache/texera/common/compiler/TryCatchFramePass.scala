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
import org.apache.texera.amber.core.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.trycatch.{
  CatchGateConfig,
  FinallyMergerConfig,
  TryCatchOpDesc
}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Compile-time expansion of try/catch frames.
  *
  * For every TryCatch frame on the physical plan, this pass:
  *  1. pairs each Finally with its TryCatch (structurally, via reachability);
  *  2. computes the frame's try/catch cones (inclusive of nested frames) and
  *     bakes them into the gate/merger executor configs for error attribution;
  *  3. synthesizes signal edges — one per try-cone tail output port — into the
  *     gate's signal ports, with `SignalPartition` requirements so the links
  *     carry only States/completion (tuples dropped at the sender);
  *  4. declares the gate's snapshot port dependent on all signal ports, which
  *     is what sequences the catch side after the try side resolves;
  *  5. validates the frame wiring rules (disjoint cones, Finally provenance,
  *     connected ports).
  *
  * The pass only reads/writes the physical plan: no scheduler or coordinator
  * involvement. Frames appear to the rest of compilation as ordinary operators.
  */
object TryCatchFramePass {

  private val SplitterClassName =
    "org.apache.texera.amber.operator.trycatch.TrySplitterOpExec"
  private val GateClassName =
    "org.apache.texera.amber.operator.trycatch.CatchGateOpExec"
  private val MergerClassName =
    "org.apache.texera.amber.operator.trycatch.FinallyMergerOpExec"

  private val TRY_PORT = PortIdentity()
  private val CATCH_PORT = PortIdentity(1)
  private val ERROR_INFO_PORT = TryCatchOpDesc.ERROR_INFO_PORT
  private val FROM_TRY = PortIdentity()
  private val FROM_CATCH = PortIdentity(1)

  private case class Frame(
      splitter: PhysicalOp,
      gate: PhysicalOp,
      merger: Option[PhysicalOp],
      tryCone: Set[PhysicalOpIdentity],
      catchCone: Set[PhysicalOpIdentity]
  ) {
    def logicalOpId: OperatorIdentity = splitter.id.logicalOpId
    def cone: Set[PhysicalOpIdentity] = tryCone ++ catchCone
    def coneIdStrings: List[String] =
      cone.toList.map(id => s"${id.logicalOpId.id}/${id.layerName}").sorted
  }

  def run(
      plan: PhysicalPlan,
      errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]]
  ): PhysicalPlan = {
    val splitters = opsWithClassName(plan, SplitterClassName)
    if (splitters.isEmpty) {
      return plan // no frames: zero-cost for ordinary workflows
    }
    try {
      expandFrames(plan)
    } catch {
      case err: Throwable =>
        errorList match {
          case Some(list) =>
            // attribute to the first frame's logical operator for UI display
            list.append((splitters.head.id.logicalOpId, err))
            plan
          case None => throw err
        }
    }
  }

  private def opsWithClassName(plan: PhysicalPlan, className: String): List[PhysicalOp] =
    plan.operators.toList
      .filter(op =>
        op.opExecInitInfo match {
          case OpExecWithClassName(cls, _) => cls == className
          case _                           => false
        }
      )
      .sortBy(_.id.logicalOpId.id)

  private def expandFrames(plan: PhysicalPlan): PhysicalPlan = {
    val splitters = opsWithClassName(plan, SplitterClassName)
    val mergers = opsWithClassName(plan, MergerClassName)

    // ---- 1. pairing: Finally F belongs to TryCatch T iff F's From Try is fed
    // from T's try side and F's From Catch from T's catch side. The pairing
    // traversal does not cross frame-internal (snapshot) links, which is what
    // makes the pairing unique under nesting.
    val gates = splitters.map(s => s.id.logicalOpId -> gateOf(plan, s)).toMap

    val mergerToFrame: Map[PhysicalOpIdentity, OperatorIdentity] = mergers.flatMap { merger =>
      val fromTrySources = sourcesInto(plan, merger.id, FROM_TRY)
      val fromCatchSources = sourcesInto(plan, merger.id, FROM_CATCH)
      val candidates = splitters.filter { s =>
        val tryReach = reach(plan, s.id, TRY_PORT, crossInternal = false, stopAt = Set.empty)
        val catchReach =
          reach(plan, gates(s.id.logicalOpId).id, CATCH_PORT, crossInternal = false, Set.empty)
        fromTrySources.exists(src => tryReach.contains(src) || src == s.id) &&
        fromCatchSources.exists(src => catchReach.contains(src))
      }
      candidates match {
        case single :: Nil => Some(merger.id -> single.id.logicalOpId)
        case Nil =>
          throw new IllegalArgumentException(
            s"Finally '${merger.id.logicalOpId.id}' is not wired to any TryCatch: " +
              "its From Try / From Catch inputs must come from the same frame's try / catch subgraphs"
          )
        case multiple =>
          throw new IllegalArgumentException(
            s"Finally '${merger.id.logicalOpId.id}' matches multiple TryCatch frames: " +
              multiple.map(_.id.logicalOpId.id).mkString(", ")
          )
      }
    }.toMap

    // ---- 2. cones (ownership; inclusive of nested frames, cut at own merger)
    val frames: List[Frame] = splitters.map { s =>
      val gate = gates(s.id.logicalOpId)
      val merger =
        mergerToFrame.collectFirst {
          case (mergerId, frameId) if frameId == s.id.logicalOpId => plan.getOperator(mergerId)
        }
      val stop = merger.map(_.id).toSet
      val tryCone = reach(plan, s.id, TRY_PORT, crossInternal = true, stopAt = stop)
      val catchCone = reach(plan, gate.id, CATCH_PORT, crossInternal = true, stopAt = stop)
      Frame(s, gate, merger, tryCone, catchCone)
    }

    // ---- 3. validations
    frames.foreach(validateFrame(plan, _))

    // Finallys close inside-out: a frame WITHOUT a Finally is terminal — its
    // branches must end in their own sinks. If its cone flows into another
    // frame's Finally (one that is not nested inside it), the inner frame
    // never closes: its unbounded cone would swallow the outer Merger and
    // everything past it, mis-owning failures and starving its own gate of
    // signal edges. Reject with the rule's own words.
    frames.filter(_.merger.isEmpty).foreach { open =>
      frames
        .filter(other =>
          other.merger.exists(m => open.cone.contains(m.id)) &&
            !open.cone.contains(other.splitter.id)
        )
        .foreach { other =>
          throw new IllegalArgumentException(
            s"TryCatch '${open.logicalOpId.id}' has no Finally, but its subgraph flows into " +
              s"the Finally of '${other.logicalOpId.id}'. A frame without a Finally must end " +
              s"in its own sinks — add a Finally to '${open.logicalOpId.id}' to continue past " +
              "it (Finallys close inside-out)."
          )
        }
    }

    // ---- 4. per-op innermost-frame assignment (smallest containing cone)
    def innermostFrameOf(opId: PhysicalOpIdentity): Option[Frame] =
      frames.filter(_.tryCone.contains(opId)).sortBy(_.tryCone.size).headOption

    // tail ports of a frame's try cone: no outgoing links, or feeding the
    // paired merger's From Try — owned by the innermost frame only
    val signalSources: Map[OperatorIdentity, List[(PhysicalOpIdentity, PortIdentity)]] =
      frames.map { frame =>
        val tails = frame.tryCone.toList
          .filter(opId => innermostFrameOf(opId).exists(_.logicalOpId == frame.logicalOpId))
          .flatMap { opId =>
            val op = plan.getOperator(opId)
            op.outputPorts.keys
              .filterNot(_.internal)
              .filter { portId =>
                val outLinks = plan.links.filter(l => l.fromOpId == opId && l.fromPortId == portId)
                outLinks.isEmpty ||
                frame.merger.exists(m =>
                  outLinks.exists(l => l.toOpId == m.id && l.toPortId == FROM_TRY)
                )
              }
              .map(portId => (opId, portId))
          }
          .sortBy { case (opId, portId) => (opId.logicalOpId.id, opId.layerName, portId.id) }
        frame.logicalOpId -> tails
      }.toMap

    // escalation: catch-cone TERMINAL leaves signal the innermost enclosing
    // frame's gate, so catch-side failures escalate even when their branch
    // never reaches the Merger. Applies to every frame: leaves feeding the
    // Merger's From Catch are excluded below (they have outgoing links; their
    // errors escalate through the Merger's forwarding), but a failing terminal
    // FORK of a catch branch would otherwise die at its result table while the
    // frame reported a clean recovery. A catch failure is an error this frame
    // did NOT handle — it must reach the enclosing frame (or, with none, the
    // console/failed-with-errors path), never the own gate (that edge would be
    // the cycle gate -> catch cone -> gate).
    val escalationSources: Map[OperatorIdentity, List[(PhysicalOpIdentity, PortIdentity)]] =
      frames
        .flatMap { frame =>
          frames
            .filter(f =>
              f.logicalOpId != frame.logicalOpId && f.tryCone.contains(frame.splitter.id)
            )
            .sortBy(_.tryCone.size)
            .headOption
            .map { enclosing =>
              val tails = frame.catchCone.toList.flatMap { opId =>
                val op = plan.getOperator(opId)
                op.outputPorts.keys
                  .filterNot(_.internal)
                  .filter(portId =>
                    plan.links.forall(l => !(l.fromOpId == opId && l.fromPortId == portId))
                  )
                  .map(portId => (opId, portId))
              }
              enclosing.logicalOpId -> tails
            }
        }
        .groupBy(_._1)
        .view
        .mapValues(_.flatMap(_._2).toList)
        .toMap

    // ---- 5. rebuild the plan with reconfigured gates/mergers + signal links
    val gateSignals: Map[PhysicalOpIdentity, List[(PhysicalOpIdentity, PortIdentity)]] =
      frames.map { frame =>
        // distinct: a nested frame's TERMINAL catch leaf is both an
        // enclosing-frame-owned tail (signal source) and an escalation tap.
        // Wiring it twice would materialize the same source port twice
        // (signal ports are dependees), racing to create one storage table.
        val signals = (signalSources(frame.logicalOpId) ++
          escalationSources.getOrElse(frame.logicalOpId, List.empty)).distinct
        frame.gate.id -> signals
      }.toMap

    // guarded = drain semantics on own failure (error State in-band). Marks
    // every cone operator plus the frame apparatus itself; everything outside
    // any frame keeps the default report-and-pause behavior.
    val guardedOps: Set[PhysicalOpIdentity] =
      frames.flatMap(f => f.cone + f.splitter.id + f.gate.id ++ f.merger.map(_.id)).toSet

    val rebuiltOps: Map[PhysicalOpIdentity, PhysicalOp] = plan.operators.map { op =>
      val rebuilt = frames.find(_.gate.id == op.id) match {
        case Some(frame) =>
          val signals = gateSignals(frame.gate.id)
          val signalPorts = signals.zipWithIndex.map {
            case (_, idx) => InputPort(PortIdentity(idx + 1, internal = true), s"signal-${idx + 1}")
          }
          val snapshotPort = InputPort(
            TryCatchOpDesc.SNAPSHOT_IN,
            "snapshot",
            dependencies = signalPorts.map(_.id)
          )
          val config = new CatchGateConfig()
          config.ownConeOpIds = frame.coneIdStrings
          op.withInputPorts(snapshotPort :: signalPorts)
            .withPartitionRequirement(
              List(None) ++ signalPorts.map(_ => Some(SignalPartition()))
            )
            .copy(opExecInitInfo =
              OpExecWithClassName(GateClassName, objectMapper.writeValueAsString(config))
            )
        case None =>
          frames.find(_.merger.exists(_.id == op.id)) match {
            case Some(frame) =>
              val config = new FinallyMergerConfig()
              config.ownConeOpIds = frame.coneIdStrings
              op.copy(opExecInitInfo =
                OpExecWithClassName(MergerClassName, objectMapper.writeValueAsString(config))
              )
            case None => op
          }
      }
      rebuilt.id -> rebuilt.withGuarded(guardedOps.contains(rebuilt.id))
    }.toMap

    val signalLinks: Set[PhysicalLink] = gateSignals.flatMap {
      case (gateId, signals) =>
        signals.zipWithIndex.map {
          case ((srcOpId, srcPortId), idx) =>
            PhysicalLink(srcOpId, srcPortId, gateId, PortIdentity(idx + 1, internal = true))
        }
    }.toSet

    rebuildPlan(rebuiltOps, plan.links ++ signalLinks)
  }

  /**
    * Rebuild the plan from scratch: ops stripped of link bookkeeping and
    * re-linked in topological order so `addLink`'s schema propagation always
    * sees a resolved source schema (mirrors `WorkflowCompiler.expandLogicalPlan`).
    *
    * The order must come from the NEW edge set, not the original plan's: a
    * synthesized signal edge runs from a try-cone leaf (late in the original
    * order) back to that frame's gate (early), so ordering by the original
    * index would add the gate's OUTGOING links before its incoming signal
    * edge — propagating an unresolved schema into the catch subgraph, which
    * later surfaces as `SchemaNotAvailableException` from the compiler's
    * strict schema check.
    */
  private def rebuildPlan(
      ops: Map[PhysicalOpIdentity, PhysicalOp],
      links: Set[PhysicalLink]
  ): PhysicalPlan = {
    val topoIndex = topologicalIndex(ops.keySet, links)

    var plan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    ops.values.toList.sortBy(op => topoIndex(op.id)).foreach { op =>
      val stripped = op
        .withInputPorts(op.inputPorts.values.map(_._1).toList)
        .withOutputPorts(op.outputPorts.values.map(_._1).toList)
      plan = plan.addOperator(stripped.propagateSchema())
    }
    links.toList
      .sortBy(link => (topoIndex(link.fromOpId), topoIndex(link.toOpId), link.toPortId.id))
      .foreach(link => plan = plan.addLink(link))
    plan
  }

  /** Kahn topological order over the given operators and links (the graph is
    * acyclic by construction: frames only add leaf->gate and escalation edges,
    * and try/catch cones are validated disjoint). Any operator left in a cycle
    * is appended last so a malformed graph fails in the compiler's own checks
    * rather than here.
    */
  private def topologicalIndex(
      opIds: Set[PhysicalOpIdentity],
      links: Set[PhysicalLink]
  ): Map[PhysicalOpIdentity, Int] = {
    val outgoing = links.groupBy(_.fromOpId).view.mapValues(_.toList.map(_.toOpId)).toMap
    val inDegree = mutable.Map(opIds.toList.map(_ -> 0): _*)
    links.foreach(link => inDegree(link.toOpId) = inDegree(link.toOpId) + 1)

    // deterministic tie-breaking: sort ready operators by id
    val ready = mutable.SortedSet[PhysicalOpIdentity]()(
      Ordering.by((id: PhysicalOpIdentity) => (id.logicalOpId.id, id.layerName))
    )
    inDegree.filter(_._2 == 0).keys.foreach(ready.add)

    val order = mutable.ArrayBuffer[PhysicalOpIdentity]()
    while (ready.nonEmpty) {
      val next = ready.head
      ready.remove(next)
      order.append(next)
      outgoing.getOrElse(next, Nil).foreach { downstream =>
        inDegree(downstream) = inDegree(downstream) - 1
        if (inDegree(downstream) == 0) ready.add(downstream)
      }
    }
    val remaining = opIds.diff(order.toSet).toList.sortBy(id => id.logicalOpId.id)
    (order.toList ++ remaining).zipWithIndex.toMap
  }

  private def gateOf(plan: PhysicalPlan, splitter: PhysicalOp): PhysicalOp = {
    val gateId = PhysicalOpIdentity(splitter.id.logicalOpId, TryCatchOpDesc.GATE_LAYER)
    plan.getOperator(gateId)
  }

  private def sourcesInto(
      plan: PhysicalPlan,
      opId: PhysicalOpIdentity,
      portId: PortIdentity
  ): List[PhysicalOpIdentity] =
    plan.links.filter(l => l.toOpId == opId && l.toPortId == portId).map(_.fromOpId).toList

  /**
    * BFS over the plan's links starting from one operator's output port.
    * `crossInternal = false` refuses to traverse links landing on internal
    * ports (frame-internal snapshot wiring) — used for pairing, where crossing
    * into a nested frame's catch side would break uniqueness. `stopAt` ops are
    * neither included nor expanded (the frame's own merger).
    */
  private def reach(
      plan: PhysicalPlan,
      fromOpId: PhysicalOpIdentity,
      fromPortId: PortIdentity,
      crossInternal: Boolean,
      stopAt: Set[PhysicalOpIdentity]
  ): Set[PhysicalOpIdentity] = {
    val visited = mutable.Set[PhysicalOpIdentity]()
    val frontier = mutable.Queue[PhysicalOpIdentity]()

    def linkAllowed(link: PhysicalLink): Boolean =
      crossInternal || !link.toPortId.internal

    plan.links
      .filter(l => l.fromOpId == fromOpId && l.fromPortId == fromPortId)
      .filter(linkAllowed)
      .map(_.toOpId)
      .filterNot(stopAt.contains)
      .foreach(op => if (visited.add(op)) frontier.enqueue(op))

    while (frontier.nonEmpty) {
      val current = frontier.dequeue()
      plan.links
        .filter(_.fromOpId == current)
        .filter(linkAllowed)
        .map(_.toOpId)
        .filterNot(stopAt.contains)
        .foreach(op => if (visited.add(op)) frontier.enqueue(op))
    }
    visited.toSet
  }

  private def validateFrame(plan: PhysicalPlan, frame: Frame): Unit = {
    val name = frame.logicalOpId.id
    if (frame.tryCone.isEmpty) {
      throw new IllegalArgumentException(
        s"TryCatch '$name': the Try port must be connected to the subgraph to guard"
      )
    }
    val overlap = frame.tryCone.intersect(frame.catchCone)
    if (overlap.nonEmpty) {
      throw new IllegalArgumentException(
        s"TryCatch '$name': the try and catch subgraphs must be disjoint; shared operators: " +
          overlap.map(_.logicalOpId.id).mkString(", ")
      )
    }
    // Error Info feeding back into the try subgraph is a structural cycle
    // (reporter -> cone op -> tails -> signal edges -> gate) and a temporal
    // paradox (the report exists only once the attempt resolved). Catch-cone
    // consumers are fine: they become part of the catch branch.
    val errorInfoReach =
      reach(plan, frame.gate.id, ERROR_INFO_PORT, crossInternal = true, stopAt = Set.empty)
    val feedback = errorInfoReach.intersect(frame.tryCone)
    if (feedback.nonEmpty) {
      throw new IllegalArgumentException(
        s"TryCatch '$name': Error Info cannot feed back into the frame's own try subgraph; " +
          s"offending: ${feedback.map(_.logicalOpId.id).mkString(", ")}"
      )
    }
    frame.merger.foreach { merger =>
      val fromTrySources = sourcesInto(plan, merger.id, FROM_TRY).toSet
      val fromCatchSources = sourcesInto(plan, merger.id, FROM_CATCH).toSet
      val badTry = fromTrySources.diff(frame.tryCone + frame.splitter.id)
      if (badTry.nonEmpty) {
        throw new IllegalArgumentException(
          s"Finally of '$name': every From Try input must come from the frame's try subgraph; " +
            s"offending: ${badTry.map(_.logicalOpId.id).mkString(", ")}"
        )
      }
      val badCatch = fromCatchSources.diff(frame.catchCone + frame.gate.id)
      if (badCatch.nonEmpty) {
        throw new IllegalArgumentException(
          s"Finally of '$name': every From Catch input must come from the frame's catch subgraph; " +
            s"offending: ${badCatch.map(_.logicalOpId.id).mkString(", ")}"
        )
      }
      val catchConnected =
        plan.links.exists(l => l.fromOpId == frame.gate.id && l.fromPortId == CATCH_PORT)
      if (!catchConnected) {
        throw new IllegalArgumentException(
          s"TryCatch '$name': a paired Finally requires the Catch port to be connected"
        )
      }
      // The Merger is the frame's only exit: a cone operator wired around it
      // into its downstream would join a branch's raw (possibly failed)
      // output with the released winner, and the synthesized signal edges
      // would close a cycle (merger -> join -> tail -> gate -> catch ->
      // merger). Reject the bypass here with the rule's own words instead of
      // letting the cycle surface later as a schema error.
      val mergerReach = merger.outputPorts.keys
        .filterNot(_.internal)
        .flatMap(portId => reach(plan, merger.id, portId, crossInternal = true, stopAt = Set.empty))
        .toSet
      val bypass = mergerReach.intersect(frame.cone + frame.splitter.id + frame.gate.id)
      if (bypass.nonEmpty) {
        throw new IllegalArgumentException(
          s"TryCatch '$name': the try/catch subgraphs may reach the region after the Finally " +
            s"only through the Finally itself; offending: " +
            bypass.map(_.logicalOpId.id).mkString(", ")
        )
      }
    }
  }
}
