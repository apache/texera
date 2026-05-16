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

package org.apache.texera.workflow.macroOp

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{LogicalOp, PortDescription}
import org.apache.texera.amber.operator.macroOp.{
  MacroBody,
  MacroInputOp,
  MacroLink,
  MacroOpDesc,
  MacroOutputOp
}
import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.workflow.{LogicalLink, LogicalPlan}

// Pre-compile pass for the amber execution-time compiler. Walks a LogicalPlan,
// inlines every MacroOpDesc by splicing its body's inner operators and links
// into the parent, and produces a flat LogicalPlan with no MacroOpDesc /
// MacroInputOp / MacroOutputOp nodes. Inner-op IDs are rewritten to
// "${macroInstanceId}--${innerOpId}" so telemetry can be aggregated per macro
// purely from the operator-ID prefix — the physical-plan layer remains
// macro-unaware. "--" is used instead of "/" to avoid breaking VFS URI paths.
//
// Mirrors the compiling-service MacroExpander; the two operate on their own
// LogicalLink/LogicalPlan classes and will converge once those types are
// unified (see WorkflowCompiler.scala TODO).
object MacroExpander {

  /**
    * Provenance of one freshly-named inner op in the expanded plan.
    *
    * @param macroChain ordered list of macro instance IDs from outermost
    *                   (parent canvas) to innermost (immediate enclosing
    *                   macro). e.g. for an op deep inside nested macro 294
    *                   which itself sits inside macro 295: List("295_inst",
    *                   "294_inst_in_295_body").
    * @param bodyOpId   the original definition-time op ID this runtime op
    *                   was cloned from. Lets the drill-down view map runtime
    *                   stats back to definition-time positions when rendering
    *                   the macro body.
    */
  case class MacroProvenance(macroChain: List[String], bodyOpId: String)

  /**
    * Side-table from `runtime fresh-UUID → MacroProvenance`. Populated by
    * `spliceIntoParent` (handles nested macros: when an outer splice re-clones
    * an op that an inner splice already touched, the outer splice prepends its
    * macro instance to the existing chain and drops the stale inner UUID).
    *
    * The frontend reads this via `/api/workflow/{wid}/macro-mapping?eid=...`
    * to aggregate inner-op stats up to the macro op on the canvas, and to
    * route stats to body-level positions inside the drill-down editor.
    *
    * Threading model: not thread-safe; each compile call should drain via
    * `takeMacroInstanceMapping()` immediately after `expand` returns.
    */
  private val currentMacroInstanceMapping =
    scala.collection.mutable.Map[String, MacroProvenance]()

  /** Snapshot + clear the current mapping. The caller takes ownership. */
  def takeMacroInstanceMapping(): Map[String, MacroProvenance] = {
    val snapshot = currentMacroInstanceMapping.toMap
    currentMacroInstanceMapping.clear()
    snapshot
  }

  def expand(plan: LogicalPlan, registry: MacroRegistry): LogicalPlan =
    expand(plan, registry, MacroCompileContext.root)

  private def expand(
      plan: LogicalPlan,
      registry: MacroRegistry,
      ctx: MacroCompileContext
  ): LogicalPlan = {
    var acc = plan
    while (acc.operators.exists(_.isInstanceOf[MacroOpDesc])) {
      val m = acc.operators.collectFirst { case x: MacroOpDesc => x }.get
      acc = inlineMacro(acc, m, registry, ctx)
    }
    acc
  }

  private def inlineMacro(
      parent: LogicalPlan,
      m: MacroOpDesc,
      registry: MacroRegistry,
      ctx: MacroCompileContext
  ): LogicalPlan = {
    ctx.guardAgainstCycle(m.macroId, m.macroVersion)
    ctx.guardAgainstDepth()

    // §9.2 AI fusion: if the macro has been verified-fused into a single
    // PythonUDF, substitute that UDF for the entire inlined body instead
    // of expanding. This eliminates inter-actor handoffs for the chain
    // and is the perf-demo path of the hackathon's `fuseMacro` flow. The
    // frontend sets `fusion.verified = true` after running sample-diff
    // verification client-side; we trust that gate here because the
    // verification protocol is owned by the agent service.
    if (m.fusion.exists(_.verified)) {
      return substituteFused(parent, m)
    }

    val body: MacroBody = m.linkMode match {
      case MacroOpDesc.SNAPSHOT =>
        m.snapshot.getOrElse(
          throw new IllegalArgumentException(
            s"MacroOpDesc[${m.macroId}] has linkMode=SNAPSHOT but no embedded snapshot"
          )
        )
      case MacroOpDesc.LIVE =>
        registry
          .fetch(m.macroId, m.macroVersion)
          .getOrElse(
            throw new IllegalArgumentException(
              s"MacroOpDesc[${m.macroId}@v${m.macroVersion}] not found in registry " +
                s"(LIVE link). The macro may be deleted or inaccessible."
            )
          )
      case other =>
        throw new IllegalArgumentException(
          s"MacroOpDesc[${m.macroId}] has unknown linkMode '$other'"
        )
    }

    val expandedBody = expand(
      LogicalPlan(body.operators, body.links.map(toLogicalLink)),
      registry,
      ctx.descend(m.macroId, m.macroVersion)
    )

    spliceIntoParent(parent, m, expandedBody)
  }

  private def toLogicalLink(ml: MacroLink): LogicalLink =
    LogicalLink(
      OperatorIdentity(ml.fromOpId),
      ml.fromPortId,
      OperatorIdentity(ml.toOpId),
      ml.toPortId
    )

  private def spliceIntoParent(
      parent: LogicalPlan,
      m: MacroOpDesc,
      body: LogicalPlan
  ): LogicalPlan = {
    val instanceId = m.operatorIdentifier.id
    val mId = m.operatorIdentifier

    val inputMarkers: Map[Int, MacroInputOp] =
      body.operators.collect { case b: MacroInputOp => b.portIndex -> b }.toMap
    val outputMarkers: Map[Int, MacroOutputOp] =
      body.operators.collect { case b: MacroOutputOp => b.portIndex -> b }.toMap

    val markerIds: Set[OperatorIdentity] =
      inputMarkers.values.map(_.operatorIdentifier).toSet ++
        outputMarkers.values.map(_.operatorIdentifier).toSet

    // Deep-clone non-marker inner ops via JSON round-trip.
    val innerOps: List[LogicalOp] = body.operators.collect {
      case op if !op.isInstanceOf[MacroInputOp] && !op.isInstanceOf[MacroOutputOp] =>
        deepClone(op)
    }

    // Assign fresh UUIDs to each inner op. The expanded LogicalPlan must be
    // STRUCTURALLY IDENTICAL to a hand-flattened workflow — otherwise downstream
    // engine behavior (Iceberg materialization table naming, partition routing
    // based on op-ID hashes, region scheduling) silently diverges.
    //
    // CRITICAL: the UUIDs MUST be DETERMINISTIC across compiles. Texera has
    // two WorkflowCompiler implementations (one in workflow-compiling-service
    // for frontend validation, one in amber for actual execution). Both run
    // MacroExpander on the SAME workflow content. If we used
    // `UUID.randomUUID()` the two compilers would generate different IDs for
    // the same op; the frontend would cache one set (whichever wrote to
    // MacroMappingCache last) but the engine would emit stats keyed by the
    // OTHER set, so stat aggregation up to the macro op would silently fail.
    //
    // Solution: derive the UUID from `nameUUIDFromBytes(macroInstanceId | body
    // op id)`. For nested macros, the inner splice's freshId already encodes
    // the inner chain, so the outer splice's seed transitively captures the
    // whole chain. Same workflow → same UUIDs across compilers.
    //
    // The previous "${macroInstanceId}--${innerOpId}" prefix scheme was
    // convenient for stats aggregation but produced 170+ char op IDs, which
    // caused observable Iceberg commit thrash on HashJoin's internal build
    // port — execution that runs fine on a hand-flattened plan hangs on the
    // macro-wrapped equivalent. Deterministic UUIDs are short.
    val idRewrite: Map[OperatorIdentity, OperatorIdentity] = innerOps.map { op =>
      val originalId = op.operatorIdentifier
      val seed = s"${m.operatorIdentifier.id}|${originalId.id}"
      val derivedUuid = java.util.UUID.nameUUIDFromBytes(seed.getBytes("UTF-8"))
      val freshId = s"${op.getClass.getSimpleName}-operator-$derivedUuid"
      op.setOperatorId(freshId)
      originalId -> op.operatorIdentifier
    }.toMap

    // Update the provenance side-table. Two cases per renamed op:
    //   1. originalId IS already a fresh UUID from a prior (inner) splice:
    //      Take the inner provenance, prepend THIS macro instance to its
    //      chain, and move the entry to the new outer UUID.
    //   2. originalId is the macro body's definition-time op ID:
    //      Create a fresh provenance with chain=[mId] and bodyOpId=originalId.
    // Drops the stale inner-UUID entry so the side-table only references
    // op IDs that exist in the final expanded plan.
    idRewrite.foreach {
      case (originalId, newId) =>
        currentMacroInstanceMapping.get(originalId.id) match {
          case Some(existing) =>
            currentMacroInstanceMapping(newId.id) =
              MacroProvenance(mId.id :: existing.macroChain, existing.bodyOpId)
            if (newId.id != originalId.id) currentMacroInstanceMapping.remove(originalId.id)
          case None =>
            currentMacroInstanceMapping(newId.id) =
              MacroProvenance(List(mId.id), originalId.id)
        }
    }

    def rewriteInnerId(id: OperatorIdentity): OperatorIdentity =
      idRewrite.getOrElse(
        id,
        throw new IllegalStateException(
          s"MacroExpander: link references unknown inner op '${id.id}' (instance=$instanceId)"
        )
      )

    val internalLinks: List[LogicalLink] = body.links.collect {
      case l if !markerIds.contains(l.fromOpId) && !markerIds.contains(l.toOpId) =>
        LogicalLink(rewriteInnerId(l.fromOpId), l.fromPortId, rewriteInnerId(l.toOpId), l.toPortId)
    }

    val inputConsumers: Map[Int, List[(OperatorIdentity, PortIdentity)]] =
      inputMarkers.map {
        case (portIndex, marker) =>
          val markerId = marker.operatorIdentifier
          val consumers = body.links
            .filter(_.fromOpId == markerId)
            .map(l => (rewriteInnerId(l.toOpId), l.toPortId))
          portIndex -> consumers
      }

    val outputProducers: Map[Int, (OperatorIdentity, PortIdentity)] =
      outputMarkers.map {
        case (portIndex, marker) =>
          val markerId = marker.operatorIdentifier
          val producers = body.links
            .filter(_.toOpId == markerId)
            .map(l => (rewriteInnerId(l.fromOpId), l.fromPortId))
          producers match {
            case single :: Nil => portIndex -> single
            case Nil =>
              throw new IllegalStateException(
                s"MacroOutputOp(portIndex=$portIndex) in macro $instanceId has no producer"
              )
            case many =>
              throw new IllegalStateException(
                s"MacroOutputOp(portIndex=$portIndex) in macro $instanceId has " +
                  s"${many.size} producers; expected exactly one."
              )
          }
      }

    val rewrittenParentLinks: List[LogicalLink] = parent.links.flatMap { link =>
      if (link.toOpId == mId) {
        val portIndex = link.toPortId.id
        inputConsumers.get(portIndex) match {
          case Some(consumers) =>
            consumers.map {
              case (innerOp, innerPort) =>
                LogicalLink(link.fromOpId, link.fromPortId, innerOp, innerPort)
            }
          case None =>
            throw new IllegalStateException(
              s"Parent link into ($instanceId, port=$portIndex) has no matching " +
                s"MacroInputOp inside the macro body."
            )
        }
      } else if (link.fromOpId == mId) {
        val portIndex = link.fromPortId.id
        outputProducers.get(portIndex) match {
          case Some((innerOp, innerPort)) =>
            List(LogicalLink(innerOp, innerPort, link.toOpId, link.toPortId))
          case None =>
            throw new IllegalStateException(
              s"Parent link out of ($instanceId, port=$portIndex) has no matching " +
                s"MacroOutputOp inside the macro body."
            )
        }
      } else {
        List(link)
      }
    }

    val newOps =
      parent.operators.filterNot(_.operatorIdentifier == mId) ++ innerOps
    val newLinks = rewrittenParentLinks ++ internalLinks
    LogicalPlan(newOps, newLinks)
  }

  // Deep-clone via JSON round-trip to avoid mutating the persisted body when we
  // rewrite inner-op IDs in spliceIntoParent.
  private def deepClone(op: LogicalOp): LogicalOp = {
    val json = objectMapper.writeValueAsString(op)
    objectMapper.readValue(json, classOf[LogicalOp])
  }

  /**
    * §9.2 AI fusion substitution: when the macro has a `verified` `fusion`,
    * replace the entire MacroOpDesc + its inlined body with a single
    * PythonUDFOpDescV2 carrying the fused code. The substitute operator
    * inherits the macro's external input/output port count so all parent
    * links re-target it cleanly (1:1 port mapping, no fan-out).
    *
    * This is the gate that powers the hackathon demo's "fuse for
    * performance" path — once the frontend marks `fusion.verified = true`,
    * the engine never sees the original inlined body for this instance.
    */
  private def substituteFused(parent: LogicalPlan, m: MacroOpDesc): LogicalPlan = {
    val fusion = m.fusion.get
    val instanceId = m.operatorIdentifier.id
    val fused = new PythonUDFOpDescV2()
    fused.code = fusion.code
    // Schema propagation for the fused UDF: a fused macro that takes an input
    // re-emits a tuple of the same shape (filter/projection/map operators
    // mutate or drop the input dict but don't introduce new columns unless
    // the user adds them in the fused code). retainInputColumns=true lets the
    // engine carry the input schema through to the output without a hand-
    // declared outputColumns list. workers=1 keeps the fused execution
    // single-actor — the whole point of fusion is collapsing serialization
    // hops, not parallelism.
    fused.retainInputColumns = m.inputPortCount > 0
    fused.outputColumns = List.empty
    fused.workers = 1
    // Keep the macro op's external interface — same input/output port
    // counts so the upstream/downstream link wiring on the parent canvas
    // doesn't need to change.
    fused.inputPorts = (0 until m.inputPortCount).map { i =>
      PortDescription(
        portID = s"input-$i",
        displayName = s"in-$i",
        disallowMultiInputs = false,
        isDynamicPort = false,
        partitionRequirement = null,
        dependencies = List.empty
      )
    }.toList
    fused.outputPorts = (0 until m.outputPortCount).map { i =>
      PortDescription(
        portID = s"output-$i",
        displayName = s"out-$i",
        disallowMultiInputs = false,
        isDynamicPort = false,
        partitionRequirement = null,
        dependencies = List.empty
      )
    }.toList
    fused.setOperatorId(instanceId) // reuse the macro instance ID — no link rewrite needed
    // Replace the macro op in the parent with the fused UDF op. Links
    // already reference `instanceId` on both ends since `setOperatorId`
    // preserved it; no link rewrite required.
    val newOps = parent.operators.map {
      case op if op.operatorIdentifier == m.operatorIdentifier => fused
      case op => op
    }
    LogicalPlan(newOps, parent.links)
  }
}
