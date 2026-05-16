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

package org.apache.texera.amber.compiler.macroOp

import org.apache.texera.amber.compiler.model.{LogicalLink, LogicalPlan}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{LogicalOp, PortDescription}
import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
import org.apache.texera.amber.operator.macroOp.{
  MacroBody,
  MacroInputOp,
  MacroLink,
  MacroOpDesc,
  MacroOutputOp
}
import org.apache.texera.amber.util.JSONUtils.objectMapper

// Pre-compile pass: walks a LogicalPlan, inlines every MacroOpDesc by splicing its
// body's inner operators and links into the parent, and produces a flat LogicalPlan
// with no MacroOpDesc / MacroInputOp / MacroOutputOp nodes. Inner-op IDs are rewritten
// to "${macroInstanceId}--${innerOpId}" so telemetry can be aggregated per macro
// purely from the operator-ID prefix — the physical-plan layer remains macro-unaware.
// Note: "--" is chosen over "/" because "/" breaks VFS URI path parsing.
object MacroExpander {

  def expand(plan: LogicalPlan, registry: MacroRegistry): LogicalPlan =
    expand(plan, registry, MacroCompileContext.root)

  private def expand(
      plan: LogicalPlan,
      registry: MacroRegistry,
      ctx: MacroCompileContext
  ): LogicalPlan = {
    // Each iteration picks the first remaining MacroOpDesc and inlines it. After
    // inlining, the plan shape changes; loop re-scans the fresh `acc`.
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

    // §9.2 AI fusion substitution — mirror of the amber WorkflowCompiler's
    // path so the compiling-service (which provides schema-propagation
    // hints to the frontend) sees the same shape as the runtime engine.
    // Frontend sets `fusion.verified = true` after sample-diff verification.
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

    // Assign DETERMINISTIC UUIDs to each inner op via nameUUIDFromBytes
    // keyed on (macroInstanceId, originalBodyOpId). Must match the amber
    // MacroExpander byte-for-byte — Texera compiles this workflow twice
    // (once here for frontend validation, once in amber for actual
    // execution); the engine emits stats keyed by the second compile's IDs,
    // and `MacroMappingCache` records them. If the IDs differed across
    // compilers, the frontend's stats-roll-up to the macro op would fail
    // because the cached mapping wouldn't match the actual runtime IDs.
    val idRewrite: Map[OperatorIdentity, OperatorIdentity] = innerOps.map { op =>
      val originalId = op.operatorIdentifier
      val seed = s"${m.operatorIdentifier.id}|${originalId.id}"
      val derivedUuid = java.util.UUID.nameUUIDFromBytes(seed.getBytes("UTF-8"))
      val freshId = s"${op.getClass.getSimpleName}-operator-$derivedUuid"
      op.setOperatorId(freshId)
      originalId -> op.operatorIdentifier
    }.toMap

    def rewriteInnerId(id: OperatorIdentity): OperatorIdentity =
      idRewrite.getOrElse(
        id,
        throw new IllegalStateException(
          s"MacroExpander: link references unknown inner op '${id.id}' (instance=$instanceId)"
        )
      )

    // 1. Internal body links (non-marker → non-marker), with prefixed IDs.
    val internalLinks: List[LogicalLink] = body.links.collect {
      case l if !markerIds.contains(l.fromOpId) && !markerIds.contains(l.toOpId) =>
        LogicalLink(rewriteInnerId(l.fromOpId), l.fromPortId, rewriteInnerId(l.toOpId), l.toPortId)
    }

    // 2. For each external input port, list the inner consumers connected via
    //    MacroInputOp_i. A port may fan out to multiple consumers.
    val inputConsumers: Map[Int, List[(OperatorIdentity, PortIdentity)]] =
      inputMarkers.map {
        case (portIndex, marker) =>
          val markerId = marker.operatorIdentifier
          val consumers = body.links
            .filter(_.fromOpId == markerId)
            .map(l => (rewriteInnerId(l.toOpId), l.toPortId))
          portIndex -> consumers
      }

    // 3. For each external output port, the single inner producer feeding
    //    MacroOutputOp_j. More than one producer is a malformed body.
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

    // 4. Rewrite parent links that touch this macro instance.
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

  // Deep-clone via JSON round-trip. Avoids mutating the persisted body when we
  // rewrite inner-op IDs in spliceIntoParent.
  private def deepClone(op: LogicalOp): LogicalOp = {
    val json = objectMapper.writeValueAsString(op)
    objectMapper.readValue(json, classOf[LogicalOp])
  }

  /**
    * §9.2 fusion substitution — replace MacroOpDesc with a single
    * PythonUDFOpDescV2 carrying the verified fused code. The new op
    * inherits the macro's external port shape and keeps the macro
    * instance ID so parent links don't need rewriting.
    */
  private def substituteFused(parent: LogicalPlan, m: MacroOpDesc): LogicalPlan = {
    val fusion = m.fusion.get
    val fused = new PythonUDFOpDescV2()
    fused.code = fusion.code
    // Schema propagation: see amber/.../MacroExpander.scala substituteFused
    // for the same rationale. retainInputColumns lets the engine carry the
    // input schema through to the output without a hand-declared
    // outputColumns list; workers=1 keeps the fused execution single-actor.
    fused.retainInputColumns = m.inputPortCount > 0
    fused.outputColumns = List.empty
    fused.workers = 1
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
    fused.setOperatorId(m.operatorIdentifier.id)
    val newOps = parent.operators.map {
      case op if op.operatorIdentifier == m.operatorIdentifier => fused
      case op => op
    }
    LogicalPlan(newOps, parent.links)
  }
}
