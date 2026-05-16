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
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.macroOp.{
  MacroBody,
  MacroInputOp,
  MacroLink,
  MacroOpDesc,
  MacroOutputOp
}
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

    // TODO §9.2: if (m.fusion.exists(_.verified)) substitute a single
    // PythonUDFOpDescV2 instead of fetching/inlining the body.

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

    // Deep-clone non-marker inner ops via JSON round-trip and prefix their IDs.
    val innerOps: List[LogicalOp] = body.operators.collect {
      case op if !op.isInstanceOf[MacroInputOp] && !op.isInstanceOf[MacroOutputOp] =>
        deepClone(op)
    }

    val idRewrite: Map[OperatorIdentity, OperatorIdentity] = innerOps.map { op =>
      val originalId = op.operatorIdentifier
      val newId = s"$instanceId--${op.operatorIdentifier.id}"
      op.setOperatorId(newId)
      originalId -> op.operatorIdentifier
    }.toMap

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
}
