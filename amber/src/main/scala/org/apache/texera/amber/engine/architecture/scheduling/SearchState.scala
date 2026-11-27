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

import org.apache.texera.amber.core.virtualidentity.PhysicalOpIdentity
import org.apache.texera.amber.core.workflow.{
  CachedResult,
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalPlan,
  WorkflowContext
}
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  OutputPortConfig,
  ResourceConfig
}
import org.jgrapht.alg.connectivity.BiconnectivityInspector

import java.net.URI
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * Internal search choice for one edge during [[CostBasedScheduleGenerator]] planning:
  *
  *  - Materialized: the source operator runs and writes the port's output; the target reads the
  *    written output once the source's region has finished.
  *  - Pipelined: the source operator runs and sends each tuple to the target as it is produced.
  *  - CacheRead: the target reads the port's saved result instead of output produced in this run.
  *  - Unused: no data crosses the edge in this run because its target operator does not run.
  */
private[scheduling] sealed trait EdgeType
private[scheduling] object EdgeType {
  case object Materialized extends EdgeType
  case object Pipelined extends EdgeType
  case object CacheRead extends EdgeType
  case object Unused extends EdgeType
}

/**
  * Candidate 4-edge-state assignment searched by [[CostBasedScheduleGenerator]]. Holds the
  * [[EdgeType]] of each [[PhysicalLink]] in the plan and the set of required output ports satisfied
  * by reading saved results rather than recomputing.
  */
private[scheduling] final case class SearchState(
    linkTypes: Map[PhysicalLink, EdgeType],
    reusedOutputs: Set[GlobalPortIdentity] = Set.empty
) {
  import EdgeType._

  require(
    reusedOutputs.forall(!_.input),
    s"reusedOutputs requires output ports, got ${reusedOutputs.filter(_.input)}"
  )

  def apply(link: PhysicalLink): EdgeType = linkTypes(link)

  def updated(link: PhysicalLink, edgeType: EdgeType): SearchState =
    copy(linkTypes = linkTypes.updated(link, edgeType))

  def linksOfType(edgeType: EdgeType): Set[PhysicalLink] =
    linkTypes.collect { case (link, t) if t == edgeType => link }.toSet

  /**
    * The operators of `physicalPlan` that run under this state: those with no output ports, those
    * with a required output not satisfied by `reusedOutputs`, and those that are the source of a
    * `Materialized` or `Pipelined` link.
    */
  def retainedOperators(
      physicalPlan: PhysicalPlan,
      requiredOutputs: Set[GlobalPortIdentity]
  ): Set[PhysicalOpIdentity] = {
    val sinkOpsWithoutOutputPorts = physicalPlan.operators.filter(_.outputPorts.isEmpty).map(_.id)
    val linkSources = linkTypes.collect {
      case (link, edgeType) if edgeType == Materialized || edgeType == Pipelined => link.fromOpId
    }
    val requiredSources = requiredOutputs.diff(reusedOutputs).map(_.opId)
    sinkOpsWithoutOutputPorts ++ linkSources ++ requiredSources
  }

  /** The operators of `physicalPlan` that do not run under this state. */
  def skippedOperators(
      physicalPlan: PhysicalPlan,
      requiredOutputs: Set[GlobalPortIdentity]
  ): Set[PhysicalOpIdentity] =
    physicalPlan.operators.map(_.id).diff(retainedOperators(physicalPlan, requiredOutputs))

  /** Whether any operator of `physicalPlan` is skipped under this state. */
  def skipsAnything(
      physicalPlan: PhysicalPlan,
      requiredOutputs: Set[GlobalPortIdentity]
  ): Boolean =
    skippedOperators(physicalPlan, requiredOutputs).nonEmpty

  /**
    * The consumption condition: every plan link is typed, every reused output belongs to
    * `requiredOutputs`, and a link is `Unused` if and only if its target operator does not run.
    */
  def satisfiesConsumptionCondition(
      physicalPlan: PhysicalPlan,
      requiredOutputs: Set[GlobalPortIdentity]
  ): Boolean =
    linkTypes.keySet == physicalPlan.links &&
      reusedOutputs.subsetOf(requiredOutputs) && {
      val retainedOps = retainedOperators(physicalPlan, requiredOutputs)
      linkTypes.forall {
        case (link, edgeType) => (edgeType == Unused) == !retainedOps.contains(link.toOpId)
      }
    }

  /**
    * Resolves which operators run and which incoming links carry data so every running operator
    * receives all of its inputs (`CacheRead`, `Materialized`, or `Pipelined`) and every skipped
    * operator receives none (`Unused`).
    *
    * Why this is needed: `SearchState` assigns an `EdgeType` per link independently, so seeding
    * matched outputs or changing one link during search leaves upstream links out of sync with
    * which operators must run:
    *  - Skipping unneeded upstream work (input necessity): when `CacheRead` or `reusedOutputs`
    *    covers every needed output of an operator, that operator skips and its incoming links must
    *    become `Unused`, cascading upstream unless another running branch (such as a shared diamond
    *    or multi-output `Split`) still requires an upstream operator.
    *  - Supplying needed upstream inputs (input satisfiability): when an operator runs (no output
    *    ports, un-reused required output, or active `Materialized`/`Pipelined` outgoing link,
    *    including when a `CacheRead` link is switched back to recompute), any `Unused` incoming
    *    link becomes required and resolves to `CacheRead` if its source port is in `matchedResults`
    *    or to `defaultActiveType` (falling back to `Materialized`) otherwise, requiring active
    *    upstream producers to run.
    *
    * How it works: one reverse-topological pass (`physicalPlan.topologicalIterator().toList.reverse`)
    * propagates data-input requirements backward from sinks and `requiredOutputs` toward sources;
    * every downstream consumer of an operator is already resolved before that operator is visited.
    */
  def resolveInputRequirements(
      physicalPlan: PhysicalPlan,
      requiredOutputs: Set[GlobalPortIdentity],
      matchedResults: Map[GlobalPortIdentity, CachedResult] = Map.empty,
      allowPipelined: Boolean = true,
      defaultActiveType: PhysicalLink => EdgeType = _ => Materialized
  ): SearchState = {
    val allowedLinks = SearchState.allowedLinkTypes(physicalPlan, matchedResults, allowPipelined)
    require(
      linkTypes.keySet == physicalPlan.links,
      "linkTypes keys must match physicalPlan.links"
    )
    require(
      reusedOutputs.subsetOf(requiredOutputs),
      s"reusedOutputs must be a subset of requiredOutputs, got ${reusedOutputs.diff(requiredOutputs)}"
    )
    val updated = mutable.Map(linkTypes.toSeq: _*)
    val outgoingByOp = physicalPlan.links.groupBy(_.fromOpId).withDefaultValue(Set.empty)
    val incomingByOp = physicalPlan.links.groupBy(_.toOpId).withDefaultValue(Set.empty)
    val sinkOpsWithoutOutputPorts = physicalPlan.operators.filter(_.outputPorts.isEmpty).map(_.id)
    val requiredNotReusedOps = requiredOutputs.diff(reusedOutputs).map(_.opId)
    physicalPlan.topologicalIterator().toList.reverse.foreach { opId =>
      val runs = sinkOpsWithoutOutputPorts.contains(opId) ||
        requiredNotReusedOps.contains(opId) ||
        outgoingByOp(opId).exists { link =>
          val t = updated(link)
          t == Materialized || t == Pipelined
        }
      if (runs) {
        incomingByOp(opId).foreach { inLink =>
          if (updated(inLink) == Unused) {
            val sourcePort = GlobalPortIdentity(inLink.fromOpId, inLink.fromPortId)
            updated(inLink) =
              if (matchedResults.contains(sourcePort)) CacheRead
              else {
                val candidate = defaultActiveType(inLink)
                if (
                  candidate != Unused && candidate != CacheRead &&
                  allowedLinks(inLink).contains(candidate)
                ) candidate
                else Materialized
              }
          }
        }
      } else {
        incomingByOp(opId).foreach { inLink =>
          if (updated(inLink) != Unused) {
            updated(inLink) = Unused
          }
        }
      }
    }
    copy(linkTypes = updated.toMap)
  }

  /**
    * The output ports whose saved results this state reads, with each saved location and row
    * count: the source port of every `CacheRead` link, plus every reused required output of a
    * skipped operator. When a required output belongs to a retained operator, this run stores and
    * shows its own result for that port while retained operators reading it use its saved copy.
    */
  def cacheReadPorts(
      physicalPlan: PhysicalPlan,
      matchedResults: Map[GlobalPortIdentity, CachedResult],
      requiredOutputs: Set[GlobalPortIdentity]
  ): Map[GlobalPortIdentity, CachedResult] = {
    SearchState.requireOutputPortsOfPlan(physicalPlan, matchedResults)
    require(
      satisfiesConsumptionCondition(physicalPlan, requiredOutputs),
      "only a search state satisfying the consumption condition has cache-read ports"
    )
    val skippedOps = skippedOperators(physicalPlan, requiredOutputs)
    val readPorts =
      linksOfType(CacheRead).map(link => GlobalPortIdentity(link.fromOpId, link.fromPortId)) ++
        reusedOutputs.filter(port => skippedOps.contains(port.opId))
    readPorts.map(port => port -> matchedResults(port)).toMap
  }

  /**
    * The sub-plan of `physicalPlan` that runs under this state: the retained operators, keeping the
    * plan's own operator objects, and the links between them except `CacheRead` links.
    */
  def retainedPlan(
      physicalPlan: PhysicalPlan,
      requiredOutputs: Set[GlobalPortIdentity]
  ): PhysicalPlan = {
    require(
      satisfiesConsumptionCondition(physicalPlan, requiredOutputs),
      "only a search state satisfying the consumption condition has a retained plan"
    )
    val retained = physicalPlan.getSubPlan(retainedOperators(physicalPlan, requiredOutputs))
    retained.copy(links = retained.links.diff(linksOfType(CacheRead)))
  }

  /**
    * Saved-result locations keyed by each consumer input port over `CacheRead` links, sorted by
    * link so multi-writer ports (such as Union) keep a deterministic order.
    */
  def readerUrisByInputPort(
      physicalPlan: PhysicalPlan,
      matchedResults: Map[GlobalPortIdentity, CachedResult],
      requiredOutputs: Set[GlobalPortIdentity]
  ): Map[GlobalPortIdentity, List[URI]] = {
    val readPorts = cacheReadPorts(physicalPlan, matchedResults, requiredOutputs)
    linksOfType(CacheRead).toList
      .sortBy(_.toString)
      .foldLeft(Map.empty[GlobalPortIdentity, List[URI]]) { (acc, link) =>
        val sourcePort = GlobalPortIdentity(link.fromOpId, link.fromPortId)
        val inputPort = GlobalPortIdentity(link.toOpId, link.toPortId, input = true)
        val savedLocation = readPorts(sourcePort).storageUri
        acc.updated(inputPort, acc.getOrElse(inputPort, List.empty[URI]) :+ savedLocation)
      }
  }

  /**
    * The skipped operators of `physicalPlan` under this state as skip regions (`Region.skipped =
    * true`), one per connected component, with ids `firstRegionId`, `firstRegionId + 1`, ... in
    * deterministic order.
    */
  def skipRegions(
      physicalPlan: PhysicalPlan,
      matchedResults: Map[GlobalPortIdentity, CachedResult],
      requiredOutputs: Set[GlobalPortIdentity],
      firstRegionId: Long
  ): Set[Region] = {
    val skippedOps = skippedOperators(physicalPlan, requiredOutputs)
    val skippedLinks = linksOfType(Unused)
    val readPorts = cacheReadPorts(physicalPlan, matchedResults, requiredOutputs)
    val groups: List[Set[PhysicalOpIdentity]] =
      new BiconnectivityInspector[PhysicalOpIdentity, PhysicalLink](
        physicalPlan.getSubPlan(skippedOps).dag
      ).getConnectedComponents.asScala
        .map(_.vertexSet().asScala.toSet)
        .toList
        .sortBy(_.map(_.toString).min)
    groups.zipWithIndex.map {
      case (group, index) =>
        val physicalOps = group.map(physicalPlan.getOperator)
        Region(
          id = RegionIdentity(firstRegionId + index),
          physicalOps = physicalOps,
          physicalLinks = skippedLinks.filter(link =>
            group.contains(link.fromOpId) && group.contains(link.toOpId)
          ),
          ports = physicalOps.flatMap(op =>
            op.inputPorts.keys.map(portId => GlobalPortIdentity(op.id, portId, input = true)) ++
              op.outputPorts.keys.map(portId => GlobalPortIdentity(op.id, portId))
          ),
          resourceConfig = Some(
            ResourceConfig(portConfigs = readPorts.collect {
              case (port, saved) if group.contains(port.opId) =>
                port -> OutputPortConfig(saved.storageUri, saved.tupleCount)
            })
          ),
          skipped = true
        )
    }.toSet
  }
}

/**
  * Validation rules and seeds for [[SearchState]], internal to the scheduling package:
  *
  *  - Every key in `matchedResults` must be an output port of `physicalPlan`
  *    ([[SearchState.requireOutputPortsOfPlan]]).
  *  - Required outputs are `outputPortsNeedingStorage` plus every output port with no outgoing link.
  *  - An operator with no output ports always runs: nothing in the workflow reads it.
  *  - Under eager reuse (`eagerSeed`), every matched result is reused and a single reverse-
  *    topological pass marks links into skipped operators `Unused`.
  */
private[scheduling] object SearchState {
  import EdgeType._

  /** Verifies that every port in `matchedResults` is an output port of `physicalPlan`. */
  def requireOutputPortsOfPlan(
      physicalPlan: PhysicalPlan,
      matchedResults: Map[GlobalPortIdentity, CachedResult]
  ): Unit = {
    if (matchedResults.isEmpty) return
    val planOutputPorts = physicalPlan.operators.flatMap(op =>
      op.outputPorts.keys.map(portId => GlobalPortIdentity(op.id, portId))
    )
    val invalidPorts = matchedResults.keySet.diff(planOutputPorts)
    require(
      invalidPorts.isEmpty,
      s"matchedResults requires output ports of physicalPlan, got $invalidPorts"
    )
  }

  /**
    * The output ports the run must have results for by its end: `outputPortsNeedingStorage` and
    * every output port with no outgoing link.
    */
  def requiredOutputPorts(
      workflowContext: WorkflowContext,
      physicalPlan: PhysicalPlan
  ): Set[GlobalPortIdentity] = {
    val portsWithLinks =
      physicalPlan.links.map(link => GlobalPortIdentity(link.fromOpId, link.fromPortId))
    physicalPlan.operators
      .flatMap(op => op.outputPorts.keys.map(portId => GlobalPortIdentity(op.id, portId)))
      .filter(port =>
        workflowContext.workflowSettings.outputPortsNeedingStorage.contains(port) ||
          !portsWithLinks.contains(port)
      )
  }

  def allowedLinkTypes(
      physicalPlan: PhysicalPlan,
      matchedResults: Map[GlobalPortIdentity, CachedResult],
      allowPipelined: Boolean
  ): Map[PhysicalLink, Set[EdgeType]] = {
    requireOutputPortsOfPlan(physicalPlan, matchedResults)
    val blockingAndDependee = physicalPlan.getBlockingAndDependeeLinks
    physicalPlan.links.map { link =>
      val sourcePort = GlobalPortIdentity(link.fromOpId, link.fromPortId)
      val types = Set[EdgeType](Materialized, Unused) ++
        (if (allowPipelined && !blockingAndDependee.contains(link)) Set(Pipelined)
         else Set.empty) ++
        (if (matchedResults.contains(sourcePort)) Set(CacheRead) else Set.empty)
      link -> types
    }.toMap
  }

  /**
    * Builds the eager-reuse seed directly via [[SearchState.resolveInputRequirements]]: starts all
    * links `Unused`, sets required end-of-run outputs present in `matchedResults` in `reusedOutputs`,
    * and propagates input requirements backward from running operators so matched inputs become
    * `CacheRead`, required unmatched inputs take `defaultActiveType(link)` (falling back to
    * `Materialized`), and unreachable upstream links stay `Unused`.
    */
  def eagerSeed(
      physicalPlan: PhysicalPlan,
      matchedResults: Map[GlobalPortIdentity, CachedResult],
      requiredOutputs: Set[GlobalPortIdentity],
      allowPipelined: Boolean = true,
      defaultActiveType: PhysicalLink => EdgeType = _ => Materialized
  ): SearchState = {
    val initialLinkTypes = physicalPlan.links.map(_ -> (Unused: EdgeType)).toMap
    val initialReusedOutputs = requiredOutputs.filter(matchedResults.contains)
    SearchState(initialLinkTypes, initialReusedOutputs).resolveInputRequirements(
      physicalPlan,
      requiredOutputs,
      matchedResults,
      allowPipelined,
      defaultActiveType
    )
  }
}
