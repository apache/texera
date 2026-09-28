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

import org.apache.texera.amber.core.storage.VFSURIFactory
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
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
  * The run skeleton of one execution marks what the execution does with every operator, output
  * port and link of the workflow:
  *
  *  - an operator is skipped when it has output ports and a usable saved result stands in for
  *    every one of them this run needs ([[SkeletonGenerator]] says which ports a run needs);
  *  - a link into a skipped operator is skipped;
  *  - a link from an output port with a usable saved result into a retained operator is a
  *    cache-read link: the retained operator reads the saved result, even when the port's own
  *    operator runs;
  *  - an output port's saved result is read when a cache-read link starts at the port, or when
  *    the port is a required output of a skipped operator;
  *  - every other operator is retained: it runs in this execution. Every other link runs.
  *
  * The skeleton holds only these marks. What the scheduler needs (the part that runs, the input
  * ports that read saved results, and the skip regions) is derived from the plan the skeleton
  * was generated for.
  *
  * @param skippedOps the operators that do not run in this execution
  * @param skippedLinks the links into skipped operators, from skipped or retained ones; no data
  *                     crosses them in this execution
  * @param cacheReadLinks the links from an output port with a usable saved result into a
  *                       retained operator
  * @param cacheReadPorts the output ports whose saved results this execution reads, with the
  *                       saved location and row count: the source port of every cache-read
  *                       link, and every required output of a skipped operator
  * @param unusable the saved results this execution does not use, with the reason
  */
case class RunSkeleton(
    skippedOps: Set[PhysicalOpIdentity],
    skippedLinks: Set[PhysicalLink],
    cacheReadLinks: Set[PhysicalLink],
    cacheReadPorts: Map[GlobalPortIdentity, CachedResult],
    unusable: Map[GlobalPortIdentity, String]
) {

  def skipsAnything: Boolean = skippedOps.nonEmpty

  /**
    * The part of `physicalPlan` that runs: the operators that run, as the plan's own operator
    * objects, and the links between them except the cache-read links. This is what the
    * schedule generator plans. Links leave the plan's link list only: an operator's own link
    * list still names every link it has in the full plan, cache-read links and links to
    * skipped operators included.
    */
  def retainedPart(physicalPlan: PhysicalPlan): PhysicalPlan = {
    val retained = physicalPlan.getSubPlan(physicalPlan.operators.map(_.id).diff(skippedOps))
    retained.copy(links = retained.links.diff(cacheReadLinks))
  }

  /**
    * The input port of every cache-read link, with the saved location of the link's source
    * port. An input port with several cache-read links gets their locations in a fixed order
    * (sorted by link).
    */
  def cacheReadInputs: CacheReadInputs =
    CacheReadInputs(
      cacheReadLinks.toList
        .sortBy(_.toString)
        .foldLeft(Map.empty[GlobalPortIdentity, List[URI]]) { (acc, link) =>
          val inputPort = GlobalPortIdentity(link.toOpId, link.toPortId, input = true)
          val savedLocation =
            cacheReadPorts(GlobalPortIdentity(link.fromOpId, link.fromPortId)).storageUri
          acc.updated(inputPort, acc.getOrElse(inputPort, List.empty[URI]) :+ savedLocation)
        }
    )

  /**
    * The skipped operators of `physicalPlan` as skip regions, one per connected group, with
    * ids `firstRegionId`, `firstRegionId + 1`, ... in a deterministic order. A region holds
    * the plan's own operator objects, the skipped links inside its group, every port of its
    * operators, and in its resource config the group's ports whose saved results are read,
    * each with the saved location and row count. The caller passes an id above every id the
    * schedule generator used. All of them can sit in one schedule level: they start no
    * workers, so `max-concurrent-regions` does not apply to them.
    */
  def skipRegions(physicalPlan: PhysicalPlan, firstRegionId: Long): Set[Region] = {
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
            ResourceConfig(portConfigs = cacheReadPorts.collect {
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
  * Generates the run skeleton of an execution (see [[RunSkeleton]]) from the saved results
  * matched to its plan (`WorkflowContext.matchedResults`):
  *
  *  - The required outputs are the output ports the run must store
  *    (`outputPortsNeedingStorage`: the ports of operators whose results the user views, and
  *    of operators with no outgoing links) and every output port with no link.
  *  - An output port is needed when it is a required output or feeds an operator that runs.
  *  - An operator runs when it has no output ports, or when one of its needed output ports has
  *    no usable saved result. A retained operator computes all of its outputs.
  *  - Every other operator is skipped, and every link into a skipped operator is skipped.
  *  - Every link from a port with a usable saved result into a retained operator is a
  *    cache-read link, whether or not the port's own operator runs.
  *  - Every other link runs.
  *
  * Reuse is full: wherever a retained operator reads a port that has a usable saved result, it
  * reads the saved result, and there is no cost-based choice between reading a saved result and
  * computing it again. The one exception is what the user sees: when a port the run must store
  * belongs to an operator that runs, this run stores its own result for that port and shows it,
  * while every retained operator the port feeds still reads the saved copy.
  *
  * Two of the rules make sure the cache changes what runs only where a saved result stands in
  * for work that something needs. An output port with no link is a required output, so unless a
  * saved result covers it, its operator runs as it would without the cache. And an operator
  * with no output ports always runs: nothing in the workflow reads it, so it may be there for
  * what it does outside the workflow, which no saved result stands in for.
  *
  * A saved result is unusable, with the reason recorded, when its port is not an output port of
  * this plan, or its location is not this workflow's, this port's, in this run's warehouse. In
  * a plan with a loop every saved result is unusable, since such a plan reuses nothing. An
  * unusable result never fails the run; the run just doesn't reuse it.
  *
  * Nothing here starts an operator; the scheduler decides what to do with the skeleton.
  */
object SkeletonGenerator {

  /**
    * The run skeleton for `physicalPlan`, given the matched results on `workflowContext`. With
    * none, or none that is usable, or none that changes what the run does, every operator and
    * link runs and the schedule generator sees exactly what it sees without a cache.
    */
  def generate(workflowContext: WorkflowContext, physicalPlan: PhysicalPlan): RunSkeleton = {
    val entries = workflowContext.matchedResults
    if (entries.isEmpty) return noReuse(Map.empty)
    if (physicalPlan.operators.exists(op => op.requiresMaterializedExecution || op.isLoopStart)) {
      return noReuse(entries.keys.map(_ -> "the plan contains a loop").toMap)
    }
    val (usable, unusable) = checkEntries(workflowContext, physicalPlan, entries)
    if (usable.isEmpty) return noReuse(unusable)

    // An output port with no link is a required output, like the ports the run must store.
    val portsWithLinks = physicalPlan.links.map(sourcePort)
    val requiredOutputs = physicalPlan.operators
      .flatMap(op => op.outputPorts.keys.map(portId => GlobalPortIdentity(op.id, portId)))
      .filter(port =>
        workflowContext.workflowSettings.outputPortsNeedingStorage.contains(port) ||
          !portsWithLinks.contains(port)
      )
    val retainedOps = determineOperatorsToRun(physicalPlan, usable.keySet, requiredOutputs)
    val skippedOps = physicalPlan.operators.map(_.id).diff(retainedOps)

    // Every link from a port with a usable saved result into a retained operator reads the
    // saved result, even when the port's own operator runs. When such a port is also one the
    // run must store and its operator runs, this run still stores its own result for the port
    // and the user sees that one; only the operators the port feeds read the saved copy.
    val cacheReadLinks = physicalPlan.links.filter(link =>
      usable.contains(sourcePort(link)) && retainedOps.contains(link.toOpId)
    )
    if (skippedOps.isEmpty && cacheReadLinks.isEmpty) return noReuse(unusable)

    // A skipped operator has a usable saved result for each of its required outputs: they are
    // needed, so without one the operator would run.
    val readPorts = cacheReadLinks.map(sourcePort) ++
      requiredOutputs.filter(port => skippedOps.contains(port.opId))
    RunSkeleton(
      skippedOps = skippedOps,
      skippedLinks = physicalPlan.links.filter(link => skippedOps.contains(link.toOpId)),
      cacheReadLinks = cacheReadLinks,
      cacheReadPorts = readPorts.map(port => port -> usable(port)).toMap,
      unusable = unusable
    )
  }

  private def noReuse(unusable: Map[GlobalPortIdentity, String]): RunSkeleton =
    RunSkeleton(
      skippedOps = Set.empty,
      skippedLinks = Set.empty,
      cacheReadLinks = Set.empty,
      cacheReadPorts = Map.empty,
      unusable = unusable
    )

  private def sourcePort(link: PhysicalLink): GlobalPortIdentity =
    GlobalPortIdentity(link.fromOpId, link.fromPortId)

  /**
    * One pass in reverse topological order. Downstream operators are decided first, so
    * "feeds an operator that runs" is known when an operator is visited.
    */
  private def determineOperatorsToRun(
      physicalPlan: PhysicalPlan,
      usablePorts: Set[GlobalPortIdentity],
      requiredOutputs: Set[GlobalPortIdentity]
  ): Set[PhysicalOpIdentity] =
    physicalPlan
      .topologicalIterator()
      .toList
      .reverse
      .foldLeft(Set.empty[PhysicalOpIdentity]) { (retained, opId) =>
        val op = physicalPlan.getOperator(opId)
        val neededOutputs = op.outputPorts.keys
          .map(portId => GlobalPortIdentity(opId, portId))
          .filter(port =>
            requiredOutputs.contains(port) ||
              physicalPlan
                .getDownstreamPhysicalLinks(opId)
                .exists(link => link.fromPortId == port.portId && retained.contains(link.toOpId))
          )
        // An operator with no output ports always runs: it may be there for what it does
        // outside the workflow, which no saved result stands in for.
        if (op.outputPorts.isEmpty || neededOutputs.exists(port => !usablePorts.contains(port)))
          retained + opId
        else retained
      }

  private def checkEntries(
      workflowContext: WorkflowContext,
      physicalPlan: PhysicalPlan,
      entries: Map[GlobalPortIdentity, CachedResult]
  ): (Map[GlobalPortIdentity, CachedResult], Map[GlobalPortIdentity, String]) = {
    val checked = entries.map {
      case (port, cached) => port -> checkEntry(workflowContext, physicalPlan, port, cached)
    }
    (
      checked.collect { case (port, Right(cached)) => port -> cached },
      checked.collect { case (port, Left(reason)) => port -> reason }
    )
  }

  /**
    * A saved result is usable when its port is an output port of the plan and its location is
    * the port base URI the factory builds for that port, in this workflow and this run's
    * warehouse. Otherwise it is unusable, with the reason, and never an error: a stale or
    * foreign saved result must not fail a run.
    */
  private def checkEntry(
      workflowContext: WorkflowContext,
      physicalPlan: PhysicalPlan,
      port: GlobalPortIdentity,
      cached: CachedResult
  ): Either[String, CachedResult] = {
    val isOutputPortOfPlan = !port.input && physicalPlan.operators.exists(op =>
      op.id == port.opId && op.outputPorts.contains(port.portId)
    )
    if (!isOutputPortOfPlan) return Left("not an output port of this plan")
    Try(VFSURIFactory.decodeURI(VFSURIFactory.resultURI(cached.storageUri))) match {
      case Failure(e) => Left(s"not a port base URI (${e.getMessage})")
      case Success(decoded) =>
        val expectedBase = VFSURIFactory.createPortBaseURI(
          decoded.workflowId,
          decoded.executionId,
          port,
          decoded.warehouse
        )
        if (cached.storageUri != expectedBase) Left("not the base URI of this port")
        else if (decoded.workflowId != workflowContext.workflowId)
          Left("stored by another workflow")
        else if (decoded.warehouse != workflowContext.warehouse) Left("stored in another warehouse")
        else Right(cached)
    }
  }
}
