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

package org.apache.texera.amber.engine.architecture.coordinator

import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  CachedResult,
  ExecutionMode,
  GlobalPortIdentity,
  HashPartition,
  PhysicalLink,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  InputPortConfig,
  OutputPortConfig
}
import org.apache.texera.amber.engine.architecture.scheduling.{CostBasedScheduleGenerator, Region}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.RoundRobinPartitioning
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.split.SplitOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.operator.{LogicalOp, TestOperators}
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI

class WorkflowSchedulerSpec extends AnyFlatSpec {

  private def buildHeaderlessCsvKeywordWorkflow(
      context: WorkflowContext = new WorkflowContext()
  ) = {
    val csvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    buildWorkflow(
      List(csvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      context
    )
  }

  /** csv1 -> join (build, port 0), csv2 -> join (probe, port 1). */
  private def buildJoinWorkflow(context: WorkflowContext = new WorkflowContext()) = {
    val csv1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val csv2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val join = TestOperators.joinOpDesc("column-1", "column-1")
    buildWorkflow(
      List(csv1, csv2, join),
      List(
        LogicalLink(
          csv1.operatorIdentifier,
          PortIdentity(0),
          join.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          csv2.operatorIdentifier,
          PortIdentity(0),
          join.operatorIdentifier,
          PortIdentity(1)
        )
      ),
      context
    )
  }

  /**
    * csv -> split; split's port 0 -> keyword A and port 1 -> keyword B. With `withUnion`, A and
    * B both feed one union. Split's port 0 gets a saved result of execution 1; port 1 has none,
    * so Split runs for B. Returns the workflow, the link from split's port 0 to A, which reads
    * the saved result, and the saved location.
    */
  private def buildSplitWorkflowWithSavedPort0(
      context: WorkflowContext,
      withUnion: Boolean = false
  ): (Workflow, PhysicalLink, URI) = {
    val csv = TestOperators.headerlessSmallCsvScanOpDesc()
    val split = new SplitOpDesc()
    val keywordA = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val keywordB = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val union = new UnionOpDesc()
    def logicalLink(from: LogicalOp, fromPort: Int, to: LogicalOp) =
      LogicalLink(
        from.operatorIdentifier,
        PortIdentity(fromPort),
        to.operatorIdentifier,
        PortIdentity(0)
      )
    val splitLinks =
      List(
        logicalLink(csv, 0, split),
        logicalLink(split, 0, keywordA),
        logicalLink(split, 1, keywordB)
      )
    val unionLinks = List(logicalLink(keywordA, 0, union), logicalLink(keywordB, 0, union))
    val workflow =
      if (withUnion)
        buildWorkflow(
          List(csv, split, keywordA, keywordB, union),
          splitLinks ++ unionLinks,
          context
        )
      else buildWorkflow(List(csv, split, keywordA, keywordB), splitLinks, context)
    val splitId = workflow.physicalPlan.getPhysicalOpsOfLogicalOp(split.operatorIdentifier).head.id
    val splitOut0 = GlobalPortIdentity(splitId, PortIdentity(0))
    val savedUri = VFSURIFactory.createPortBaseURI(
      context.workflowId,
      ExecutionIdentity(1L),
      splitOut0,
      context.warehouse
    )
    context.matchedResults = Map(splitOut0 -> CachedResult(savedUri, Some(4L)))
    val splitToA = workflow.physicalPlan.links
      .find(link => link.fromOpId == splitId && link.fromPortId == PortIdentity(0))
      .get
    (workflow, splitToA, savedUri)
  }

  private def pipelinedContext() =
    new WorkflowContext(
      WorkflowIdentity(7L),
      ExecutionIdentity(2L),
      WorkflowSettings(executionMode = ExecutionMode.PIPELINED)
    )

  /** The locations an input port reads from storage, from the one region that configures it. */
  private def readerUris(regions: Iterable[Region], inputPort: GlobalPortIdentity): List[URI] = {
    val configs = regions.flatMap(_.resourceConfig.get.portConfigs.get(inputPort)).toList
    assert(configs.size == 1, s"expected one region to configure $inputPort, got $configs")
    configs.head match {
      case InputPortConfig(pairs) => pairs.map(_._1)
      case other                  => fail(s"expected a reader for $inputPort, got $other")
    }
  }

  "WorkflowScheduler.updateSchedule" should "populate the schedule and physicalPlan fields" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)

    assert(scheduler.getSchedule == null)
    assert(scheduler.physicalPlan == null)

    scheduler.updateSchedule(workflow.physicalPlan)

    assert(scheduler.getSchedule != null)
    assert(scheduler.physicalPlan != null)
    assert(scheduler.getSchedule.getRegions.nonEmpty)
  }

  it should "include every workflow operator in some region of the produced schedule" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)
    scheduler.updateSchedule(workflow.physicalPlan)

    val operatorsInSchedule = scheduler.getSchedule.getRegions
      .flatMap(_.getOperators.map(_.id.logicalOpId))
      .toSet
    val operatorsInPlan = scheduler.physicalPlan.operators.map(_.id.logicalOpId)

    assert(operatorsInPlan.subsetOf(operatorsInSchedule))
  }

  "WorkflowScheduler.getNextRegions" should "exhaust the schedule and then return an empty set" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)
    scheduler.updateSchedule(workflow.physicalPlan)

    val pulledLevels = Iterator
      .continually(scheduler.getNextRegions)
      .takeWhile(_.nonEmpty)
      .toList

    assert(pulledLevels.nonEmpty)
    assert(scheduler.getNextRegions.isEmpty)
  }

  it should "yield region sets that together cover every region in the schedule" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)
    scheduler.updateSchedule(workflow.physicalPlan)

    val expectedRegions = scheduler.getSchedule.getRegions.toSet
    val pulledRegions = Iterator
      .continually(scheduler.getNextRegions)
      .takeWhile(_.nonEmpty)
      .flatten
      .toSet

    assert(pulledRegions == expectedRegions)
  }

  // ---------------------------------------------------------------------------
  // Matched results on the context
  // ---------------------------------------------------------------------------

  /** A region without its id, which the generator does not assign deterministically. */
  private def shape(regions: Set[Region]) =
    regions.map(r =>
      (r.physicalOps.map(_.id), r.physicalLinks, r.resourceConfig.map(_.portConfigs), r.skipped)
    )

  private def directLevels(workflow: Workflow) =
    new CostBasedScheduleGenerator(workflow.context, workflow.physicalPlan, COORDINATOR)
      .generate()
      ._1
      .levelSets

  private def assertSameLevelsAsDirectRun(workflow: Workflow): Unit = {
    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)
    scheduler.updateSchedule(workflow.physicalPlan)
    val expected = directLevels(workflow)
    val actual = scheduler.getSchedule.levelSets
    assert(actual.keySet == expected.keySet)
    actual.foreach { case (level, regions) => assert(shape(regions) == shape(expected(level))) }
    assert(actual.values.flatten.forall(!_.skipped))
    assert(scheduler.physicalPlan eq workflow.physicalPlan)
  }

  private def outputPortOf(
      workflow: Workflow,
      logicalOpId: org.apache.texera.amber.core.virtualidentity.OperatorIdentity
  ) = {
    val op = workflow.physicalPlan.operators.find(_.id.logicalOpId == logicalOpId).get
    GlobalPortIdentity(op.id, PortIdentity(0))
  }

  "WorkflowScheduler.updateSchedule" should "produce the same levels as a direct generator run when the cache is empty" in {
    assertSameLevelsAsDirectRun(buildHeaderlessCsvKeywordWorkflow())
    assertSameLevelsAsDirectRun(buildJoinWorkflow())
  }

  it should "produce the same levels as a direct generator run when every saved result is unusable" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val keywordOut = outputPortOf(workflow, workflow.logicalPlan.getTerminalOperatorIds.head)
    // a warehouse this run does not write into
    workflow.context.matchedResults = Map(
      keywordOut -> CachedResult(
        VFSURIFactory.createPortBaseURI(
          workflow.context.workflowId,
          ExecutionIdentity(1L),
          keywordOut,
          Some("theirs")
        ),
        Some(1L)
      )
    )
    assertSameLevelsAsDirectRun(workflow)
  }

  it should "keep cuid and warehouse, and put skip regions first, on a run with a match" in {
    val context = new WorkflowContext(
      workflowId = WorkflowIdentity(7L),
      executionId = ExecutionIdentity(2L),
      cuid = Some(7),
      warehouse = Some("wh1")
    )
    val workflow = buildHeaderlessCsvKeywordWorkflow(context)
    val csvOp = workflow.physicalPlan.operators.find(_.isSourceOperator).get
    val keywordOp = workflow.physicalPlan.operators.find(!_.isSourceOperator).get
    val csvOut = GlobalPortIdentity(csvOp.id, PortIdentity(0))
    val cachedUri =
      VFSURIFactory.createPortBaseURI(
        WorkflowIdentity(7L),
        ExecutionIdentity(1L),
        csvOut,
        Some("wh1")
      )
    context.matchedResults = Map(csvOut -> CachedResult(cachedUri, Some(5L)))

    val scheduler = new WorkflowScheduler(context, COORDINATOR)
    scheduler.updateSchedule(workflow.physicalPlan)
    val levels = scheduler.getSchedule.levelSets
    assert(levels.keySet == Set(0, 1))

    // level 0: the skipped csv operator, with its stored output
    val skipRegions = levels(0)
    assert(skipRegions.size == 1)
    val skipRegion = skipRegions.head
    assert(skipRegion.skipped)
    assert(skipRegion.physicalOps.map(_.id) == Set(csvOp.id))
    assert(
      skipRegion.resourceConfig.get.portConfigs == Map(
        csvOut -> OutputPortConfig(cachedUri, Some(5L))
      )
    )

    // level 1: the keyword operator, planned with the original context
    val generated = levels(1)
    assert(generated.size == 1)
    val region = generated.head
    assert(!region.skipped)
    assert(region.physicalOps.map(_.id) == Set(keywordOp.id))
    val config = region.resourceConfig.get
    assert(config.operatorConfigs.values.flatMap(_.workerConfigs).forall(_.cuid.contains(7)))
    config.portConfigs(GlobalPortIdentity(keywordOp.id, PortIdentity(0), input = true)) match {
      case InputPortConfig(pairs) => assert(pairs.map(_._1) == List(cachedUri))
      case other                  => fail(s"expected a reader for the saved csv output, got $other")
    }
    config.portConfigs(GlobalPortIdentity(keywordOp.id, PortIdentity(0))) match {
      case OutputPortConfig(uri, count) =>
        assert(uri.toString.contains("/wh/wh1/"))
        assert(uri.toString.contains("/eid/2/"))
        assert(count.isEmpty)
      case other => fail(s"expected a new output URI, got $other")
    }

    // skip region ids sit above the generated ids; the full plan stays on the scheduler
    assert(skipRegion.id.id > generated.map(_.id.id).max)
    assert(scheduler.physicalPlan eq workflow.physicalPlan)
  }

  it should "give a run that skips every operator exactly one level of skip regions" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val keywordOut = outputPortOf(workflow, workflow.logicalPlan.getTerminalOperatorIds.head)
    val cachedUri = VFSURIFactory.createPortBaseURI(
      workflow.context.workflowId,
      ExecutionIdentity(1L),
      keywordOut
    )
    workflow.context.matchedResults = Map(keywordOut -> CachedResult(cachedUri, None))

    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)
    scheduler.updateSchedule(workflow.physicalPlan)
    val levels = scheduler.getSchedule.levelSets
    assert(levels.keySet == Set(0))
    val regions = levels(0)
    assert(regions.forall(_.skipped))
    assert(regions.flatMap(_.physicalOps.map(_.id)) == workflow.physicalPlan.operators.map(_.id))
    assert(
      regions.flatMap(_.resourceConfig.get.portConfigs).toMap ==
        Map(keywordOut -> OutputPortConfig(cachedUri, None))
    )
    assert(scheduler.hasPendingRegions)
    assert(scheduler.getNextRegions == regions)
    assert(!scheduler.hasPendingRegions)
  }

  it should "read the build side's input from the cache" in {
    // The saved result is the output of the scan that feeds the build operator: the scan is
    // skipped, and the build operator runs and reads its input from storage.
    val workflow = buildJoinWorkflow()
    val plan = workflow.physicalPlan
    val buildLink = plan.links.find(_.toPortId == PortIdentity(0)).get
    val buildSourceOut = GlobalPortIdentity(buildLink.fromOpId, buildLink.fromPortId)
    val cachedUri = VFSURIFactory.createPortBaseURI(
      workflow.context.workflowId,
      ExecutionIdentity(1L),
      buildSourceOut
    )
    workflow.context.matchedResults = Map(buildSourceOut -> CachedResult(cachedUri, Some(9L)))

    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)
    scheduler.updateSchedule(plan)
    val levels = scheduler.getSchedule.levelSets
    assert(levels.size >= 2)
    val skipRegions = levels(0)
    assert(skipRegions.forall(_.skipped))
    assert(skipRegions.flatMap(_.physicalOps.map(_.id)) == Set(buildLink.fromOpId))
    val generated = (1 until levels.size).flatMap(levels).toSet
    assert(generated.forall(!_.skipped))
    assert(
      generated.flatMap(_.physicalOps.map(_.id)) == plan.operators.map(_.id) - buildLink.fromOpId
    )
    val buildInput = GlobalPortIdentity(buildLink.toOpId, buildLink.toPortId, input = true)
    val readers = generated.flatMap(_.resourceConfig.get.portConfigs.get(buildInput))
    assert(readers.size == 1)
    readers.head match {
      case InputPortConfig(pairs) => assert(pairs.map(_._1) == List(cachedUri))
      case other                  => fail(s"expected a reader for the saved scan output, got $other")
    }
    assert(skipRegions.map(_.id.id).min > generated.map(_.id.id).max)
  }

  it should "skip the scan and the build operator when the build operator's output is saved" in {
    // The build operator sends the rows of its hash table to the probe through an internal
    // output port. That link is blocking, so every run stores the port. With its saved
    // result, neither the build operator nor the scan feeding it runs, and the probe reads
    // its build input from storage while the other scan still sends its rows to the probe.
    val workflow = buildJoinWorkflow()
    val plan = workflow.physicalPlan
    val buildToProbe = plan.links.find(_.fromPortId == PortIdentity(0, internal = true)).get
    val buildOpId = buildToProbe.fromOpId
    val buildScanId = plan.getUpstreamPhysicalOpIds(buildOpId).head
    val buildOut = GlobalPortIdentity(buildOpId, buildToProbe.fromPortId)
    val otherScanToProbe =
      plan.links.find(link => link.toOpId == buildToProbe.toOpId && link != buildToProbe).get
    val cachedUri = VFSURIFactory.createPortBaseURI(
      workflow.context.workflowId,
      ExecutionIdentity(1L),
      buildOut
    )
    workflow.context.matchedResults = Map(buildOut -> CachedResult(cachedUri, Some(9L)))

    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)
    scheduler.updateSchedule(plan)
    val levels = scheduler.getSchedule.levelSets
    assert(levels.keySet == Set(0, 1))

    // level 0: the scan and the build operator in one skip region, with the build output's
    // saved result
    val skipRegions = levels(0)
    assert(skipRegions.size == 1)
    val skipRegion = skipRegions.head
    assert(skipRegion.skipped)
    assert(skipRegion.physicalOps.map(_.id) == Set(buildScanId, buildOpId))
    assert(
      skipRegion.resourceConfig.get.portConfigs == Map(
        buildOut -> OutputPortConfig(cachedUri, Some(9L))
      )
    )

    // level 1: the probe and the scan that feeds it, with the probe's build input read from
    // the saved result
    val generated = levels(1)
    assert(generated.size == 1)
    val region = generated.head
    assert(!region.skipped)
    assert(region.physicalOps.map(_.id) == plan.operators.map(_.id) - buildScanId - buildOpId)
    assert(region.physicalLinks == Set(otherScanToProbe))
    val probeBuildInput =
      GlobalPortIdentity(buildToProbe.toOpId, buildToProbe.toPortId, input = true)
    region.resourceConfig.get.portConfigs(probeBuildInput) match {
      case InputPortConfig(pairs) => assert(pairs.map(_._1) == List(cachedUri))
      case other                  => fail(s"expected a reader for the saved build output, got $other")
    }

    // skip region ids sit above the generated ids; the full plan stays on the scheduler
    assert(skipRegion.id.id > region.id.id)
    assert(scheduler.physicalPlan eq plan)
  }

  it should "hold the plan's own operator objects in every region" in {
    // The scan and the build operator are skipped, and the probe reads the build output from
    // storage. Neither the probe nor the build operator is a copy with their shared link
    // removed: the regions hold the plan's own operators, whose link lists still name it.
    val workflow = buildJoinWorkflow()
    val plan = workflow.physicalPlan
    val buildToProbe = plan.links.find(_.fromPortId == PortIdentity(0, internal = true)).get
    val buildOut = GlobalPortIdentity(buildToProbe.fromOpId, buildToProbe.fromPortId)
    val cachedUri = VFSURIFactory.createPortBaseURI(
      workflow.context.workflowId,
      ExecutionIdentity(1L),
      buildOut
    )
    workflow.context.matchedResults = Map(buildOut -> CachedResult(cachedUri, Some(9L)))

    val scheduler = new WorkflowScheduler(workflow.context, COORDINATOR)
    scheduler.updateSchedule(plan)
    val regions = scheduler.getSchedule.getRegions
    assert(regions.exists(_.skipped) && regions.exists(!_.skipped))
    assert(regions.flatMap(_.getOperators).map(_.id).toSet == plan.operators.map(_.id))
    regions.flatMap(_.getOperators).foreach(op => assert(op eq plan.getOperator(op.id)))
    // the link between them is in no region, but both operators still list it
    assert(regions.forall(!_.getLinks.contains(buildToProbe)))
    val probeRegion = regions.find(_.getOperators.exists(_.id == buildToProbe.toOpId)).get
    assert(probeRegion.getOperator(buildToProbe.toOpId).getInputLinks().contains(buildToProbe))
    val buildRegion = regions.find(_.getOperators.exists(_.id == buildToProbe.fromOpId)).get
    assert(buildRegion.skipped)
    assert(
      buildRegion
        .getOperator(buildToProbe.fromOpId)
        .getOutputLinks(buildToProbe.fromPortId)
        .contains(buildToProbe)
    )
  }

  it should "read a saved port from storage even when its operator runs" in {
    // Split runs for B, whose input has no saved result, and A reads split's port 0 from its
    // saved result instead of waiting for Split. Nothing is skipped, so there is no skip level.
    val context = pipelinedContext()
    val (workflow, splitToA, savedUri) = buildSplitWorkflowWithSavedPort0(context)
    val plan = workflow.physicalPlan
    val scheduler = new WorkflowScheduler(context, COORDINATOR)
    scheduler.updateSchedule(plan)
    val regions = scheduler.getSchedule.getRegions
    assert(regions.forall(!_.skipped))
    assert(scheduler.getSchedule.levelSets.values.forall(_.nonEmpty))
    assert(regions.flatMap(_.getOperators).map(_.id).toSet == plan.operators.map(_.id))
    // the link from split's port 0 to A carries nothing: no region holds it, and A reads only
    // the saved location
    assert(regions.forall(!_.getLinks.contains(splitToA)))
    val aInput = GlobalPortIdentity(splitToA.toOpId, splitToA.toPortId, input = true)
    assert(readerUris(regions, aInput) == List(savedUri))
    // split's port 1 has no saved result, so B reads it over its link, as without a cache
    val splitToB = plan.links
      .find(link => link.fromOpId == splitToA.fromOpId && link.fromPortId == PortIdentity(1))
      .get
    assert(regions.exists(_.getLinks.contains(splitToB)))
    val bInput = GlobalPortIdentity(splitToB.toOpId, splitToB.toPortId, input = true)
    assert(regions.forall(!_.resourceConfig.get.portConfigs.contains(bInput)))
    assert(scheduler.physicalPlan eq plan)
  }

  it should "store a port the run must store from this run while its reader uses the saved copy" in {
    // The user views Split's results, so the run must store both of Split's ports. Split runs
    // for B, so this run stores its own port 0 result, which the user sees; A still reads the
    // saved copy of port 0.
    val context = pipelinedContext()
    val (workflow, splitToA, savedUri) = buildSplitWorkflowWithSavedPort0(context)
    val splitOut0 = GlobalPortIdentity(splitToA.fromOpId, splitToA.fromPortId)
    context.workflowSettings = context.workflowSettings.copy(
      outputPortsNeedingStorage = context.workflowSettings.outputPortsNeedingStorage ++
        Set(splitOut0, GlobalPortIdentity(splitToA.fromOpId, PortIdentity(1)))
    )
    val scheduler = new WorkflowScheduler(context, COORDINATOR)
    scheduler.updateSchedule(workflow.physicalPlan)
    val regions = scheduler.getSchedule.getRegions
    val splitRegion = regions.find(_.getOperators.exists(_.id == splitOut0.opId)).get
    splitRegion.resourceConfig.get.portConfigs(splitOut0) match {
      case OutputPortConfig(uri, count) =>
        assert(
          uri == VFSURIFactory.createPortBaseURI(
            WorkflowIdentity(7L),
            ExecutionIdentity(2L),
            splitOut0,
            None
          )
        )
        assert(uri != savedUri)
        assert(count.isEmpty)
      case other => fail(s"expected this run's location for split's port 0, got $other")
    }
    val aInput = GlobalPortIdentity(splitToA.toOpId, splitToA.toPortId, input = true)
    assert(readerUris(regions, aInput) == List(savedUri))
  }

  it should "read a saved result inside the region of the port's retained operator" in {
    // A and B both feed a union, so in pipelined mode every operator shares one region, Split
    // and A included. The link from split's port 0 to A is not one of the region's links, yet
    // A's own link list still names it: A has no input link in the region, so it counts as one
    // of the region's sources, and it starts first because its input port reads the saved
    // result.
    val context = pipelinedContext()
    val (workflow, splitToA, savedUri) = buildSplitWorkflowWithSavedPort0(context, withUnion = true)
    // A (a keyword search) gets a hash partition requirement on its input, which it does not
    // have by itself, so that the partitioning of its output link shows how A is treated.
    val plan = workflow.physicalPlan.setOperator(
      workflow.physicalPlan
        .getOperator(splitToA.toOpId)
        .withPartitionRequirement(List(Some(HashPartition(List("column-1")))))
    )
    val scheduler = new WorkflowScheduler(context, COORDINATOR)
    scheduler.updateSchedule(plan)
    val regions = scheduler.getSchedule.getRegions
    assert(regions.size == 1)
    val region = regions.head
    assert(region.getOperators.map(_.id) == plan.operators.map(_.id))
    assert(!region.getLinks.contains(splitToA))
    assert(region.getLinks == plan.links - splitToA)
    val a = region.getOperator(splitToA.toOpId)
    assert(a.getInputLinks().contains(splitToA))
    assert(region.getSourceOperators.map(_.id).contains(a.id))
    assert(region.getStarterOperators.map(_.id).contains(a.id))
    val aInput = GlobalPortIdentity(a.id, splitToA.toPortId, input = true)
    assert(readerUris(Seq(region), aInput) == List(savedUri))
    // B reads split's port 1 over its link in the region: neither a source nor a starter
    val splitToB = plan.links
      .find(link => link.fromOpId == splitToA.fromOpId && link.fromPortId == PortIdentity(1))
      .get
    assert(!region.getSourceOperators.map(_.id).contains(splitToB.toOpId))
    assert(!region.getStarterOperators.map(_.id).contains(splitToB.toOpId))
    // A reads a saved result, so its output link to the union is partitioned as behind a
    // materialized link: round robin, since A's output partitioning is unknown. Taken for a
    // source, or partitioned from the link it no longer reads, A would pass the hash
    // partitioning its input requires on to the union.
    val aToUnion = plan.links.find(_.fromOpId == a.id).get
    assert(
      region.resourceConfig.get
        .linkConfigs(aToUnion)
        .partitioning
        .isInstanceOf[RoundRobinPartitioning]
    )
  }
}
