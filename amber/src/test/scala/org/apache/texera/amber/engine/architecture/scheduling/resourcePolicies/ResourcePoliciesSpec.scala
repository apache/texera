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

package org.apache.texera.amber.engine.architecture.scheduling.resourcePolicies

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  HashPartition,
  InputPort,
  OutputPort,
  PartitionInfo,
  PhysicalLink,
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  IntermediateInputPortConfig,
  ResourceConfig
}
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  Partitioning,
  RangeBasedShufflePartitioning,
  RoundRobinPartitioning
}
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.flatspec.AnyFlatSpec

class ResourcePoliciesSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // ExecutionClusterInfo
  // ---------------------------------------------------------------------------

  "ExecutionClusterInfo" should "construct without arguments" in {
    // No-arg constructor must not throw; the type currently has no observable
    // state to assert beyond that.
    new ExecutionClusterInfo()
  }

  // ---------------------------------------------------------------------------
  // DefaultResourceAllocator (helpers + tests)
  // ---------------------------------------------------------------------------

  /** Build a small linear `csv -> keyword` workflow to feed the allocator. */
  private def buildLinearWorkflow() = {
    val csv = TestOperators.headerlessSmallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    buildWorkflow(
      List(csv, keyword),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          keyword.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )
  }

  private def newAllocator(): (DefaultResourceAllocator, Region) = {
    val workflow = buildLinearWorkflow()
    val allocator = new DefaultResourceAllocator(
      workflow.physicalPlan,
      new ExecutionClusterInfo(),
      workflow.context.workflowSettings
    )
    val region = Region(
      id = RegionIdentity(0),
      physicalOps = workflow.physicalPlan.operators,
      physicalLinks = workflow.physicalPlan.links
    )
    (allocator, region)
  }

  "DefaultResourceAllocator.allocate" should "return zero cost (placeholder)" in {
    val (allocator, region) = newAllocator()
    val (_, cost) = allocator.allocate(region)
    assert(cost == 0d)
  }

  it should "produce an OperatorConfig entry for every operator in the region" in {
    val (allocator, region) = newAllocator()
    val (resourceConfig, _) = allocator.allocate(region)
    val opIds = region.getOperators.map(_.id)
    assert(resourceConfig.operatorConfigs.keySet == opIds)
  }

  it should "respect parallelizable / suggested-worker settings on each PhysicalOp" in {
    val (allocator, region) = newAllocator()
    val (resourceConfig, _) = allocator.allocate(region)
    region.getOperators.foreach { op =>
      val workers = resourceConfig.operatorConfigs(op.id).workerConfigs.size
      val expected =
        if (!op.parallelizable) 1
        else
          op.suggestedWorkerNum.getOrElse(
            org.apache.texera.common.config.ApplicationConfig.numWorkerPerOperatorByDefault
          )
      assert(workers == expected, s"unexpected worker count for ${op.id}")
    }
  }

  it should "honor an explicit suggestedWorkerNum on a parallelizable op" in {
    val workflow = buildLinearWorkflow()
    val keywordPhysicalOpId =
      workflow.physicalPlan.operators.find(_.parallelizable).map(_.id).get
    val rebuiltOps = workflow.physicalPlan.operators.map { op =>
      if (op.id == keywordPhysicalOpId) op.withSuggestedWorkerNum(7) else op
    }
    val rebuiltPlan = workflow.physicalPlan.copy(operators = rebuiltOps)
    val allocator = new DefaultResourceAllocator(
      rebuiltPlan,
      new ExecutionClusterInfo(),
      workflow.context.workflowSettings
    )
    val region = Region(
      id = RegionIdentity(0),
      physicalOps = rebuiltOps,
      physicalLinks = rebuiltPlan.links
    )
    val (resourceConfig, _) = allocator.allocate(region)
    assert(resourceConfig.operatorConfigs(keywordPhysicalOpId).workerConfigs.size == 7)
  }

  it should "emit distinct worker ids per operator" in {
    val (allocator, region) = newAllocator()
    val (resourceConfig, _) = allocator.allocate(region)
    val ids = resourceConfig.operatorConfigs.values.flatMap(_.workerConfigs.map(_.workerId)).toList
    assert(ids.distinct.size == ids.size, s"duplicate worker ids in $ids")
  }

  it should "produce a LinkConfig entry for every physical link in the region" in {
    val (allocator, region) = newAllocator()
    val (resourceConfig, _) = allocator.allocate(region)
    assert(resourceConfig.linkConfigs.keySet == region.getLinks)
  }

  it should "wire each LinkConfig so its Partitioning channels match its channelConfigs" in {
    val (allocator, region) = newAllocator()
    val (resourceConfig, _) = allocator.allocate(region)
    resourceConfig.linkConfigs.values.foreach { link =>
      assert(link.channelConfigs.nonEmpty)
      val partitioningChannels = partitioningOf(link.partitioning)
      assert(partitioningChannels == link.channelConfigs.map(_.channelId))
    }
  }

  private def partitioningOf(p: Partitioning) =
    p match {
      case x: OneToOnePartitioning          => x.channels
      case x: RoundRobinPartitioning        => x.channels
      case x: HashBasedShufflePartitioning  => x.channels
      case x: RangeBasedShufflePartitioning => x.channels
      case x: BroadcastPartitioning         => x.channels
      case other                            => fail(s"allocator emitted unexpected Partitioning: $other")
    }

  it should "leave portConfigs empty when the region has no prior resourceConfig" in {
    val (allocator, region) = newAllocator()
    val (resourceConfig, _) = allocator.allocate(region)
    assert(resourceConfig.portConfigs.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Operators that read saved results
  // ---------------------------------------------------------------------------

  /**
    * y requires a hash partition on its input; z requires nothing. In plan A, y reads x over
    * a materialized link that is outside the region. In plan B, y reads the same data from a
    * saved result: x and the x -> y link are not in the plan, though y's own link list still
    * names x -> y, as in the part of a plan that runs. The y -> z link must get the same
    * partitioning in both, so the part of a plan that runs is planned as it would be in the
    * whole plan.
    */
  "DefaultResourceAllocator" should "treat an operator that reads a saved result like one that reads a materialized link" in {
    val wid = WorkflowIdentity(1L)
    val eid = ExecutionIdentity(1L)
    def id(name: String) = PhysicalOpIdentity(OperatorIdentity(name), "main")
    def parallelOp(name: String, requirement: Option[PartitionInfo]): PhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(id(name), wid, eid, OpExecInitInfo.Empty)
        .withInputPorts(List(InputPort(PortIdentity(0))))
        .withOutputPorts(List(OutputPort(PortIdentity(0))))
        .withParallelizable(true)
        .withSuggestedWorkerNum(2)
        .withPartitionRequirement(List(requirement))
    val xy = PhysicalLink(id("x"), PortIdentity(0), id("y"), PortIdentity(0))
    val yz = PhysicalLink(id("y"), PortIdentity(0), id("z"), PortIdentity(0))
    val x = parallelOp("x", None).addOutputLink(xy)
    val y = parallelOp("y", Some(HashPartition(List("k")))).addInputLink(xy).addOutputLink(yz)
    val z = parallelOp("z", None).addInputLink(yz)
    val planA = PhysicalPlan(Set(x, y, z), Set(xy, yz))
    val planB = PhysicalPlan(Set(y, z), Set(yz))
    val readerUri =
      VFSURIFactory.createPortBaseURI(wid, eid, GlobalPortIdentity(id("x"), PortIdentity(0)))
    val yInput = GlobalPortIdentity(id("y"), PortIdentity(0), input = true)
    val region =
      Region(
        RegionIdentity(0),
        Set(y, z),
        Set(yz),
        resourceConfig = Some(
          ResourceConfig(portConfigs = Map(yInput -> IntermediateInputPortConfig(List(readerUri))))
        )
      )
    def partitioningOfYZ(allocator: DefaultResourceAllocator) =
      allocator.allocate(region)._1.linkConfigs(yz).partitioning.getClass

    val fromMaterializedLink = partitioningOfYZ(
      new DefaultResourceAllocator(planA, new ExecutionClusterInfo(), WorkflowSettings())
    )
    val fromStorage = partitioningOfYZ(
      new DefaultResourceAllocator(
        planB,
        new ExecutionClusterInfo(),
        WorkflowSettings(),
        operatorsReadingFromCache = Set(id("y"))
      )
    )
    val asSource = partitioningOfYZ(
      new DefaultResourceAllocator(planB, new ExecutionClusterInfo(), WorkflowSettings())
    )
    assert(fromMaterializedLink == classOf[RoundRobinPartitioning])
    assert(fromStorage == fromMaterializedLink)
    // Without y in operatorsReadingFromCache, y counts as a source and its output claims the
    // hash partitioning of its input; z would then get a hash shuffle main never gives it.
    assert(asSource == classOf[HashBasedShufflePartitioning])
  }
}
