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

import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
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
import org.apache.texera.workflow.LogicalLink
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
            org.apache.texera.amber.config.ApplicationConfig.numWorkerPerOperatorByDefault
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
}
