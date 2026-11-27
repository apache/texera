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

import org.apache.texera.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import org.apache.texera.amber.core.storage.result.ResultSchema
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.aggregate.{AggregateOpDesc, AggregationFunction}
import org.apache.texera.amber.operator.keywordSearch.KeywordSearchOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.common.compiler.model.LogicalLink
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.web.ExecutionDatabaseTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterEach

import java.net.URI
import java.sql.Timestamp
import scala.jdk.CollectionConverters.CollectionHasAsScala

class DefaultCostEstimatorSpec
    extends AnyFlatSpec
    with BeforeAndAfterEach
    with MockTexeraDB
    with ExecutionDatabaseTestSupport {

  private val headerlessCsvOpDesc: CSVScanSourceOpDesc =
    TestOperators.headerlessSmallCsvScanOpDesc()
  private val keywordOpDesc: KeywordSearchOpDesc =
    TestOperators.keywordSearchOpDesc("column-1", "Asia")
  private val groupByOpDesc: AggregateOpDesc =
    TestOperators.aggregateAndGroupByDesc("column-1", AggregationFunction.COUNT, List[String]())

  private var uri: URI = _
  private var writer: BufferedItemWriter[Tuple] = _
  private var document: VirtualDocument[_] = _

  private def seedExecutionRecord(): Unit = {
    insertUser(1, "test_user", "test_user@test.com", role = Some(UserRoleEnum.ADMIN))
    insertWorkflow(
      1,
      "test workflow",
      content = "test workflow content",
      description = "test description",
      withTimestamps = false
    )
    insertWorkflowVersion(
      1,
      vidOpt = Some(1),
      content = "test version content",
      withTimestamp = false
    )
    insertWorkflowExecution(
      vid = 1,
      uid = 1,
      eidOpt = Some(1),
      status = 3.toByte,
      name = "",
      envVersion = "test engine",
      result = None,
      runtimeStatsUri = Some(uri.toString),
      bookmarked = None,
      startOffsetMillis = None
    )
  }

  override protected def beforeEach(): Unit = {
    initializeDBAndReplaceDSLContext()
    uri = VFSURIFactory.createRuntimeStatisticsURI(
      WorkflowIdentity(1L),
      ExecutionIdentity(1L)
    )
    document = DocumentFactory.createDocument(uri, ResultSchema.runtimeStatisticsSchema)
    writer = document
      .writer("runtime_statistics_1")
      .asInstanceOf[BufferedItemWriter[Tuple]]
    writer.open()
  }

  override protected def afterEach(): Unit = {
    document.clear()
    closeConnectionPool()
  }

  "DefaultCostEstimator" should "use fallback method when no past statistics are available" in {
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )
    val resourceAllocator =
      new DefaultResourceAllocator(
        workflow.physicalPlan,
        new ExecutionClusterInfo(),
        workflow.context.workflowSettings
      )

    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      resourceAllocator,
      COORDINATOR
    )
    val ports = workflow.physicalPlan.operators.flatMap(op =>
      op.inputPorts.keys
        .map(inputPortId => GlobalPortIdentity(op.id, inputPortId, input = true))
        .toSet ++ op.outputPorts.keys
        .map(outputPortId => GlobalPortIdentity(op.id, outputPortId))
        .toSet
    )

    val region = Region(
      id = RegionIdentity(0),
      physicalOps = workflow.physicalPlan.operators,
      physicalLinks = workflow.physicalPlan.links,
      ports = ports
    )

    val (_, costOfRegion) = costEstimator.allocateResourcesAndEstimateCost(region, 1)

    assert(costOfRegion == 0)
  }

  "DefaultCostEstimator" should "use the latest successful execution to estimate cost when available" in {
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

    seedExecutionRecord()

    val headerlessCsvOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        headerlessCsvOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        100L,
        100L,
        0L,
        1,
        0
      )
    )
    val keywordOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        keywordOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        300L,
        300L,
        0L,
        1,
        0
      )
    )

    writer.putOne(headerlessCsvOpRuntimeStatistics)
    writer.putOne(keywordOpRuntimeStatistics)
    writer.close()

    val resourceAllocator =
      new DefaultResourceAllocator(
        workflow.physicalPlan,
        new ExecutionClusterInfo(),
        workflow.context.workflowSettings
      )

    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      resourceAllocator,
      COORDINATOR
    )

    val ports = workflow.physicalPlan.operators.flatMap(op =>
      op.inputPorts.keys
        .map(inputPortId => GlobalPortIdentity(op.id, inputPortId, input = true))
        .toSet ++ op.outputPorts.keys
        .map(outputPortId => GlobalPortIdentity(op.id, outputPortId))
        .toSet
    )

    val region = Region(
      id = RegionIdentity(0),
      physicalOps = workflow.physicalPlan.operators,
      physicalLinks = workflow.physicalPlan.links,
      ports = ports
    )

    val (_, costOfRegion) = costEstimator.allocateResourcesAndEstimateCost(region, 1)

    assert(costOfRegion != 0)
  }

  "DefaultCostEstimator" should "use correctly estimate costs in a search" in {
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, groupByOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          groupByOpDesc.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          groupByOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

    seedExecutionRecord()

    val headerlessCsvOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        headerlessCsvOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        100L,
        100L,
        0L,
        1,
        0
      )
    )
    val groupByOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        groupByOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        1000L,
        1000L,
        0L,
        1,
        0
      )
    )
    val keywordOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        keywordOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        300L,
        300L,
        0L,
        1,
        0
      )
    )

    writer.putOne(headerlessCsvOpRuntimeStatistics)
    writer.putOne(groupByOpRuntimeStatistics)
    writer.putOne(keywordOpRuntimeStatistics)
    writer.close()

    // Should contain two regions, one with CSV->localAgg->globalAgg, another with keyword
    val searchResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).bottomUpSearch()

    val groupByRegion =
      searchResult.regionDAG.vertexSet().asScala.filter(region => region.physicalOps.size == 3).head
    val keywordRegion =
      searchResult.regionDAG.vertexSet().asScala.filter(region => region.physicalOps.size == 1).head

    val resourceAllocator =
      new DefaultResourceAllocator(
        workflow.physicalPlan,
        new ExecutionClusterInfo(),
        workflow.context.workflowSettings
      )

    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      resourceAllocator,
      COORDINATOR
    )

    val (_, groupByRegionCost) = costEstimator.allocateResourcesAndEstimateCost(groupByRegion, 1)

    val groupByOperatorCost = (groupByOpRuntimeStatistics.getField(6).asInstanceOf[Long] +
      groupByOpRuntimeStatistics.getField(7).asInstanceOf[Long]) / 1e9

    // The cost of the first region should be the cost of the GroupBy operator (note the two physical operators for
    // the GroupBy logical operator have the same cost because we use logical operator in the statistics.
    // The GroupBy operator has a longer running time.
    assert(groupByRegionCost == groupByOperatorCost)

    val (_, keywordRegionCost) = costEstimator.allocateResourcesAndEstimateCost(keywordRegion, 1)

    val keywordOperatorCost = (keywordOpRuntimeStatistics.getField(6).asInstanceOf[Long] +
      keywordOpRuntimeStatistics.getField(7).asInstanceOf[Long]) / 1e9

    // The cost of the second region should be the cost of the keyword operator.
    assert(keywordRegionCost == keywordOperatorCost)

    // The cost of the region plan should be the sum of region costs
    assert(searchResult.cost == groupByRegionCost + keywordRegionCost)
  }

  "DefaultCostEstimator.withResourceAllocator" should "share loaded execution statistics without re-querying DB or storage" in {
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

    seedExecutionRecord()

    val headerlessCsvOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        headerlessCsvOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        100L,
        100L,
        0L,
        1,
        0
      )
    )
    val keywordOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        keywordOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        300L,
        300L,
        0L,
        1,
        0
      )
    )

    writer.putOne(headerlessCsvOpRuntimeStatistics)
    writer.putOne(keywordOpRuntimeStatistics)
    writer.close()

    val firstAllocator =
      new DefaultResourceAllocator(
        workflow.physicalPlan,
        new ExecutionClusterInfo(),
        workflow.context.workflowSettings
      )
    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      firstAllocator,
      COORDINATOR
    )

    // Delete the DB execution row so any re-query would fall back to 0.
    workflowExecutionsDao.deleteById(Integer.valueOf(1))

    val secondAllocator =
      new DefaultResourceAllocator(
        workflow.physicalPlan,
        new ExecutionClusterInfo(),
        workflow.context.workflowSettings
      )
    val scopedEstimator = costEstimator.withResourceAllocator(secondAllocator)
    assert(scopedEstimator.resourceAllocator eq secondAllocator)

    val ports = workflow.physicalPlan.operators.flatMap(op =>
      op.inputPorts.keys
        .map(inputPortId => GlobalPortIdentity(op.id, inputPortId, input = true))
        .toSet ++ op.outputPorts.keys
        .map(outputPortId => GlobalPortIdentity(op.id, outputPortId))
        .toSet
    )
    val region = Region(
      id = RegionIdentity(0),
      physicalOps = workflow.physicalPlan.operators,
      physicalLinks = workflow.physicalPlan.links,
      ports = ports
    )

    val (_, costOfRegion) = scopedEstimator.allocateResourcesAndEstimateCost(region, 1)
    val expectedKeywordCost = (300L + 300L) / 1e9
    assert(costOfRegion == expectedKeywordCost)
  }

  it should "keep fallback mode on withResourceAllocator when no past statistics existed at construction" in {
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )
    val firstAllocator =
      new DefaultResourceAllocator(
        workflow.physicalPlan,
        new ExecutionClusterInfo(),
        workflow.context.workflowSettings
      )
    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      firstAllocator,
      COORDINATOR
    )

    val secondAllocator =
      new DefaultResourceAllocator(
        workflow.physicalPlan,
        new ExecutionClusterInfo(),
        workflow.context.workflowSettings
      )
    val scopedEstimator = costEstimator.withResourceAllocator(secondAllocator)
    assert(scopedEstimator.resourceAllocator eq secondAllocator)

    val ports = workflow.physicalPlan.operators.flatMap(op =>
      op.inputPorts.keys
        .map(inputPortId => GlobalPortIdentity(op.id, inputPortId, input = true))
        .toSet ++ op.outputPorts.keys
        .map(outputPortId => GlobalPortIdentity(op.id, outputPortId))
        .toSet
    )
    val region = Region(
      id = RegionIdentity(0),
      physicalOps = workflow.physicalPlan.operators,
      physicalLinks = workflow.physicalPlan.links,
      ports = ports
    )

    val (_, costOfRegion) = scopedEstimator.allocateResourcesAndEstimateCost(region, 1)
    assert(costOfRegion == 0)
  }
}
