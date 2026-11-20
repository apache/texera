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

package org.apache.texera.amber.engine.e2e

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext, WorkflowSettings}
import org.apache.texera.amber.engine.architecture.coordinator._
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings._
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.{LogicalOp, TestOperators}
import org.apache.texera.amber.operator.aggregate.AggregationFunction
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration.DurationInt

class BatchSizePropagationSpec
    extends TestKit(ActorSystem("BatchSizePropagationSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = Timeout(5.seconds)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def verifyBatchSizeInPartitioning(
      workflowScheduler: WorkflowScheduler,
      expectedBatchSize: Int
  ): Unit = {
    var nextRegions = workflowScheduler.getNextRegions
    while (nextRegions.nonEmpty) {
      nextRegions.foreach { region =>
        region.resourceConfig.foreach { resourceConfig =>
          resourceConfig.linkConfigs.foreach {
            case (_, linkConfig) =>
              val (partitioningName, actualBatchSize) = linkConfig.partitioning match {
                case oneToOne: OneToOnePartitioning =>
                  ("OneToOnePartitioning", oneToOne.batchSize)
                case roundRobin: RoundRobinPartitioning =>
                  ("RoundRobinPartitioning", roundRobin.batchSize)
                case hashBased: HashBasedShufflePartitioning =>
                  ("HashBasedShufflePartitioning", hashBased.batchSize)
                case rangeBased: RangeBasedShufflePartitioning =>
                  ("RangeBasedShufflePartitioning", rangeBased.batchSize)
                case broadcast: BroadcastPartitioning =>
                  ("BroadcastPartitioning", broadcast.batchSize)
                case _ =>
                  throw new IllegalArgumentException("Unknown partitioning type encountered")
              }
              println(s"Testing $partitioningName with batch size: $actualBatchSize")
              assert(
                actualBatchSize == expectedBatchSize,
                s"Batch size mismatch: $actualBatchSize != $expectedBatchSize"
              )
          }
        }
      }
      nextRegions = workflowScheduler.getNextRegions
    }
  }

  private def verifyWorkflowBatchSize(
      expectedBatchSize: Int,
      operators: List[LogicalOp],
      links: List[LogicalLink] = List.empty
  ): Unit = {
    val context =
      new WorkflowContext(
        workflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)
      )
    val workflow = buildWorkflow(operators, links, context)
    val workflowScheduler = new WorkflowScheduler(context, COORDINATOR)
    workflowScheduler.updateSchedule(workflow.physicalPlan)
    verifyBatchSizeInPartitioning(workflowScheduler, expectedBatchSize)
  }

  "Engine" should "propagate the correct batch size for headerlessCsv workflow" in {
    verifyWorkflowBatchSize(1, List(TestOperators.headerlessSmallCsvScanOpDesc()))
  }

  "Engine" should "propagate the correct batch size for headerlessCsv->keyword workflow" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    verifyWorkflowBatchSize(
      500,
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        )
      )
    )
  }

  "Engine" should "propagate the correct batch size for csv->keyword->count workflow" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val countOpDesc =
      TestOperators.aggregateAndGroupByDesc("Region", AggregationFunction.COUNT, List[String]())
    verifyWorkflowBatchSize(
      100,
      List(csvOpDesc, keywordOpDesc, countOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          countOpDesc.operatorIdentifier,
          PortIdentity()
        )
      )
    )
  }

  "Engine" should "propagate the correct batch size for csv->keyword->averageAndGroupBy workflow" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val averageAndGroupByOpDesc =
      TestOperators.aggregateAndGroupByDesc(
        "Units Sold",
        AggregationFunction.AVERAGE,
        List[String]("Country")
      )
    verifyWorkflowBatchSize(
      300,
      List(csvOpDesc, keywordOpDesc, averageAndGroupByOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          averageAndGroupByOpDesc.operatorIdentifier,
          PortIdentity()
        )
      )
    )
  }

  "Engine" should "propagate the correct batch size for csv->(csv->)->join workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    verifyWorkflowBatchSize(
      1,
      List(headerlessCsvOpDesc1, headerlessCsvOpDesc2, joinOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc2.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        )
      )
    )
  }

}
