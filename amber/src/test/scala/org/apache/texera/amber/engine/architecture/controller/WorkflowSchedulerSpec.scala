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

package org.apache.texera.amber.engine.architecture.controller

import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.common.virtualidentity.util.CONTROLLER
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowSchedulerSpec extends AnyFlatSpec {

  private def buildHeaderlessCsvKeywordWorkflow() = {
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
      new WorkflowContext()
    )
  }

  "WorkflowScheduler.updateSchedule" should "populate the schedule and physicalPlan fields" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val scheduler = new WorkflowScheduler(workflow.context, CONTROLLER)

    assert(scheduler.getSchedule == null)
    assert(scheduler.physicalPlan == null)

    scheduler.updateSchedule(workflow.physicalPlan)

    assert(scheduler.getSchedule != null)
    assert(scheduler.physicalPlan != null)
    assert(scheduler.getSchedule.getRegions.nonEmpty)
  }

  it should "include every workflow operator in some region of the produced schedule" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val scheduler = new WorkflowScheduler(workflow.context, CONTROLLER)
    scheduler.updateSchedule(workflow.physicalPlan)

    val operatorsInSchedule = scheduler.getSchedule.getRegions
      .flatMap(_.getOperators.map(_.id.logicalOpId))
      .toSet
    val operatorsInPlan = scheduler.physicalPlan.operators.map(_.id.logicalOpId)

    assert(operatorsInPlan.subsetOf(operatorsInSchedule))
  }

  "WorkflowScheduler.getNextRegions" should "exhaust the schedule and then return an empty set" in {
    val workflow = buildHeaderlessCsvKeywordWorkflow()
    val scheduler = new WorkflowScheduler(workflow.context, CONTROLLER)
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
    val scheduler = new WorkflowScheduler(workflow.context, CONTROLLER)
    scheduler.updateSchedule(workflow.physicalPlan)

    val expectedRegions = scheduler.getSchedule.getRegions.toSet
    val pulledRegions = Iterator
      .continually(scheduler.getNextRegions)
      .takeWhile(_.nonEmpty)
      .flatten
      .toSet

    assert(pulledRegions == expectedRegions)
  }
}
