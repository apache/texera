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

package org.apache.texera.web.service

import org.apache.texera.amber.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{PhysicalPlan, PortIdentity, WorkflowContext, WorkflowSettings}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.aggregate.AggregationFunction
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.web.model.websocket.request.{TexeraWebSocketRequest, WorkflowExecuteRequest}
import org.apache.texera.workflow.WorkflowCompiler
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * The new architecture has the client compile the workflow and ship a ready-to-run
  * [[PhysicalPlan]] to the ComputingUnitMaster inside a [[WorkflowExecuteRequest]]. These tests pin
  * the two things that makes possible: the request (with its PhysicalPlan) survives the exact
  * polymorphic JSON round-trip the CU's websocket parser performs, and the CU re-derives the
  * result-storage ports from the plan + the to-view operators.
  */
class ClientPhysicalPlanRequestSpec extends AnyFlatSpec with Matchers {

  /** Compile CSV scan -> group-by aggregate into a physical plan; return it and the aggregate id. */
  private def compiledPlanAndViewOp(): (PhysicalPlan, String) = {
    val csv = TestOperators.smallCsvScanOpDesc()
    val agg =
      TestOperators.aggregateAndGroupByDesc("Units Sold", AggregationFunction.SUM, List("Country"))
    val plan = new WorkflowCompiler(new WorkflowContext())
      .compile(
        LogicalPlanPojo(
          List(csv, agg),
          List(
            LogicalLink(csv.operatorIdentifier, PortIdentity(), agg.operatorIdentifier, PortIdentity())
          ),
          List.empty,
          List.empty
        )
      )
      .physicalPlan
    (plan, agg.operatorIdentifier.id)
  }

  private def buildRequest(plan: PhysicalPlan, viewOps: List[String]): WorkflowExecuteRequest =
    WorkflowExecuteRequest(
      executionName = "test",
      engineVersion = "1.0",
      physicalPlan = plan,
      opsToViewResult = viewOps,
      replayFromExecution = None,
      workflowSettings = WorkflowSettings(dataTransferBatchSize = 400),
      emailNotificationEnabled = false,
      computingUnitId = 0
    )

  "A WorkflowExecuteRequest carrying a PhysicalPlan" should
    "survive the websocket polymorphic JSON round-trip with the plan intact" in {
    val (plan, aggId) = compiledPlanAndViewOp()
    val request: TexeraWebSocketRequest = buildRequest(plan, List(aggId))

    // Mirror WorkflowWebsocketResource: serialize via the polymorphic base ("type" discriminator),
    // then read it back as the base and dispatch on the concrete request type.
    val json = objectMapper.writeValueAsString(request)
    json should include(""""type":"WorkflowExecuteRequest"""")
    val back = objectMapper
      .readValue(json, classOf[TexeraWebSocketRequest])
      .asInstanceOf[WorkflowExecuteRequest]

    back.opsToViewResult shouldBe List(aggId)
    back.physicalPlan.operators.map(_.id) shouldBe plan.operators.map(_.id)
    back.physicalPlan.links shouldBe plan.links
    // The runtime-critical executor descriptor of every operator survives.
    plan.operators.foreach { op =>
      back.physicalPlan.getOperator(op.id).opExecInitInfo shouldBe op.opExecInitInfo
    }
  }

  "outputPortsForViewResult" should "select exactly the to-view operators' non-internal output ports" in {
    val (plan, aggId) = compiledPlanAndViewOp()

    val ports = WorkflowExecutionService.outputPortsForViewResult(plan, List(aggId))
    ports should not be empty
    ports.foreach(_.opId.logicalOpId shouldBe OperatorIdentity(aggId))
    ports.foreach(_.portId.internal shouldBe false)

    // No to-view operators -> no storage ports requested (terminal sinks are handled by the scheduler).
    WorkflowExecutionService.outputPortsForViewResult(plan, List.empty) shouldBe empty
    // An unknown operator id contributes nothing.
    WorkflowExecutionService.outputPortsForViewResult(plan, List("does-not-exist")) shouldBe empty
  }
}
