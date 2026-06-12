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

import com.twitter.util.{Await, Duration, Promise}
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.workflow.{
  ExecutionMode,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.architecture.controller._
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmptyRequest
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  setUpWorkflowExecutionData
}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}

import scala.concurrent.duration.DurationInt

/**
  * End-to-end loop tests: run a real TextInput -> LoopStart -> LoopEnd workflow
  * through the engine (controller + Python workers + the DCM back-jump and
  * region re-execution) and assert both that it terminates AND that it ran the
  * expected number of iterations.
  *
  * Termination alone is too weak: a counter bug that still terminated (e.g.
  * off-by-one) would pass. So each test asserts the terminal LoopEnd's
  * cumulative output-tuple count. LoopEnd is an identity pass-through on data,
  * so by conservation that count equals the number of rows that flowed through
  * the loop -- i.e. the iteration count: 3 for the single loop, 9 for the 3x3
  * nested loop. The count comes from `ExecutionStatsUpdate`, which the
  * controller delivers (after querying final worker stats) before
  * `ExecutionStateUpdate(COMPLETED)`; the worker persists across the
  * `JumpToOperatorRegion` re-executions so its output statistic accumulates
  * across iterations rather than resetting.
  *
  * Tagged @IntegrationTest because it spawns Python workers; routed to the
  * `amber-integration` CI job.
  */
@IntegrationTest
class LoopIntegrationSpec
    extends TestKit(ActorSystem("LoopIntegrationSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Retries {

  override def withFixture(test: NoArgTest): Outcome =
    withRetry { super.withFixture(test) }

  implicit val timeout: Timeout = Timeout(5.seconds)

  override protected def beforeEach(): Unit = setUpWorkflowExecutionData()

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData()

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  // Loops require MATERIALIZED execution mode (the cross-region state channel
  // is the loop back-edge).
  private def materializedContext(): WorkflowContext =
    new WorkflowContext(
      workflowSettings = WorkflowSettings(
        dataTransferBatchSize = 400,
        executionMode = ExecutionMode.MATERIALIZED
      )
    )

  /**
    * Run the workflow to completion and return each logical operator's
    * cumulative output-tuple count (keyed by logical op id). The map is
    * captured from the latest `ExecutionStatsUpdate`, which the controller
    * delivers before `ExecutionStateUpdate(COMPLETED)`, so it is complete by
    * the time the loop terminates.
    */
  private def runAndGetOutputCounts(
      operators: List[LogicalOp],
      links: List[LogicalLink]
  ): Map[String, Long] = {
    val workflow = buildWorkflow(operators, links, materializedContext())
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      ControllerConfig.default,
      _ => {}
    )
    val completion = Promise[Unit]()
    // Latest per-operator cumulative output-tuple counts. ExecutionStatsUpdate
    // is keyed by logical op id (WorkflowExecution aggregates worker stats by
    // logicalOpId), so tests look up an operator via its operatorIdentifier.id.
    var outputCounts: Map[String, Long] = Map.empty
    client.registerCallback[ExecutionStatsUpdate](evt => {
      outputCounts = evt.operatorMetrics.map {
        case (opId, metrics) =>
          opId -> metrics.operatorStatistics.outputMetrics.map(_.tupleMetrics.count).sum
      }
    })
    client.registerCallback[FatalError](evt => {
      completion.setException(evt.e)
      client.shutdown()
    })
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == COMPLETED) completion.setDone()
    })
    Await.result(client.controllerInterface.startWorkflow(EmptyRequest(), ()))
    // A correct loop terminates; a broken one hangs until this deadline.
    Await.result(completion, Duration.fromMinutes(3))
    client.shutdown()
    outputCounts
  }

  private def textInput(text: String): TextInputSourceOpDesc = {
    val op = new TextInputSourceOpDesc()
    op.textInput = text
    op
  }

  private def loopStart(initialization: String, output: String): LoopStartOpDesc = {
    val op = new LoopStartOpDesc()
    op.initialization = initialization
    op.output = output
    op
  }

  private def loopEnd(update: String, condition: String): LoopEndOpDesc = {
    val op = new LoopEndOpDesc()
    op.update = update
    op.condition = condition
    op
  }

  private def link(from: LogicalOp, to: LogicalOp): LogicalLink =
    LogicalLink(from.operatorIdentifier, PortIdentity(), to.operatorIdentifier, PortIdentity())

  "Engine" should "run a single TextInput -> LoopStart -> LoopEnd loop for exactly 3 iterations" in {
    val src = textInput("1\n2\n3")
    val start = loopStart("i = 0", "table.iloc[i]")
    val end = loopEnd("i += 1", "i < len(table)")
    val counts = runAndGetOutputCounts(
      List(src, start, end),
      List(link(src, start), link(start, end))
    )
    // LoopStart emits one row per iteration (table.iloc[i]); i advances
    // 0,1,2 and stops at i == 3, so the body runs exactly 3 times. LoopEnd
    // passes those rows through unchanged, so its cumulative output count is
    // the iteration count. An off-by-one counter bug that still terminated
    // would land on 2 or 4 here.
    assert(counts.getOrElse(end.operatorIdentifier.id, -1L) == 3)
  }

  it should "run a nested loop for exactly 9 inner iterations (3 outer x 3 inner)" in {
    // TextInput -> OuterStart -> InnerStart -> InnerEnd -> OuterEnd.
    //
    // The outer LoopStart emits the WHOLE 3-row table on each outer iteration
    // (output = "table"), so the inner loop iterates over 3 rows; with 3 outer
    // iterations the inner body runs 3 x 3 = 9 times. Because every LoopEnd is
    // an identity pass-through on data, the same 9 rows flow out of the
    // terminal outer LoopEnd, so its cumulative output count is 9.
    //
    // This is the case that exercises the loop_counter increment/decrement and
    // the LoopStartId/LoopStartStateURI routing carried on the StateFrame
    // envelope: the outer loop's state passes THROUGH the inner LoopStart (+1)
    // and inner LoopEnd (-1) untouched, and is consumed only at the outer
    // LoopEnd (counter == 0). A routing or counter bug would change the 9, or
    // mis-consume and hang.
    val src = textInput("1\n2\n3")
    val outerStart = loopStart("i = 0", "table")
    val innerStart = loopStart("j = 0", "table.iloc[j]")
    val innerEnd = loopEnd("j += 1", "j < len(table)")
    val outerEnd = loopEnd("i += 1", "i < len(table)")
    val counts = runAndGetOutputCounts(
      List(src, outerStart, innerStart, innerEnd, outerEnd),
      List(
        link(src, outerStart),
        link(outerStart, innerStart),
        link(innerStart, innerEnd),
        link(innerEnd, outerEnd)
      )
    )
    // Every inner iteration produces one row, and each LoopEnd forwards it
    // unchanged, so the 9 inner-iteration rows flow all the way out of the
    // terminal outer LoopEnd. (This matches the Nested.Loop.json demo, which
    // was observed to run the inner body 9 times.)
    assert(counts.getOrElse(outerEnd.operatorIdentifier.id, -1L) == 9)
  }
}
