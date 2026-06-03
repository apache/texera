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
  * region re-execution) and assert it reaches COMPLETED. Termination is the
  * assertion -- a loop whose counter / state hand-off were broken by the
  * "loop_counter / LoopStartId / LoopStartStateURI off State" and guarded
  * exec-namespace changes would hang and time out rather than complete.
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

  private def runToCompletion(
      operators: List[LogicalOp],
      links: List[LogicalLink]
  ): Unit = {
    val workflow = buildWorkflow(operators, links, materializedContext())
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      ControllerConfig.default,
      _ => {}
    )
    val completion = Promise[Unit]()
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

  "Engine" should "run a single TextInput -> LoopStart -> LoopEnd loop to completion" in {
    val src = textInput("1\n2\n3")
    val start = loopStart("i = 0", "table.iloc[i]")
    val end = loopEnd("i += 1", "i < len(table)")
    runToCompletion(
      List(src, start, end),
      List(link(src, start), link(start, end))
    )
    succeed // reaching COMPLETED within the deadline is the assertion
  }

  it should "run a nested loop (outer x inner) to completion" in {
    // TextInput -> OuterStart -> InnerStart -> InnerEnd -> OuterEnd. This is
    // the case that actually exercises the loop_counter increment/decrement
    // and the LoopStartId/LoopStartStateURI routing now carried on the
    // StateFrame envelope: the outer loop's state passes THROUGH the inner
    // LoopStart (+1) and inner LoopEnd (-1) untouched, and is consumed only at
    // the outer LoopEnd (counter == 0). With 3 input rows the inner body runs
    // 3 times per outer iteration (9 total); a routing bug would mis-consume
    // and hang or never terminate.
    val src = textInput("1\n2\n3")
    val outerStart = loopStart("i = 0", "table.iloc[i]")
    val innerStart = loopStart("j = 0", "table.iloc[j]")
    val innerEnd = loopEnd("j += 1", "j < len(table)")
    val outerEnd = loopEnd("i += 1", "i < len(table)")
    runToCompletion(
      List(src, outerStart, innerStart, innerEnd, outerEnd),
      List(
        link(src, outerStart),
        link(outerStart, innerStart),
        link(innerStart, innerEnd),
        link(innerEnd, outerEnd)
      )
    )
    succeed
  }
}
