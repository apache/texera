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
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.workflow.{
  ExecutionMode,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
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
  * off-by-one) would pass. So each test asserts the LoopEnd's MATERIALIZED
  * result-table row count, read from iceberg after the run. LoopEnd is an
  * identity pass-through on data, so the rows it materializes equal the rows
  * that flowed through it: a single (outermost) LoopEnd accumulates every
  * iteration (3 for the single loop; 9 for the terminal outer LoopEnd of the
  * 3x3 nested loop), and an inner LoopEnd resets once per outer iteration (so
  * 3, not 9).
  *
  * NOTE: the cumulative `ExecutionStatsUpdate` output count is NOT usable as
  * the iteration count here. A loop region's workers are recreated on every
  * `JumpToOperatorRegion` re-execution (each iteration spawns a fresh worker
  * actor), so the per-logical-op output statistic reflects only the final
  * iteration's worker and does not accumulate. The iceberg result, by
  * contrast, persists across iterations (the scheduler reuses a LoopEnd's
  * output document via `reusesOutputStorageOnReExecution`), so it is the
  * reliable signal.
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
    * Run the loop workflow to completion and return each operator's
    * materialized RESULT-table row count, keyed by logical op id.
    *
    * Read inside the `COMPLETED` callback while the engine and storage
    * singletons are still alive (the same point `DataProcessingSpec` reads its
    * results), looking up each operator's external RESULT uri the way the
    * frontend does. Operators whose uri is absent, or whose document can't be
    * opened, are omitted (tolerated, not fatal). Failure messages below
    * include the full map so an unexpected count is visible.
    */
  private def runAndGetMaterializedRowCounts(
      operators: List[LogicalOp],
      links: List[LogicalLink]
  ): Map[String, Long] = {
    val workflow = buildWorkflow(operators, links, materializedContext())
    val eid = workflow.context.executionId
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      ControllerConfig.default,
      _ => {}
    )
    val completion = Promise[Unit]()
    var materializedCounts: Map[String, Long] = Map.empty
    client.registerCallback[FatalError](evt => {
      completion.setException(evt.e)
      client.shutdown()
    })
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == COMPLETED) {
        materializedCounts = operators.flatMap { op =>
          WorkflowExecutionsResource
            .getResultUriByLogicalPortId(eid, op.operatorIdentifier, PortIdentity())
            .flatMap { uri =>
              scala.util
                .Try(
                  DocumentFactory
                    .openDocument(uri)
                    ._1
                    .asInstanceOf[VirtualDocument[Tuple]]
                    .getCount
                )
                .toOption
                .map(count => op.operatorIdentifier.id -> count)
            }
        }.toMap
        completion.setDone()
      }
    })
    Await.result(client.controllerInterface.startWorkflow(EmptyRequest(), ()))
    // A correct loop terminates; a broken one hangs until this deadline.
    Await.result(completion, Duration.fromMinutes(3))
    client.shutdown()
    materializedCounts
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
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, end),
      List(link(src, start), link(start, end))
    )
    // LoopStart emits one row per iteration (table.iloc[i]); i advances 0,1,2
    // and stops at i == 3, so the body runs exactly 3 times. A single
    // (outermost) LoopEnd is an identity pass-through that accumulates every
    // iteration and never resets, so its materialized result holds all 3 rows.
    // An off-by-one counter bug that still terminated would land on 2 or 4.
    val endRows = materialized.getOrElse(end.operatorIdentifier.id, -1L)
    assert(
      endRows == 3,
      s"single LoopEnd must accumulate all 3 iterations in its materialized " +
        s"result: expected 3, got $endRows (all: $materialized)"
    )
  }

  it should "run a nested loop for exactly 9 inner iterations (3 outer x 3 inner)" in {
    // TextInput -> OuterStart -> InnerStart -> InnerEnd -> OuterEnd.
    //
    // The outer LoopStart emits the WHOLE 3-row table on each outer iteration
    // (output = "table"), so the inner loop iterates over 3 rows; with 3 outer
    // iterations the inner body runs 3 x 3 = 9 times. Because every LoopEnd is
    // an identity pass-through on data, the same 9 rows flow out of the
    // terminal outer LoopEnd.
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
    val materialized = runAndGetMaterializedRowCounts(
      List(src, outerStart, innerStart, innerEnd, outerEnd),
      List(
        link(src, outerStart),
        link(outerStart, innerStart),
        link(innerStart, innerEnd),
        link(innerEnd, outerEnd)
      )
    )
    // Materialized results distinguish accumulate-vs-reset: the terminal outer
    // LoopEnd accumulates all 9 inner-iteration rows that flowed through, but
    // the INNER LoopEnd resets once per outer iteration, so its materialized
    // result holds only the final outer iteration's 3 inner rows -- not 9.
    // The inner == 3 assertion is the one that fails against the pre-fix code
    // (where the inner reset never fired and it accumulated all 9).
    val outerRows = materialized.getOrElse(outerEnd.operatorIdentifier.id, -1L)
    val innerRows = materialized.getOrElse(innerEnd.operatorIdentifier.id, -1L)
    assert(
      outerRows == 9,
      s"outer LoopEnd must accumulate all 9 inner-iteration rows: " +
        s"expected 9, got $outerRows (all: $materialized)"
    )
    assert(
      innerRows == 3,
      s"inner LoopEnd must reset per outer iteration (3 rows, not 9): " +
        s"expected 3, got $innerRows (all: $materialized)"
    )
  }
}
