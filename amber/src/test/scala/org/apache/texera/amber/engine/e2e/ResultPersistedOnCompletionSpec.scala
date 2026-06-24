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
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadResults,
  setUpWorkflowExecutionData
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.aggregate.AggregationFunction
import org.apache.texera.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}

import scala.concurrent.duration.DurationInt

/**
  * Pins the engine invariant that `SyncExecutionResource` relies on: by the time a worker is
  * observable as COMPLETED, its result storage is already durably committed. The worker commits
  * synchronously — `OutputManager.closeOutputStorageWriterIfNeeded` joins the per-port writer
  * thread (forcing `IcebergTableWriter.close()`/`commit()`) BEFORE it emits `PortCompleted` and
  * transitions to COMPLETED — so a reader needs no sleep/poll after seeing COMPLETED.
  *
  * Each case reads result storage the instant the workflow reports COMPLETED (inside
  * `runWorkflowAndReadResults`' completion callback, with no wait) and asserts the committed row
  * count (`getCount`, the metadata the sync endpoint trusts) matches the rows actually readable.
  * If a regression made the commit lag COMPLETED, the read would come up short and fail here
  * instead of being masked by a fixed delay.
  */
class ResultPersistedOnCompletionSpec
    extends TestKit(ActorSystem("ResultPersistedOnCompletionSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Retries {

  // Mirror DataProcessingSpec: retry once to absorb the known CI flakiness where a run
  // occasionally fails to observe COMPLETED.
  override def withFixture(test: NoArgTest): Outcome =
    withRetry { super.withFixture(test) }

  implicit val timeout: Timeout = Timeout(5.seconds)

  private val workflowContext: WorkflowContext = new WorkflowContext()

  // (committed count from storage metadata, rows actually readable) read at COMPLETED, no wait.
  private val committedAndReadable: VirtualDocument[Tuple] => (Long, Long) =
    doc => (doc.getCount, doc.get().size.toLong)

  override protected def beforeEach(): Unit = setUpWorkflowExecutionData()

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData()

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    // CI occasionally cannot find the jdbc driver; load it explicitly to avoid flaky failures.
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A terminal operator's result" should "be fully committed and counted the moment the workflow reports COMPLETED" in {
    val scan = TestOperators.headerlessSmallCsvScanOpDesc() // emits exactly 100 rows
    val workflow = buildWorkflow(List(scan), List(), workflowContext)

    val counts = runWorkflowAndReadResults(
      system,
      workflow,
      List(scan.operatorIdentifier),
      committedAndReadable
    )

    val (committed, readable) = counts(scan.operatorIdentifier)
    committed shouldBe 100L
    readable shouldBe 100L
  }

  "Every materialized operator in a multi-region DAG" should "have its full output committed at COMPLETED with no wait" in {
    // count is a blocking aggregate, so the engine materializes the keyword output at the region
    // boundary in addition to the terminal — giving us an intermediate operator to check too.
    val scan = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val count =
      TestOperators.aggregateAndGroupByDesc("Region", AggregationFunction.COUNT, List[String]())
    val workflow = buildWorkflow(
      List(scan, keyword, count),
      List(
        LogicalLink(
          scan.operatorIdentifier,
          PortIdentity(),
          keyword.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keyword.operatorIdentifier,
          PortIdentity(),
          count.operatorIdentifier,
          PortIdentity()
        )
      ),
      workflowContext
    )

    val counts = runWorkflowAndReadResults(
      system,
      workflow,
      List(scan.operatorIdentifier, keyword.operatorIdentifier, count.operatorIdentifier),
      committedAndReadable
    )

    // Whatever the engine chose to materialize must be complete at COMPLETED: the committed
    // count metadata equals the rows actually readable. A short read would fail here.
    counts should not be empty
    counts.foreach {
      case (opId, (committed, readable)) =>
        withClue(s"operator $opId: committed=$committed readable=$readable: ") {
          committed shouldBe readable
        }
    }

    // The terminal global COUNT is always materialized and yields a single aggregate row.
    val (termCommitted, termReadable) = counts(count.operatorIdentifier)
    termReadable shouldBe 1L
    termCommitted shouldBe 1L
  }
}
