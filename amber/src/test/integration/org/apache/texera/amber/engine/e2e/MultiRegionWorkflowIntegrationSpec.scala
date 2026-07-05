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

import com.twitter.util.Duration
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadTerminalResults,
  setUpWorkflowExecutionData
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.workflow.LogicalLink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

/**
  * End-to-end coverage for a workflow that executes across MULTIPLE regions.
  *
  * Cross-region data used to be delivered by dedicated cache-source operators;
  * #3425 replaced them with input-port materialization reader threads. Before
  * removing the now-dead cache-source plumbing, this spec adds the missing
  * end-to-end coverage for that multi-region path: until now it was exercised
  * only by scheduling unit tests (region counting on a physical plan).
  *
  * The workflow is a big "X": two Python sources fan into a hash join
  * (build/probe) feeding a Python sort UDF, and also into a union; the sort and
  * the union are the two terminal (materialized) outputs.
  *
  * {{{
  *   pySrc1 ─┬─▶ join.build (port 0) ─┐
  *           │                         join ─▶ pythonSort ─▶ (terminal)
  *   pySrc2 ─┼─▶ join.probe (port 1) ─┘
  *           │
  *   pySrc1 ─┼─▶ union ───────────────────────────────────▶ (terminal)
  *   pySrc2 ─┘   (two links fan into union's single port)
  * }}}
  *
  * The hash join's probe-depends-on-build ordering forces the build input to be
  * materialized, so the controller schedules the plan as >=2 regions and the
  * data crosses a region boundary via the reader-thread path. The test drives
  * the workflow as a black box: it builds a logical plan and runs it through the
  * real compiler, controller, and scheduler, then asserts only on the
  * materialized outputs -- correct results are only possible if the cross-region
  * delivery worked. (The region count itself is covered by the scheduling unit
  * tests, so it is not re-asserted here against a hand-wired scheduler.)
  *
  * Every operator runs as a real Python worker, so it is class-level
  * `@IntegrationTest` tagged and routed to the `amber-integration` CI job (which
  * provisions Python deps); the lighter `amber` job excludes this tag.
  */
@IntegrationTest
class MultiRegionWorkflowIntegrationSpec
    extends TestKit(ActorSystem("MultiRegionWorkflowIntegrationSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Retries {

  /**
    * Retry each test once if it fails. Mirrors the other e2e integration specs:
    * in CI there is a small chance the run does not observe "COMPLETED", so a
    * single retry stabilizes the suite until that root cause is fixed.
    */
  override def withFixture(test: NoArgTest): Outcome =
    withRetry { super.withFixture(test) }

  implicit val timeout: Timeout = Timeout(5.seconds)

  private val logger = Logger("MultiRegionWorkflowIntegrationSpecLogger")
  private val specId = 5

  // Each Python source emits `field_1` = "0".."N-1" (unique strings), so the
  // self-join on `field_1` matches one row per key and the union doubles them.
  private val sourceRowCount = 100

  // Buffer every input tuple, then emit them ordered by "field_1" once the input
  // port is exhausted. This makes the UDF a genuine sort and forces a real
  // Python worker to process data that crossed a region boundary.
  private val sortByFieldCode =
    """
      |from pytexera import *
      |
      |class ProcessTupleOperator(UDFOperatorV2):
      |    def open(self) -> None:
      |        self.buffer = []
      |
      |    @overrides
      |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
      |        self.buffer.append(tuple_)
      |        yield from []
      |
      |    @overrides
      |    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
      |        for row in sorted(self.buffer, key=lambda t: t["field_1"]):
      |            yield row
      |""".stripMargin

  override protected def beforeEach(): Unit = {
    setUpWorkflowExecutionData(specId)
  }

  override protected def afterEach(): Unit = {
    cleanupWorkflowExecutionData(specId)
  }

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    // These test cases access postgres in CI, but occasionally the jdbc driver cannot be found during CI run.
    // Explicitly load the JDBC driver to avoid flaky CI failures.
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
    warmupOnce()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  /**
    * Runs a TextInput -> Python UDF workflow once before the timed test so the
    * Python worker cold-start is paid here, not inside the timed run. Reuses the
    * shared runner (which owns the client lifecycle, fails fast on FatalError,
    * and shuts the client down), and is wrapped/capped so warmup can never fail
    * or hang the suite.
    */
  private def warmupOnce(): Unit = {
    setUpWorkflowExecutionData(specId)
    try {
      val src = new TextInputSourceOpDesc()
      src.textInput = "warmup"
      val udf = TestOperators.pythonOpDesc()
      val workflow = buildWorkflow(
        List(src, udf),
        List(
          LogicalLink(
            src.operatorIdentifier,
            PortIdentity(),
            udf.operatorIdentifier,
            PortIdentity()
          )
        ),
        TestUtils.workflowContext(specId)
      )
      runWorkflowAndReadTerminalResults(system, workflow, Duration.fromSeconds(60))
    } catch {
      case e: Throwable =>
        logger.warn(
          s"warmup workflow did not finish; tests will run cold and rely on Retries: ${e.getMessage}"
        )
    } finally {
      cleanupWorkflowExecutionData(specId)
    }
  }

  "Engine" should "execute an X-shaped multi-region workflow (python sources + join + union + python UDF) end-to-end" in {
    val pySrc1 = TestOperators.pythonSourceOpDesc(sourceRowCount)
    val pySrc2 = TestOperators.pythonSourceOpDesc(sourceRowCount)
    // Self-join on the unique "field_1" key: each row matches exactly one row
    // from the other source, so the join emits exactly `sourceRowCount` rows.
    val join = TestOperators.joinOpDesc("field_1", "field_1")
    val pythonSort = TestOperators.pythonOpDesc()
    pythonSort.code = sortByFieldCode
    val union = new UnionOpDesc()

    val workflow = buildWorkflow(
      List(pySrc1, pySrc2, join, pythonSort, union),
      List(
        // Left arm of the X: the two sources feed the join (build/probe) ...
        LogicalLink(
          pySrc1.operatorIdentifier,
          PortIdentity(),
          join.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          pySrc2.operatorIdentifier,
          PortIdentity(),
          join.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          join.operatorIdentifier,
          PortIdentity(),
          pythonSort.operatorIdentifier,
          PortIdentity()
        ),
        // ... and also fan into the union's single input port (the crossing).
        LogicalLink(
          pySrc1.operatorIdentifier,
          PortIdentity(),
          union.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          pySrc2.operatorIdentifier,
          PortIdentity(),
          union.operatorIdentifier,
          PortIdentity()
        )
      ),
      TestUtils.workflowContext(specId)
    )

    // Black box: run the built workflow through the real controller/scheduler
    // and read only the terminal outputs. The join branch and the union branch
    // land in different regions, so correct results here prove the cross-region
    // materialization path delivered the data.
    val results = runWorkflowAndReadTerminalResults(system, workflow, Duration.fromMinutes(2))

    val sortedRows = results(pythonSort.operatorIdentifier)
    val unionRows = results(union.operatorIdentifier)

    // Join branch: self-join on a unique key emits one row per source row, and
    // the Python UDF returns them ordered by "field_1".
    assert(sortedRows.size == sourceRowCount)
    val keys = sortedRows.map(_.getField("field_1").asInstanceOf[String])
    assert(keys == keys.sorted, "python UDF output should be sorted by field_1")

    // Union branch: concatenation of both sources, which share a schema.
    assert(unionRows.size == 2 * sourceRowCount)
  }

}
