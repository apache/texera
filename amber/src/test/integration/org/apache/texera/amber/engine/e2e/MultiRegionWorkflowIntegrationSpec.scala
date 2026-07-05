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
import org.apache.texera.amber.operator.aggregate.AggregationFunction
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.workflow.LogicalLink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

/**
  * End-to-end coverage for a workflow that executes across MULTIPLE regions and
  * strings together a representative operator from each of the main
  * data-processing families.
  *
  * Cross-region data used to be delivered by dedicated cache-source operators;
  * #3425 replaced them with input-port materialization reader threads. Before
  * removing the now-dead cache-source plumbing, this spec adds the missing
  * end-to-end coverage for that multi-region path: until now it was exercised
  * only by scheduling unit tests (region counting on a physical plan).
  *
  * The workflow is a big "X" -- two CSV scans fan into a hash join and, in
  * parallel, into a union -- with a blocking processing chain hanging off each
  * arm so the plan is cut into several regions:
  *
  * {{{
  *   csvLeft  ─▶ join.build (port 0) ┐
  *                                   ├─▶ join ─▶ aggregate ─▶ sort ──▶ (terminal A)
  *   csvRight ─▶ join.probe (port 1) ┘
  *
  *   csvLeft  ─┐
  *             ├─▶ union ─▶ projection ─▶ pythonUDF ──────────────────▶ (terminal B)
  *   csvRight ─┘   (both scans fan into union's single input port)
  * }}}
  *
  * Representative operators, one per family: CSV scan (Data Input), hash join
  * (Join), aggregate (Aggregate), the native pandas-based Sort (Sort), union
  * (Set), projection (Data Cleaning), and a Python UDF (User-defined Functions).
  *
  * Region boundaries are forced by BLOCKING outputs -- the hash join's build
  * side (probe depends on build, so build must be materialized), the aggregate,
  * and the sort. Each boundary is a materialization point that the downstream
  * region reads back through the input-port reader-thread path, so a run of this
  * plan necessarily spans multiple regions and crosses several of them. The
  * exact region count is already pinned by the scheduling unit tests
  * (`CostBasedScheduleGeneratorSpec`); this spec instead drives the workflow as
  * a black box -- building a logical plan and running it through the real
  * compiler, controller, and scheduler -- and asserts only on the materialized
  * terminal outputs, which can only be correct if the cross-region delivery
  * worked.
  *
  * Assertions (`country_sales_small.csv` has 100 rows, a unique `Order ID`, and
  * 7 distinct `Region` values):
  *
  *   - Terminal A: the self-join on the unique `Order ID` key emits one row per
  *     source row (100), the aggregate collapses them into one COUNT per region
  *     (7 rows) whose counts sum back to 100, and the native Sort returns those
  *     rows ordered by `Region`.
  *   - Terminal B: the union concatenates both scans (200 rows), the projection
  *     narrows the schema to `{Region, Order ID}`, and the Python UDF passes the
  *     rows through unchanged.
  *
  * The native Sort and the Python UDF both run as real Python worker
  * subprocesses, so this spec is class-level `@IntegrationTest` tagged and routed
  * to the `amber-integration` CI job (which provisions Python deps); the lighter
  * `amber` job excludes this tag.
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

  // Stable properties of the checked-in test resource, asserted below.
  private val sourceRowCount = 100
  private val distinctRegionCount = 7

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

  private def sortOpDesc(attributeName: String, order: SortPreference): SortOpDesc = {
    val criteria = new SortCriteriaUnit()
    criteria.attributeName = attributeName
    criteria.sortPreference = order
    val sortOp = new SortOpDesc()
    sortOp.attributes = List(criteria)
    sortOp
  }

  private def projectionOpDesc(attributeNames: String*): ProjectionOpDesc = {
    val projectionOp = new ProjectionOpDesc()
    projectionOp.attributes = attributeNames.map(name => new AttributeUnit(name, name)).toList
    projectionOp
  }

  "Engine" should "execute an X-shaped multi-region workflow spanning representative operators end-to-end" in {
    val csvLeft = TestOperators.smallCsvScanOpDesc()
    val csvRight = TestOperators.smallCsvScanOpDesc()

    // Join arm: self-join on the unique "Order ID" key, so each row matches
    // exactly one row from the other scan and the join emits `sourceRowCount`
    // rows. The join's build output is blocking (probe depends on build), which
    // cuts the plan and forces the data across a region boundary.
    val join = TestOperators.joinOpDesc("Order ID", "Order ID")
    val countPerRegion =
      TestOperators.aggregateAndGroupByDesc("Order ID", AggregationFunction.COUNT, List("Region"))
    val sort = sortOpDesc("Region", SortPreference.ASC)

    // Union arm: concatenate both scans, narrow the schema, then pass the rows
    // through a Python worker.
    val union = new UnionOpDesc()
    val projection = projectionOpDesc("Region", "Order ID")
    val pythonUDF = TestOperators.pythonOpDesc()

    val workflow = buildWorkflow(
      List(csvLeft, csvRight, join, countPerRegion, sort, union, projection, pythonUDF),
      List(
        // Join arm: the two scans feed the join (build / probe) ...
        LogicalLink(
          csvLeft.operatorIdentifier,
          PortIdentity(),
          join.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          csvRight.operatorIdentifier,
          PortIdentity(),
          join.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          join.operatorIdentifier,
          PortIdentity(),
          countPerRegion.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          countPerRegion.operatorIdentifier,
          PortIdentity(),
          sort.operatorIdentifier,
          PortIdentity()
        ),
        // ... and, in parallel, both fan into the union's single input port.
        LogicalLink(
          csvLeft.operatorIdentifier,
          PortIdentity(),
          union.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          csvRight.operatorIdentifier,
          PortIdentity(),
          union.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          union.operatorIdentifier,
          PortIdentity(),
          projection.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          projection.operatorIdentifier,
          PortIdentity(),
          pythonUDF.operatorIdentifier,
          PortIdentity()
        )
      ),
      TestUtils.workflowContext(specId)
    )

    // Black box: run the built workflow through the real controller / scheduler
    // and read only the terminal outputs. The two arms land in different
    // regions, so correct results here prove the cross-region materialization
    // path delivered the data.
    val results = runWorkflowAndReadTerminalResults(system, workflow, Duration.fromMinutes(2))

    val sortedCounts = results(sort.operatorIdentifier)
    val unionRows = results(pythonUDF.operatorIdentifier)

    // Terminal A -- join -> aggregate -> sort:
    // one COUNT row per region, ordered by "Region", whose counts sum back to
    // every joined row. Correct counts require every joined row to have crossed
    // the region boundary between the join and the aggregate.
    assert(sortedCounts.size == distinctRegionCount)
    val regions = sortedCounts.map(_.getField[String]("Region"))
    assert(regions == regions.sorted, "native Sort output should be ordered by Region ascending")
    val totalCount = sortedCounts.map(_.getField[Any]("aggregate-result").toString.toInt).sum
    assert(totalCount == sourceRowCount)

    // Terminal B -- union -> projection -> pythonUDF:
    // concatenation of both scans, narrowed to two columns and passed through
    // the Python worker unchanged.
    assert(unionRows.size == 2 * sourceRowCount)
    assert(unionRows.head.getSchema.getAttributeNames.toSet == Set("Region", "Order ID"))
  }

}
