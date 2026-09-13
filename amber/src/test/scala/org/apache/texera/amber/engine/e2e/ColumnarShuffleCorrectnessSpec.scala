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
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadTerminalResults,
  setUpWorkflowExecutionData
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.filter.{ComparisonType, FilterPredicate, SpecializedFilterOpDesc}
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.workflow.LogicalLink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import com.twitter.util.Duration
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

/**
  * Multi-worker columnar correctness: scan -> filter -> count-by-group. The
  * aggregate's group-by induces a hash-shuffle edge upstream, so with more than
  * one worker the columnar path must split each Arrow batch per receiver. Run
  * this under COLUMNAR_WIRE=0 and =1 (with CONSTANTS_NUM_WORKER_PER_OPERATOR=2)
  * and diff the SHUFFLE lines: the group counts must match. Proves the columnar
  * shuffle partitioner produces the same result as the row path.
  */
class ColumnarShuffleCorrectnessSpec
    extends TestKit(ActorSystem("ColumnarShuffleCorrectnessSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  private val ids = new AtomicInteger(90000)
  private val tag = if (sys.env.get("COLUMNAR_WIRE").contains("1")) "COLUMNAR" else "ROW"
  private val workers = sys.env.getOrElse("CONSTANTS_NUM_WORKER_PER_OPERATOR", "1")

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def filterGT(attr: String, v: String): SpecializedFilterOpDesc = {
    val op = new SpecializedFilterOpDesc()
    op.predicates = List(new FilterPredicate(attr, ComparisonType.GREATER_THAN, v)); op
  }
  private def agg(fn: AggregationFunction, attr: String, res: String): AggregationOperation = {
    val a = new AggregationOperation(); a.aggFunction = fn; a.attribute = attr; a.resultAttribute = res; a
  }
  private def aggByRegion(): AggregateOpDesc = {
    val op = new AggregateOpDesc()
    op.aggregations = List(
      agg(AggregationFunction.COUNT, "Region", "cnt"),
      agg(AggregationFunction.SUM, "Units Sold", "sum_units"),
      agg(AggregationFunction.MIN, "Units Sold", "min_units"),
      agg(AggregationFunction.MAX, "Units Sold", "max_units"),
      agg(AggregationFunction.AVERAGE, "Units Sold", "avg_units")
    )
    op.groupByKeys = List("Region"); op
  }

  private def report(res: Map[OperatorIdentity, List[Tuple]]): Unit = {
    val rows = res.values.headOption.getOrElse(Nil)
    val lines = rows
      .map(t =>
        s"${t.getField[Any]("Region")} cnt=${t.getField[Any]("cnt")} sum=${t.getField[Any]("sum_units")} " +
          s"min=${t.getField[Any]("min_units")} max=${t.getField[Any]("max_units")} avg=${t.getField[Any]("avg_units")}"
      )
      .sorted
    println(s"SHUFFLE[$tag workers=$workers] groups=${rows.size}")
    lines.foreach(l => println(s"SHUFFLE[$tag]   $l"))
  }

  "columnar shuffle" should "scan -> filter -> agg-by-Region match the row path" in {
    val id = ids.incrementAndGet(); setUpWorkflowExecutionData(id)
    try {
      val ctx: WorkflowContext = TestUtils.workflowContext(id)
      val scan = TestOperators.smallCsvScanOpDesc()
      val f = filterGT("Units Sold", "5000")
      val aggOp = aggByRegion()
      val wf = buildWorkflow(
        List(scan, f, aggOp),
        List(
          LogicalLink(scan.operatorIdentifier, PortIdentity(), f.operatorIdentifier, PortIdentity()),
          LogicalLink(f.operatorIdentifier, PortIdentity(), aggOp.operatorIdentifier, PortIdentity())
        ),
        ctx
      )
      report(runWorkflowAndReadTerminalResults(system, wf, Duration.fromMinutes(5)))
    } finally cleanupWorkflowExecutionData(id)
  }

  private def projectRegionUnits(): ProjectionOpDesc = {
    val op = new ProjectionOpDesc()
    op.attributes = List(new AttributeUnit("Region", "Region"), new AttributeUnit("Units Sold", "units"))
    op
  }
  private def countSumByRegionOn(unitsCol: String): AggregateOpDesc = {
    val op = new AggregateOpDesc()
    op.aggregations = List(
      agg(AggregationFunction.COUNT, "Region", "cnt"),
      agg(AggregationFunction.SUM, unitsCol, "sum_units")
    )
    op.groupByKeys = List("Region"); op
  }
  private def reportCntSum(res: Map[OperatorIdentity, List[Tuple]]): Unit = {
    val rows = res.values.headOption.getOrElse(Nil)
    val lines =
      rows.map(t => s"${t.getField[Any]("Region")} cnt=${t.getField[Any]("cnt")} sum=${t.getField[Any]("sum_units")}").sorted
    println(s"PROJ[$tag workers=$workers] groups=${rows.size}")
    lines.foreach(l => println(s"PROJ[$tag]   $l"))
  }

  private def reportJoin(res: Map[OperatorIdentity, List[Tuple]]): Unit = {
    val rows = res.values.headOption.getOrElse(Nil)
    val checksum = rows.map(_.getFields.mkString("|")).sorted.mkString("\n").hashCode
    println(s"JOIN[$tag workers=$workers] rows=${rows.size} checksum=$checksum")
  }

  // Selective columnar probe: the probe decodes only the join key per row and
  // fully decodes a row only on a match. Row count + content checksum must match
  // the row path.
  "columnar join" should "csv join csv on column-1 match the row path" in {
    val id = ids.incrementAndGet(); setUpWorkflowExecutionData(id)
    try {
      val ctx: WorkflowContext = TestUtils.workflowContext(id)
      val c1 = TestOperators.headerlessSmallCsvScanOpDesc()
      val c2 = TestOperators.headerlessSmallCsvScanOpDesc()
      val join = TestOperators.joinOpDesc("column-1", "column-1")
      val wf = buildWorkflow(
        List(c1, c2, join),
        List(
          LogicalLink(c1.operatorIdentifier, PortIdentity(), join.operatorIdentifier, PortIdentity()),
          LogicalLink(c2.operatorIdentifier, PortIdentity(), join.operatorIdentifier, PortIdentity(1))
        ),
        ctx
      )
      reportJoin(runWorkflowAndReadTerminalResults(system, wf, Duration.fromMinutes(5)))
    } finally cleanupWorkflowExecutionData(id)
  }

  // Projection renames "Units Sold" -> "units"; the downstream filter and
  // aggregate then reference the renamed column, all over the columnar wire.
  "columnar projection" should "scan -> project(rename) -> filter -> agg match the row path" in {
    val id = ids.incrementAndGet(); setUpWorkflowExecutionData(id)
    try {
      val ctx: WorkflowContext = TestUtils.workflowContext(id)
      val scan = TestOperators.smallCsvScanOpDesc()
      val proj = projectRegionUnits()
      val f = filterGT("units", "5000")
      val aggOp = countSumByRegionOn("units")
      val wf = buildWorkflow(
        List(scan, proj, f, aggOp),
        List(
          LogicalLink(scan.operatorIdentifier, PortIdentity(), proj.operatorIdentifier, PortIdentity()),
          LogicalLink(proj.operatorIdentifier, PortIdentity(), f.operatorIdentifier, PortIdentity()),
          LogicalLink(f.operatorIdentifier, PortIdentity(), aggOp.operatorIdentifier, PortIdentity())
        ),
        ctx
      )
      reportCntSum(runWorkflowAndReadTerminalResults(system, wf, Duration.fromMinutes(5)))
    } finally cleanupWorkflowExecutionData(id)
  }
}
