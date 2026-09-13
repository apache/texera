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

import com.twitter.util.{Await, Duration, Promise, Return, Throw}
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator._
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
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.workflow.LogicalLink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

/**
  * In-engine A/B for the columnar path (M2): scan -> selective filter -> count.
  * With COLUMNAR_WIRE=1 the scan and filter build no Tuples; only the filter's
  * survivors are decoded downstream, so a selective filter builds far fewer
  * Tuples end-to-end. Run single-worker (CONSTANTS_NUM_WORKER_PER_OPERATOR=1) so
  * all edges are one-to-one and the columnar path stays active. Prints CWBENCH.
  */
class ColumnarWorkflowBenchSpec
    extends TestKit(ActorSystem("ColumnarWorkflowBenchSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  private val dataDir = sys.env.getOrElse("TPCH_DATA", "/tmp/tpch/data_sf1")
  private val runs = sys.env.getOrElse("CW_RUNS", "2").toInt
  private val ids = new AtomicInteger(80000)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def csvScan(path: String): CSVScanSourceOpDesc = {
    val op = new CSVScanSourceOpDesc()
    op.fileName = Some(path); op.customDelimiter = Some(","); op.hasHeader = true
    op.setResolvedFileName(FileResolver.resolve(path)); op
  }
  private def filterGT(attr: String, v: String): SpecializedFilterOpDesc = {
    val op = new SpecializedFilterOpDesc()
    op.predicates = List(new FilterPredicate(attr, ComparisonType.GREATER_THAN, v)); op
  }
  private def countAgg(): AggregateOpDesc = {
    val op = new AggregateOpDesc(); val a = new AggregationOperation()
    a.aggFunction = AggregationFunction.COUNT; a.attribute = ""; a.resultAttribute = "cnt"
    op.aggregations = List(a); op.groupByKeys = List(); op
  }
  private def link(f: LogicalOp, t: LogicalOp): LogicalLink =
    LogicalLink(f.operatorIdentifier, PortIdentity(), t.operatorIdentifier, PortIdentity())

  private def timeRun(): Double = {
    val id = ids.incrementAndGet()
    setUpWorkflowExecutionData(id)
    try {
      val ctx: WorkflowContext = TestUtils.workflowContext(id)
      val scan = csvScan(s"$dataDir/lineitem.csv")
      val filt = filterGT("l_quantity", "45")
      val agg = countAgg()
      val wf = buildWorkflow(List(scan, filt, agg), List(link(scan, filt), link(filt, agg)), ctx)
      val completion = Promise[Unit]()
      val client = new AmberClient(
        system, wf.context, wf.physicalPlan, CoordinatorConfig.default,
        e => completion.updateIfEmpty(Throw(e))
      )
      try {
        client.registerCallback[FatalError](evt => completion.updateIfEmpty(Throw(evt.e)))
        client.registerCallback[ExecutionStateUpdate](evt =>
          if (evt.state == COMPLETED) completion.updateIfEmpty(Return(()))
        )
        val t0 = System.nanoTime()
        Await.result(client.coordinatorInterface.startWorkflow(EmptyRequest(), ()), Duration.fromSeconds(60))
        Await.result(completion, Duration.fromMinutes(15))
        (System.nanoTime() - t0) / 1e6
      } finally client.shutdown()
    } finally cleanupWorkflowExecutionData(id)
  }

  private val label = if (sys.env.get("COLUMNAR_WIRE").contains("1")) "COLUMNAR" else "ROW"

  "Workflow" should "time scan->filter->count" in {
    timeRun() // warmup
    val times = (1 to runs).map { i => val ms = timeRun(); println(f"CWBENCH $label run $i: ${ms / 1000}%.2fs"); ms }
    println(f"CWBENCH $label MEDIAN=${times.sorted.apply(times.length / 2) / 1000}%.2fs MIN=${times.min / 1000}%.2fs")
  }
}
