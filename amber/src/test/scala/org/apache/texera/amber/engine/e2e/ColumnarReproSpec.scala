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
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.BeforeAndAfterAll

import com.twitter.util.Duration
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class ColumnarReproSpec
    extends TestKit(ActorSystem("ColumnarReproSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  private val ids = new AtomicInteger(70000)
  private val tag = if (sys.env.get("COLUMNAR_WIRE").contains("1")) "COLUMNAR" else "ROW"

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def filt(attr: String, v: String): SpecializedFilterOpDesc = {
    val op = new SpecializedFilterOpDesc()
    op.predicates = List(new FilterPredicate(attr, ComparisonType.GREATER_THAN, v)); op
  }

  private def report(name: String, res: Map[OperatorIdentity, List[Tuple]]): Unit = {
    val rows = res.values.headOption.getOrElse(Nil)
    println(s"REPRO[$tag] $name: ${rows.size} rows")
    rows.take(3).foreach { t =>
      println(
        s"REPRO[$tag]   Region=${t.getField[Any]("Region")} UnitsSold=${t.getField[Any]("Units Sold")}"
      )
    }
  }

  "columnar source" should "scan -> filter(terminal)" in {
    val id = ids.incrementAndGet(); setUpWorkflowExecutionData(id)
    try {
      val ctx: WorkflowContext = TestUtils.workflowContext(id)
      val scan = TestOperators.smallCsvScanOpDesc()
      val f = filt("Units Sold", "5000")
      val wf = buildWorkflow(
        List(scan, f),
        List(
          LogicalLink(scan.operatorIdentifier, PortIdentity(), f.operatorIdentifier, PortIdentity())
        ),
        ctx
      )
      report("scan->filter", runWorkflowAndReadTerminalResults(system, wf, Duration.fromMinutes(5)))
    } finally cleanupWorkflowExecutionData(id)
  }

  "native filter" should "scan -> filter -> filter(terminal)" in {
    val id = ids.incrementAndGet(); setUpWorkflowExecutionData(id)
    try {
      val ctx: WorkflowContext = TestUtils.workflowContext(id)
      val scan = TestOperators.smallCsvScanOpDesc()
      val f1 = filt("Units Sold", "5000")
      val f2 = filt("Units Sold", "0")
      val wf = buildWorkflow(
        List(scan, f1, f2),
        List(
          LogicalLink(
            scan.operatorIdentifier,
            PortIdentity(),
            f1.operatorIdentifier,
            PortIdentity()
          ),
          LogicalLink(f1.operatorIdentifier, PortIdentity(), f2.operatorIdentifier, PortIdentity())
        ),
        ctx
      )
      report(
        "scan->filter->filter",
        runWorkflowAndReadTerminalResults(system, wf, Duration.fromMinutes(5))
      )
    } finally cleanupWorkflowExecutionData(id)
  }
}
