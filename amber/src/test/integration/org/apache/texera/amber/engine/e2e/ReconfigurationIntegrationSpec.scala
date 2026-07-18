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

import com.twitter.util.{Await, Duration, Promise, Return}
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.executor.{OpExecInitInfo, OpExecWithCode}
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  ExecutionStateUpdate
}
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
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.{LogicalOp, TestOperators}
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.workflow.LogicalLink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

/**
  * E2E reconfiguration tests that spawn Python UDF workers. Routed to the
  * `amber-integration` CI job via the class-level `@IntegrationTest` tag,
  * which provisions Python deps; the lighter `amber` job excludes this tag.
  *
  * Pure-Scala reconfiguration tests live in [[ReconfigurationSpec]] and run
  * in the regular `amber` job.
  */
@IntegrationTest
class ReconfigurationIntegrationSpec
    extends TestKit(ActorSystem("ReconfigurationIntegrationSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Retries {

  /**
    * This block retries each test once if it fails.
    * In the CI environment, there is a chance that executeWorkflow does not receive "COMPLETED" status.
    * Until we find the root cause of this issue, we use a retry mechanism here to stabilize CI runs.
    */
  override def withFixture(test: NoArgTest): Outcome =
    withRetry { super.withFixture(test) }

  implicit val timeout: Timeout = Timeout(5.seconds)

  val logger = Logger("ReconfigurationIntegrationSpecLogger")
  private val specId = 4
  val ctx = TestUtils.workflowContext(specId)

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
    * Runs a TextInput -> Python UDF workflow once before the timed tests so
    * Python worker cold-start is paid here, not inside a timed test. Capped and
    * wrapped so warmup can never fail or hang the suite.
    */
  private def warmupOnce(): Unit = {
    val warmupCap = Duration.fromSeconds(60)
    setUpWorkflowExecutionData(specId)
    var client: AmberClient = null
    try {
      val src = new TextInputSourceOpDesc()
      src.textInput = "warmup"
      val udf = TestOperators.pythonOpDesc()
      val warmupCtx = TestUtils.workflowContext(specId)
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
        warmupCtx
      )
      client = new AmberClient(
        system,
        workflow.context,
        workflow.physicalPlan,
        CoordinatorConfig.default,
        _ => {}
      )
      val completion = Promise[Unit]()
      client.registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED) completion.updateIfEmpty(Return(()))
      })
      Await.result(
        client.coordinatorInterface.startWorkflow(EmptyRequest(), ()),
        warmupCap
      )
      Await.result(completion, warmupCap)
    } catch {
      case e: Throwable =>
        logger.warn(
          s"warmup workflow did not finish within ${warmupCap}; tests will run cold and rely on Retries: ${e.getMessage}"
        )
    } finally {
      if (client != null) {
        try client.shutdown()
        catch { case _: Throwable => () }
      }
      cleanupWorkflowExecutionData(specId)
    }
  }

  // Thin wrapper around the shared TestUtils helper so call sites below stay
  // ctx/system-implicit. The actual workflow-driver logic lives in TestUtils
  // and is reused by ReconfigurationSpec.
  def shouldReconfigure(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      targetOps: Seq[LogicalOp],
      newOpExecInitInfo: OpExecInitInfo
  ): Map[OperatorIdentity, List[Tuple]] =
    TestUtils.shouldReconfigure(system, ctx, operators, links, targetOps, newOpExecInitInfo)

  // Small source that emits slowly (30 rows, 0.25s apart) so a pause lands
  // mid-run and the workflow still completes quickly after resume.
  private def slowSource() =
    TestOperators.slowRegionSourceOpDesc(numTuple = 30, delaySeconds = 0.25)

  "Engine" should "be able to modify a python UDF worker in workflow" in {
    val sourceOpDesc = slowSource()
    val udfOpDesc = TestOperators.pythonOpDesc()
    val code = """
                 |from pytexera import *
                 |
                 |class ProcessTupleOperator(UDFOperatorV2):
                 |    @overrides
                 |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                 |        tuple_['Region'] = tuple_['Region'] + '_reconfigured'
                 |        yield tuple_
                 |""".stripMargin

    val result = shouldReconfigure(
      List(sourceOpDesc, udfOpDesc),
      List(
        LogicalLink(
          sourceOpDesc.operatorIdentifier,
          PortIdentity(),
          udfOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      Seq(udfOpDesc),
      OpExecWithCode(code, "python")
    )
    assert(result(udfOpDesc.operatorIdentifier).exists { t =>
      t.getField("Region").asInstanceOf[String].contains("_reconfigured")
    })
  }

  "Engine" should "propagate reconfiguration through a source operator in workflow" in {
    val sourceOpDesc = slowSource()
    val udfOpDesc = TestOperators.pythonOpDesc()
    val code = """
                 |from pytexera import *
                 |
                 |class ProcessTupleOperator(UDFOperatorV2):
                 |    @overrides
                 |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                 |        tuple_['Region'] = tuple_['Region'] + '_reconfigured'
                 |        yield tuple_
                 |""".stripMargin
    val result = shouldReconfigure(
      List(sourceOpDesc, udfOpDesc),
      List(
        LogicalLink(
          sourceOpDesc.operatorIdentifier,
          PortIdentity(),
          udfOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      Seq(udfOpDesc),
      OpExecWithCode(code, "python")
    )
    assert(result(udfOpDesc.operatorIdentifier).exists { t =>
      t.getField("Region").asInstanceOf[String].contains("_reconfigured")
    })
  }

  "Engine" should "be able to modify two python UDFs in workflow" in {
    val sourceOpDesc = slowSource()
    val udfOpDesc1 = TestOperators.pythonOpDesc()
    val udfOpDesc2 = TestOperators.pythonOpDesc()
    val code = """
                 |from pytexera import *
                 |
                 |class ProcessTupleOperator(UDFOperatorV2):
                 |    @overrides
                 |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                 |        tuple_['Region'] = tuple_['Region'] + '_reconfigured'
                 |        yield tuple_
                 |""".stripMargin

    val result = shouldReconfigure(
      List(sourceOpDesc, udfOpDesc1, udfOpDesc2),
      List(
        LogicalLink(
          sourceOpDesc.operatorIdentifier,
          PortIdentity(),
          udfOpDesc1.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          udfOpDesc1.operatorIdentifier,
          PortIdentity(),
          udfOpDesc2.operatorIdentifier,
          PortIdentity()
        )
      ),
      Seq(udfOpDesc1, udfOpDesc2),
      OpExecWithCode(code, "python")
    )
    assert(result(udfOpDesc2.operatorIdentifier).exists { t =>
      t.getField("Region").asInstanceOf[String].contains("_reconfigured_reconfigured")
    })
  }

}
