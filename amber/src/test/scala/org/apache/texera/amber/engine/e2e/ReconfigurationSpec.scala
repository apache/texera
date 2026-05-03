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
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.executor.{OpExecInitInfo, OpExecWithCode}
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.controller.{
  ControllerConfig,
  ExecutionStateUpdate
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  EmptyRequest,
  UpdateExecutorRequest,
  WorkflowReconfigureRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  PAUSED
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.e2e.TestUtils.{
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  setUpWorkflowExecutionData,
  stateReached
}
import org.apache.texera.amber.operator.{LogicalOp, TestOperators}
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource.getResultUriByLogicalPortId
import org.apache.texera.workflow.LogicalLink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

class ReconfigurationSpec
    extends TestKit(ActorSystem("ReconfigurationSpec", AmberRuntime.akkaConfig))
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

  val logger = Logger("ReconfigurationSpecLogger")
  val ctx = new WorkflowContext()

  override protected def beforeEach(): Unit = {
    setUpWorkflowExecutionData()
  }

  override protected def afterEach(): Unit = {
    cleanupWorkflowExecutionData()
  }

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    // These test cases access postgres in CI, but occasionally the jdbc driver cannot be found during CI run.
    // Explicitly load the JDBC driver to avoid flaky CI failures.
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def shouldReconfigure(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      targetOps: Seq[LogicalOp],
      newOpExecInitInfo: OpExecInitInfo
  ): Map[OperatorIdentity, List[Tuple]] = {
    val workflow =
      TestUtils.buildWorkflow(operators, links, ctx)
    val client =
      new AmberClient(
        system,
        workflow.context,
        workflow.physicalPlan,
        ControllerConfig.default,
        error => {}
      )
    val completion = Promise[Unit]()
    var result: Map[OperatorIdentity, List[Tuple]] = null
    client
      .registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED) {
          result = workflow.logicalPlan.getTerminalOperatorIds
            .filter(terminalOpId => {
              val uri = getResultUriByLogicalPortId(
                workflow.context.executionId,
                terminalOpId,
                PortIdentity()
              )
              uri.nonEmpty
            })
            .map(terminalOpId => {
              val uri = getResultUriByLogicalPortId(
                workflow.context.executionId,
                terminalOpId,
                PortIdentity()
              ).get
              terminalOpId -> DocumentFactory
                .openDocument(uri)
                ._1
                .asInstanceOf[VirtualDocument[Tuple]]
                .get()
                .toList
            })
            .toMap
          completion.setDone()
        }
      })
    Await.result(
      client.controllerInterface.startWorkflow(EmptyRequest(), ()),
      Duration.fromSeconds(5)
    )
    val pausedReached = stateReached(client, PAUSED)
    Await.result(
      client.controllerInterface.pauseWorkflow(EmptyRequest(), ()),
      Duration.fromSeconds(5)
    )
    Await.result(pausedReached, Duration.fromSeconds(10))
    val physicalOps = targetOps.flatMap(op =>
      workflow.physicalPlan.getPhysicalOpsOfLogicalOp(op.operatorIdentifier)
    )
    Await.result(
      client.controllerInterface.reconfigureWorkflow(
        WorkflowReconfigureRequest(
          reconfiguration = physicalOps.map(op => UpdateExecutorRequest(op.id, newOpExecInitInfo)),
          reconfigurationId = "test-reconfigure-1"
        ),
        ()
      ),
      Duration.fromSeconds(5)
    )
    Await.result(
      client.controllerInterface.resumeWorkflow(EmptyRequest(), ()),
      Duration.fromSeconds(5)
    )
    Await.result(completion, Duration.fromMinutes(1))
    result
  }

  "Engine" should "be able to modify a python UDF worker in workflow" in {
    val sourceOpDesc = TestOperators.smallCsvScanOpDesc()
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

  "Engine" should "be able to modify a java operator in workflow" in {
    val sourceOpDesc = TestOperators.mediumCsvScanOpDesc()
    val keywordMatchNoneOpDesc = TestOperators.keywordSearchOpDesc("Region", "ShouldMatchNone")
    val keywordMatchManyOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val result = shouldReconfigure(
      List(sourceOpDesc, keywordMatchNoneOpDesc),
      List(
        LogicalLink(
          sourceOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordMatchNoneOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      Seq(keywordMatchNoneOpDesc),
      keywordMatchManyOpDesc.getPhysicalOp(ctx.workflowId, ctx.executionId).opExecInitInfo
    )
    assert(result(keywordMatchNoneOpDesc.operatorIdentifier).nonEmpty)
  }

  "Engine" should "not be able to modify a source operator in workflow" in {
    val sourceOpDesc = TestOperators.mediumCsvScanOpDesc()
    val sourceOpDesc2 = TestOperators.mediumCsvScanOpDesc()
    val keywordMatchNoneOpDesc = TestOperators.keywordSearchOpDesc("Region", "ShouldMatchNone")
    val ex = intercept[Throwable] {
      shouldReconfigure(
        List(sourceOpDesc, keywordMatchNoneOpDesc),
        List(
          LogicalLink(
            sourceOpDesc.operatorIdentifier,
            PortIdentity(),
            keywordMatchNoneOpDesc.operatorIdentifier,
            PortIdentity()
          )
        ),
        Seq(sourceOpDesc),
        sourceOpDesc2.getPhysicalOp(ctx.workflowId, ctx.executionId).opExecInitInfo
      )
    }
    assert(
      ex.getMessage == "java.lang.IllegalStateException: Reconfiguration cannot be applied to source operators"
    )
  }

  "Engine" should "propagate reconfiguration through a source operator in workflow" in {
    val sourceOpDesc = TestOperators.pythonSourceOpDesc(10000)
    val udfOpDesc = TestOperators.pythonOpDesc()
    val code = """
                 |from pytexera import *
                 |
                 |class ProcessTupleOperator(UDFOperatorV2):
                 |    @overrides
                 |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                 |        tuple_['field_1'] = tuple_['field_1'] + '_reconfigured'
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
      t.getField("field_1").asInstanceOf[String].contains("_reconfigured")
    })
  }

  "Engine" should "be able to modify two python UDFs in workflow" in {
    val sourceOpDesc = TestOperators.smallCsvScanOpDesc()
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
