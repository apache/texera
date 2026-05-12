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
import org.apache.pekko.actor.ActorSystem
import org.apache.texera.amber.config.StorageConfig
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.controller.{
  ControllerConfig,
  ExecutionStateUpdate,
  Workflow
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  EmptyRequest,
  UpdateExecutorRequest,
  WorkflowReconfigureRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  PAUSED
}
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  WorkflowExecutions,
  WorkflowVersion,
  Workflow => WorkflowPojo
}
import org.apache.texera.web.model.websocket.request.LogicalPlanPojo
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource.getResultUriByLogicalPortId
import org.apache.texera.workflow.{LogicalLink, WorkflowCompiler}

object TestUtils {

  def buildWorkflow(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      context: WorkflowContext
  ): Workflow = {
    val workflowCompiler = new WorkflowCompiler(
      context
    )
    workflowCompiler.compile(
      LogicalPlanPojo(operators, links, List(), List())
    )
  }

  /**
    * If a test case accesses the user system through singleton resources that cache the DSLContext (e.g., executes a
    * workflow, which accesses WorkflowExecutionsResource), we use a separate texera_db specifically for such test cases.
    * Note such test cases need to clean up the database at the end of running each test case.
    */
  def initiateTexeraDBForTestCases(): Unit = {
    SqlServer.initConnection(
      StorageConfig.jdbcUrlForTestCases,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )
  }

  val testUser: User = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRoleEnum.ADMIN)
    user.setPassword("123")
    user.setEmail("test_user@test.com")
    user
  }

  val testWorkflowEntry: WorkflowPojo = {
    val workflow = new WorkflowPojo
    workflow.setName("test workflow")
    workflow.setWid(Integer.valueOf(1))
    workflow.setContent("test workflow content")
    workflow.setDescription("test description")
    workflow
  }

  val testWorkflowVersionEntry: WorkflowVersion = {
    val workflowVersion = new WorkflowVersion
    workflowVersion.setWid(Integer.valueOf(1))
    workflowVersion.setVid(Integer.valueOf(1))
    workflowVersion.setContent("test version content")
    workflowVersion
  }

  val testWorkflowExecutionEntry: WorkflowExecutions = {
    val workflowExecution = new WorkflowExecutions
    workflowExecution.setEid(Integer.valueOf(1))
    workflowExecution.setVid(Integer.valueOf(1))
    workflowExecution.setUid(Integer.valueOf(1))
    workflowExecution.setStatus(3.toByte)
    workflowExecution.setEnvironmentVersion("test engine")
    workflowExecution
  }

  def setUpWorkflowExecutionData(): Unit = {
    val dslConfig = SqlServer.getInstance().context.configuration()
    val userDao = new UserDao(dslConfig)
    val workflowDao = new WorkflowDao(dslConfig)
    val workflowExecutionsDao = new WorkflowExecutionsDao(dslConfig)
    val workflowVersionDao = new WorkflowVersionDao(dslConfig)
    userDao.insert(testUser)
    workflowDao.insert(testWorkflowEntry)
    workflowVersionDao.insert(testWorkflowVersionEntry)
    workflowExecutionsDao.insert(testWorkflowExecutionEntry)
  }

  /**
    * Returns a Promise that completes the next time the client emits an
    * ExecutionStateUpdate with the given target state. Must be called BEFORE
    * the action that triggers the state change, since AmberClient observables
    * do not replay past events.
    */
  def stateReached(
      client: AmberClient,
      target: WorkflowAggregatedState
  ): Promise[Unit] = {
    val p = Promise[Unit]()
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == target) {
        p.updateIfEmpty(Return(()))
      }
    })
    p
  }

  /**
    * Pause a freshly-started workflow, swap the executor for the given target
    * operators via WorkflowReconfigureRequest, resume, and collect the
    * terminal-port outputs once the run completes. Shared by ReconfigurationSpec
    * (pure-Scala) and ReconfigurationIntegrationSpec (Python-tagged), so an
    * earlier in-spec copy doesn't drift between the two as new e2e specs
    * land. The caller passes its own `system` (TestKit) and `ctx`
    * (WorkflowContext) since both are tied to the spec lifecycle.
    */
  def shouldReconfigure(
      system: ActorSystem,
      ctx: WorkflowContext,
      operators: List[LogicalOp],
      links: List[LogicalLink],
      targetOps: Seq[LogicalOp],
      newOpExecInitInfo: OpExecInitInfo
  ): Map[OperatorIdentity, List[Tuple]] = {
    val workflow = buildWorkflow(operators, links, ctx)
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      ControllerConfig.default,
      error => {}
    )
    val completion = Promise[Unit]()
    var result: Map[OperatorIdentity, List[Tuple]] = null
    client.registerCallback[ExecutionStateUpdate](evt => {
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

  def cleanupWorkflowExecutionData(): Unit = {
    val dslConfig = SqlServer.getInstance().context.configuration()
    val userDao = new UserDao(dslConfig)
    val workflowDao = new WorkflowDao(dslConfig)
    val workflowExecutionsDao = new WorkflowExecutionsDao(dslConfig)
    val workflowVersionDao = new WorkflowVersionDao(dslConfig)
    workflowExecutionsDao.deleteById(1)
    workflowVersionDao.deleteById(1)
    workflowDao.deleteById(1)
    userDao.deleteById(1)
  }

}
