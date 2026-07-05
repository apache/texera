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

import com.twitter.util.{Await, Duration, Promise, Return, Throw, Try}
import org.apache.pekko.actor.ActorSystem
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext, WorkflowSettings}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  ExecutionStateUpdate,
  FatalError,
  OperatorPortResultUriAvailable,
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
import org.apache.texera.web.service.ExecutionResultService
import org.apache.texera.workflow.{LogicalLink, WorkflowCompiler}

object TestUtils {

  /**
    * A WorkflowContext whose workflow- and execution-id are both `id`. Each e2e
    * suite passes a distinct id so its results land in a disjoint storage
    * keyspace (`vfs:///wid/{id}/eid/{id}/...`) and disjoint DB rows, letting the
    * suites run concurrently without colliding on the shared Iceberg catalog.
    */
  def workflowContext(
      id: Int,
      workflowSettings: WorkflowSettings = WorkflowSettings()
  ): WorkflowContext =
    new WorkflowContext(
      workflowId = WorkflowIdentity(id.toLong),
      executionId = ExecutionIdentity(id.toLong),
      workflowSettings = workflowSettings
    )

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
    * Resolve and read each operator's external RESULT document at `executionId`,
    * applying `extract` to the opened document. Operators with no external
    * RESULT uri (e.g. one whose output wasn't materialized) are omitted. Shared
    * by the e2e specs so the lookup-open-extract block doesn't drift between
    * copies.
    */
  def readMaterializedResults[T](
      executionId: ExecutionIdentity,
      operatorIds: Iterable[OperatorIdentity],
      extract: VirtualDocument[Tuple] => T
  ): Map[OperatorIdentity, T] =
    operatorIds.flatMap { opId =>
      getResultUriByLogicalPortId(executionId, opId, PortIdentity()).map { uri =>
        opId -> extract(
          DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]]
        )
      }
    }.toMap

  /**
    * Convenience over `readMaterializedResults` for the common case: read each
    * terminal operator's result of `workflow` as a `List[Tuple]`.
    */
  def readMaterializedResults(workflow: Workflow): Map[OperatorIdentity, List[Tuple]] =
    readMaterializedResults(
      workflow.context.executionId,
      workflow.logicalPlan.getTerminalOperatorIds,
      _.get().toList
    )

  /**
    * Run `workflow` to COMPLETED, then read the requested operators' materialized
    * results via `readMaterializedResults`. A FatalError aborts the run and is
    * surfaced as the exception from the completion await. Specs that drive the
    * run differently (e.g. a pause/resume flow) read results directly inside
    * their own completion callback instead.
    */
  def runWorkflowAndReadResults[T](
      system: ActorSystem,
      workflow: Workflow,
      operatorIds: Iterable[OperatorIdentity],
      extract: VirtualDocument[Tuple] => T,
      completionTimeout: Duration = Duration.fromMinutes(1)
  ): Map[OperatorIdentity, T] = {
    // The Promise carries the result so completing the run and handing back the
    // value are atomic. Every terminal path uses `updateIfEmpty`, so a second
    // event (a late FatalError after COMPLETED, or a repeated state update)
    // can't throw inside a callback and get swallowed -- which would otherwise
    // mask the real failure as a timeout. A read failure inside the COMPLETED
    // callback fails the Promise (via `Try`) instead of hanging, and
    // `shutdown()` runs in a `finally` so a timeout or error can't leak the
    // client's actors.
    val completion = Promise[Map[OperatorIdentity, T]]()
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      CoordinatorConfig.default,
      e => completion.updateIfEmpty(Throw(e))
    )
    try {
      client.registerCallback[FatalError](evt => completion.updateIfEmpty(Throw(evt.e)))
      // The engine emits `OperatorPortResultUriAvailable` for each
      // materialized output port; production wires this to a DB insert in
      // `ExecutionResultService.persistOperatorPortResultUri`. The e2e
      // harness doesn't construct an `ExecutionResultService` (it builds an
      // `AmberClient` directly), so register the same callback here so the
      // post-completion `readMaterializedResults` lookup via
      // `getResultUriByLogicalPortId` finds the rows.
      registerResultUriPersistence(client, workflow.context.executionId)
      client.registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED) {
          completion.updateIfEmpty(
            Try(readMaterializedResults(workflow.context.executionId, operatorIds, extract))
          )
        }
      })
      Await.result(client.coordinatorInterface.startWorkflow(EmptyRequest(), ()))
      Await.result(completion, completionTimeout)
    } finally {
      client.shutdown()
    }
  }

  /**
    * Mirror the production `OperatorPortResultUriAvailable` → DB write that
    * `ExecutionResultService.persistOperatorPortResultUri` does, but driven
    * from a test-owned `AmberClient`. Specs that build their own client
    * (the harness above, or `shouldReconfigure` for the pause/resume flow)
    * call this so subsequent `getResultUriByLogicalPortId` lookups succeed.
    */
  def registerResultUriPersistence(client: AmberClient, executionId: ExecutionIdentity): Unit =
    client.registerCallback[OperatorPortResultUriAvailable](evt =>
      ExecutionResultService.persistOperatorPortResultUri(executionId, evt)
    )

  /**
    * Convenience over `runWorkflowAndReadResults` for the common case: run
    * `workflow` and read each terminal operator's result as a `List[Tuple]`.
    */
  def runWorkflowAndReadTerminalResults(
      system: ActorSystem,
      workflow: Workflow,
      completionTimeout: Duration = Duration.fromMinutes(1)
  ): Map[OperatorIdentity, List[Tuple]] =
    runWorkflowAndReadResults(
      system,
      workflow,
      workflow.logicalPlan.getTerminalOperatorIds,
      _.get().toList,
      completionTimeout
    )

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

  // All fixture rows for one suite share `id` as uid/wid/vid/eid; the email is
  // derived from it so concurrent suites don't collide on the unique email key.
  def testUser(id: Int): User = {
    val user = new User
    user.setUid(Integer.valueOf(id))
    user.setName(s"test_user_$id")
    user.setRole(UserRoleEnum.ADMIN)
    user.setPassword("123")
    user.setEmail(s"test_user_$id@test.com")
    user
  }

  def testWorkflowEntry(id: Int): WorkflowPojo = {
    val workflow = new WorkflowPojo
    workflow.setName("test workflow")
    workflow.setWid(Integer.valueOf(id))
    workflow.setContent("test workflow content")
    workflow.setDescription("test description")
    workflow
  }

  def testWorkflowVersionEntry(id: Int): WorkflowVersion = {
    val workflowVersion = new WorkflowVersion
    workflowVersion.setWid(Integer.valueOf(id))
    workflowVersion.setVid(Integer.valueOf(id))
    workflowVersion.setContent("test version content")
    workflowVersion
  }

  def testWorkflowExecutionEntry(id: Int): WorkflowExecutions = {
    val workflowExecution = new WorkflowExecutions
    workflowExecution.setEid(Integer.valueOf(id))
    workflowExecution.setVid(Integer.valueOf(id))
    workflowExecution.setUid(Integer.valueOf(id))
    workflowExecution.setStatus(3.toByte)
    workflowExecution.setEnvironmentVersion("test engine")
    workflowExecution
  }

  def setUpWorkflowExecutionData(id: Int): Unit = {
    val dslConfig = SqlServer.getInstance().context.configuration()
    val userDao = new UserDao(dslConfig)
    val workflowDao = new WorkflowDao(dslConfig)
    val workflowExecutionsDao = new WorkflowExecutionsDao(dslConfig)
    val workflowVersionDao = new WorkflowVersionDao(dslConfig)
    userDao.insert(testUser(id))
    workflowDao.insert(testWorkflowEntry(id))
    workflowVersionDao.insert(testWorkflowVersionEntry(id))
    workflowExecutionsDao.insert(testWorkflowExecutionEntry(id))
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
      CoordinatorConfig.default,
      error => {}
    )
    // Timeout for control-command acks (start/pause/reconfigure/resume).
    val commandTimeout = Duration.fromSeconds(30)
    registerResultUriPersistence(client, workflow.context.executionId)
    val completion = Promise[Unit]()
    var result: Map[OperatorIdentity, List[Tuple]] = null
    client.registerCallback[ExecutionStateUpdate](evt => {
      if (evt.state == COMPLETED) {
        result = readMaterializedResults(workflow)
        completion.setDone()
      }
    })
    Await.result(
      client.coordinatorInterface.startWorkflow(EmptyRequest(), ()),
      commandTimeout
    )
    val pausedReached = stateReached(client, PAUSED)
    Await.result(
      client.coordinatorInterface.pauseWorkflow(EmptyRequest(), ()),
      commandTimeout
    )
    Await.result(pausedReached, commandTimeout)
    val physicalOps = targetOps.flatMap(op =>
      workflow.physicalPlan.getPhysicalOpsOfLogicalOp(op.operatorIdentifier)
    )
    Await.result(
      client.coordinatorInterface.reconfigureWorkflow(
        WorkflowReconfigureRequest(
          reconfiguration = physicalOps.map(op => UpdateExecutorRequest(op.id, newOpExecInitInfo)),
          reconfigurationId = "test-reconfigure-1"
        ),
        ()
      ),
      commandTimeout
    )
    Await.result(
      client.coordinatorInterface.resumeWorkflow(EmptyRequest(), ()),
      commandTimeout
    )
    Await.result(completion, Duration.fromMinutes(1))
    result
  }

  def cleanupWorkflowExecutionData(id: Int): Unit = {
    val dslConfig = SqlServer.getInstance().context.configuration()
    val userDao = new UserDao(dslConfig)
    val workflowDao = new WorkflowDao(dslConfig)
    val workflowExecutionsDao = new WorkflowExecutionsDao(dslConfig)
    val workflowVersionDao = new WorkflowVersionDao(dslConfig)
    workflowExecutionsDao.deleteById(id)
    workflowVersionDao.deleteById(id)
    workflowDao.deleteById(id)
    userDao.deleteById(id)
  }

}
