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

package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.twitter.util.{Await, Promise}
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.config.StorageConfig
import edu.uci.ics.amber.core.workflow.{PortIdentity, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionStateUpdate}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.operator.{LogicalOp, TestOperators}
import edu.uci.ics.texera.dao.{MockTexeraDB, SqlServer}
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowDao, WorkflowExecutionsDao, WorkflowVersionDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{User, WorkflowExecutions, WorkflowVersion, Workflow => WorkflowPojo}
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

class PauseSpec
    extends TestKit(ActorSystem("PauseSpec", AmberRuntime.akkaConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  implicit val timeout: Timeout = Timeout(5.seconds)

  val logger = Logger("PauseSpecLogger")

  private val testUser: User = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRoleEnum.ADMIN)
    user.setPassword("123")
    user.setEmail("test_user@test.com")
    user
  }

  private val testWorkflowEntry: WorkflowPojo = {
    val workflow = new WorkflowPojo
    workflow.setName("test workflow")
    workflow.setWid(Integer.valueOf(1))
    workflow.setContent("test workflow content")
    workflow.setDescription("test description")
    workflow
  }

  private val testWorkflowVersionEntry: WorkflowVersion = {
    val workflowVersion = new WorkflowVersion
    workflowVersion.setWid(Integer.valueOf(1))
    workflowVersion.setVid(Integer.valueOf(1))
    workflowVersion.setContent("test version content")
    workflowVersion
  }

  private val testWorkflowExecutionEntry: WorkflowExecutions = {
    val workflowExecution = new WorkflowExecutions
    workflowExecution.setEid(Integer.valueOf(1))
    workflowExecution.setVid(Integer.valueOf(1))
    workflowExecution.setUid(Integer.valueOf(1))
    workflowExecution.setStatus(3.toByte)
    workflowExecution.setEnvironmentVersion("test engine")
    workflowExecution
  }

  override protected def beforeEach(): Unit = {
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

  override protected def afterEach(): Unit = {
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

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    // These test cases access postgres in CI, but occasionally the jdbc driver cannot be found during CI run.
    // Explicitly load the JDBC driver to avoid flaky CI failures.
    Class.forName("org.postgresql.Driver")
    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def shouldPause(
      operators: List[LogicalOp],
      links: List[LogicalLink]
  ): Unit = {
    val workflow =
      TestUtils.buildWorkflow(operators, links, new WorkflowContext())
    val client =
      new AmberClient(
        system,
        workflow.context,
        workflow.physicalPlan,
        ControllerConfig.default,
        error => {}
      )
    val completion = Promise[Unit]()
    client
      .registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED) {
          completion.setDone()
        }
      })
    Await.result(client.controllerInterface.startWorkflow(EmptyRequest(), ()))
    Await.result(client.controllerInterface.pauseWorkflow(EmptyRequest(), ()))
    Thread.sleep(4000)
    Await.result(client.controllerInterface.resumeWorkflow(EmptyRequest(), ()))
    Thread.sleep(400)
    Await.result(client.controllerInterface.pauseWorkflow(EmptyRequest(), ()))
    Thread.sleep(4000)
    Await.result(client.controllerInterface.resumeWorkflow(EmptyRequest(), ()))
    Await.result(completion)
  }

  "Engine" should "be able to pause csv workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    logger.info(s"csv-id ${csvOpDesc.operatorIdentifier}")
    shouldPause(
      List(csvOpDesc),
      List()
    )
  }

  "Engine" should "be able to pause csv->keyword workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    logger.info(
      s"csv-id ${csvOpDesc.operatorIdentifier}, keyword-id ${keywordOpDesc.operatorIdentifier}"
    )
    shouldPause(
      List(csvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        )
      )
    )
  }

}
