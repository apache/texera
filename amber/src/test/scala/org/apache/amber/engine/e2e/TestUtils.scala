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

package org.apache.amber.engine.e2e

import org.apache.amber.config.StorageConfig
import org.apache.amber.core.workflow.WorkflowContext
import org.apache.amber.engine.architecture.controller.Workflow
import org.apache.amber.operator.LogicalOp
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
