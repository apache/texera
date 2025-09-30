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

package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowVersion
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, PrivateMethodTester}

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

class WorkflowExecutionsResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB
    with PrivateMethodTester {

  private val testWorkflowWid = 3000 + scala.util.Random.nextInt(1000)
  private val testUserId = 1000 + scala.util.Random.nextInt(1000)

  private var testWorkflow: Workflow = _
  private var testVersion: WorkflowVersion = _
  private var testUser: User = _
  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    testUser = new User
    testUser.setUid(testUserId)
    testUser.setName("test_user")
    testUser.setEmail("test@example.com")
    testUser.setPassword("password")
    testUser.setGoogleAvatar("avatar_url")

    testWorkflow = new Workflow
    testWorkflow.setWid(testWorkflowWid)
    testWorkflow.setName("test_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    testWorkflow.setContent("{}")
    testWorkflow.setDescription("test description")
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))

    testVersion = new WorkflowVersion
    testVersion.setWid(testWorkflowWid)
    testVersion.setContent("{}")
    testVersion.setCreationTime(new Timestamp(System.currentTimeMillis()))

    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    userDao = new UserDao(getDSLContext.configuration())
    workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())

    cleanupTestData()

    userDao.insert(testUser)
    workflowDao.insert(testWorkflow)
    workflowVersionDao.insert(testVersion)
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  private def cleanupTestData(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(
        WORKFLOW_EXECUTIONS.VID.in(
          getDSLContext
            .select(WORKFLOW_VERSION.VID)
            .from(WORKFLOW_VERSION)
            .where(WORKFLOW_VERSION.WID.eq(testWorkflowWid))
        )
      )
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW)
      .where(WORKFLOW.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(USER)
      .where(USER.UID.eq(testUserId))
      .execute()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  "WorkflowExecutionsResource.getWorkflowExecutions" should "return executions with EIDs in descending order" in {
    val numExecutions = 10
    val executionIds = ArrayBuffer.empty[Integer]

    for (i <- 1 to numExecutions) {
      val execution = new WorkflowExecutions
      execution.setVid(testVersion.getVid)
      execution.setUid(testUser.getUid)
      execution.setStatus(0.toByte)
      execution.setResult("")
      execution.setStartingTime(
        new Timestamp(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(numExecutions - i))
      )
      execution.setBookmarked(false)
      execution.setName(s"Execution ${i}")
      execution.setEnvironmentVersion("test-env-1.0")

      workflowExecutionsDao.insert(execution)
      executionIds.append(execution.getEid)
    }

    val result = WorkflowExecutionsResource.getWorkflowExecutions(testWorkflowWid, getDSLContext)

    assert(result.nonEmpty, "Result should not be empty")
    assert(
      result.size == numExecutions,
      s"Expected $numExecutions executions, but got ${result.size}"
    )

    for (i <- 0 until result.size - 1) {
      assert(
        result(i).eId > result(i + 1).eId,
        s"Executions are not in descending order: ${result(i).eId} should be > ${result(i + 1).eId}"
      )
    }

    val returnedIds = result.map(_.eId).toSet
    assert(
      executionIds.toSet.subsetOf(returnedIds),
      "All inserted execution IDs should be returned"
    )
  }

}
