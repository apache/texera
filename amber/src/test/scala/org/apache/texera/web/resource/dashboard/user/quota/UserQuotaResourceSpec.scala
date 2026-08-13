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

package org.apache.texera.web.resource.dashboard.user.quota

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  DatasetDao,
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowOfUser,
  WorkflowUserAccess,
  WorkflowVersion
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import java.util.UUID

class UserQuotaResourceSpec extends AnyFlatSpec with BeforeAndAfterAll with MockTexeraDB {

  private val testUid = 4000 + scala.util.Random.nextInt(1000)
  private val testWid = 5000 + scala.util.Random.nextInt(1000)

  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _
  private var datasetDao: DatasetDao = _

  private var testUser: User = _
  private var testWorkflow: Workflow = _
  private var testVersion: WorkflowVersion = _
  private val resource = new UserQuotaResource

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())
    workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
    datasetDao = new DatasetDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = shutdownDB()

  // Each test starts from a clean slate for the ids under test, then seeds
  // only what it needs, so tests are order-independent.
  private def resetFixtures(): Unit = {
    cleanupTestData()

    testUser = new User
    testUser.setUid(testUid)
    testUser.setName("quota_user")
    testUser.setEmail("quota@example.com")
    userDao.insert(testUser)

    testWorkflow = new Workflow
    testWorkflow.setWid(testWid)
    testWorkflow.setName("quota_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    testWorkflow.setContent("{}")
    testWorkflow.setDescription("desc")
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(testWorkflow)

    testVersion = new WorkflowVersion
    testVersion.setWid(testWid)
    testVersion.setContent("{}")
    testVersion.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflowVersionDao.insert(testVersion)
  }

  private def cleanupTestData(): Unit = {
    val vidSubquery = getDSLContext
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWid))

    getDSLContext
      .deleteFrom(OPERATOR_PORT_EXECUTIONS)
      .where(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(
          getDSLContext
            .select(WORKFLOW_EXECUTIONS.EID)
            .from(WORKFLOW_EXECUTIONS)
            .where(WORKFLOW_EXECUTIONS.UID.eq(testUid))
        )
      )
      .execute()
    getDSLContext
      .deleteFrom(OPERATOR_EXECUTIONS)
      .where(
        OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(
          getDSLContext
            .select(WORKFLOW_EXECUTIONS.EID)
            .from(WORKFLOW_EXECUTIONS)
            .where(WORKFLOW_EXECUTIONS.UID.eq(testUid))
        )
      )
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.VID.in(vidSubquery))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW_VERSION).where(WORKFLOW_VERSION.WID.eq(testWid)).execute()
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.UID.eq(testUid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW_OF_USER).where(WORKFLOW_OF_USER.UID.eq(testUid)).execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
    getDSLContext.deleteFrom(DATASET).where(DATASET.OWNER_UID.eq(testUid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.eq(testUid)).execute()
  }

  // ─── helpers ──────────────────────────────────────────────────────────────

  private def insertOwnership(): Unit = {
    val ownership = new WorkflowOfUser
    ownership.setUid(testUid)
    ownership.setWid(testWid)
    workflowOfUserDao.insert(ownership)
  }

  private def insertAccess(): Unit = {
    val access = new WorkflowUserAccess
    access.setUid(testUid)
    access.setWid(testWid)
    access.setPrivilege(PrivilegeEnum.READ)
    workflowUserAccessDao.insert(access)
  }

  private def insertExecution(): WorkflowExecutions = {
    val execution = new WorkflowExecutions
    execution.setVid(testVersion.getVid)
    execution.setUid(testUid)
    execution.setStatus(0.toByte)
    execution.setResult("")
    execution.setLogLocation("")
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("exec-" + UUID.randomUUID().toString.substring(0, 8))
    execution.setEnvironmentVersion("test-env-1.0")
    workflowExecutionsDao.insert(execution)
    execution
  }

  private def insertDataset(): Dataset = {
    val dataset = new Dataset
    dataset.setOwnerUid(testUid)
    dataset.setName("quota_ds_" + UUID.randomUUID().toString.substring(0, 8))
    dataset.setRepositoryName("repo-" + UUID.randomUUID().toString.substring(0, 8))
    dataset.setIsPublic(false)
    dataset.setIsDownloadable(true)
    dataset.setDescription("")
    dataset.setCreationTime(new Timestamp(System.currentTimeMillis()))
    datasetDao.insert(dataset)
    dataset
  }

  // ─── getUserCreatedWorkflow ────────────────────────────────────────────────

  "getUserCreatedWorkflow" should "return an empty list when the user owns no workflows" in {
    resetFixtures()
    assert(UserQuotaResource.getUserCreatedWorkflow(testUid).isEmpty)
  }

  it should "return the workflows owned by the user with their names and timestamps" in {
    resetFixtures()
    insertOwnership()

    val workflows = UserQuotaResource.getUserCreatedWorkflow(testUid)

    assert(workflows.size == 1)
    val w = workflows.head
    assert(w.userId == testUid)
    assert(w.workflowId == testWid)
    assert(w.workflowName == testWorkflow.getName)
    // Compare against the DB round-tripped value: Postgres rounds the stored
    // timestamp, so the in-memory pojo's millis would not match exactly.
    assert(w.creationTime == workflowDao.fetchOneByWid(testWid).getCreationTime.getTime)
  }

  // ─── getUserAccessedWorkflow ───────────────────────────────────────────────

  "getUserAccessedWorkflow" should "return an empty list when the user has no access grants" in {
    resetFixtures()
    assert(UserQuotaResource.getUserAccessedWorkflow(testUid).isEmpty)
  }

  it should "return the workflow ids the user can access" in {
    resetFixtures()
    insertAccess()

    val accessed = UserQuotaResource.getUserAccessedWorkflow(testUid)

    assert(accessed.size == 1)
    assert(accessed.get(0) == testWid)
  }

  // ─── getUserQuotaSize ──────────────────────────────────────────────────────

  "getUserQuotaSize" should "return an empty array when the user has no executions" in {
    resetFixtures()
    assert(UserQuotaResource.getUserQuotaSize(testUid).isEmpty)
  }

  it should "assemble per-execution result / runtime-stats / log sizes" in {
    resetFixtures()
    val execution = insertExecution()
    getDSLContext
      .update(WORKFLOW_EXECUTIONS)
      .set(WORKFLOW_EXECUTIONS.RUNTIME_STATS_SIZE, java.lang.Long.valueOf(200L))
      .where(WORKFLOW_EXECUTIONS.EID.eq(execution.getEid))
      .execute()
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .columns(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI,
        OPERATOR_PORT_EXECUTIONS.RESULT_SIZE
      )
      .values(execution.getEid, "gp-1", "vfs:///r", java.lang.Long.valueOf(100L))
      .execute()
    getDSLContext
      .insertInto(OPERATOR_EXECUTIONS)
      .columns(
        OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_EXECUTIONS.OPERATOR_ID,
        OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_URI,
        OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_SIZE
      )
      .values(execution.getEid, "op-1", "vfs:///c", java.lang.Long.valueOf(50L))
      .execute()

    val quota = UserQuotaResource.getUserQuotaSize(testUid)

    assert(quota.length == 1)
    val q = quota.head
    assert(q.eid == execution.getEid)
    assert(q.workflowId == testWid)
    assert(q.workflowName == testWorkflow.getName)
    assert(q.resultBytes == 100L)
    assert(q.runTimeStatsBytes == 200L)
    assert(q.logBytes == 50L)
  }

  // ─── class endpoints (delegate to the object using the SessionUser's uid) ──

  "getCreatedWorkflow (endpoint)" should "return the current user's created workflows" in {
    resetFixtures()
    insertOwnership()

    val workflows = resource.getCreatedWorkflow(new SessionUser(testUser))

    assert(workflows.size == 1)
    assert(workflows.head.workflowId == testWid)
  }

  "getAccessedWorkflow (endpoint)" should "return the current user's accessible workflow ids" in {
    resetFixtures()
    insertAccess()

    val accessed = resource.getAccessedWorkflow(new SessionUser(testUser))

    assert(accessed.size == 1)
    assert(accessed.get(0) == testWid)
  }

  "getUserQuota (endpoint)" should "return the current user's quota storage array" in {
    resetFixtures()
    insertExecution()

    val quota = resource.getUserQuota(new SessionUser(testUser))

    assert(quota.length == 1)
    assert(quota.head.workflowId == testWid)
  }

  "getCreatedDatasets (endpoint)" should "return an empty list when the user owns no datasets" in {
    resetFixtures()
    assert(resource.getCreatedDatasets(new SessionUser(testUser)).isEmpty)
  }

  it should "return the datasets the user created (with size disabled to 0)" in {
    resetFixtures()
    val dataset = insertDataset()

    val datasets = resource.getCreatedDatasets(new SessionUser(testUser))

    assert(datasets.size == 1)
    assert(datasets.head.name == dataset.getName)
    assert(datasets.head.size == 0L)
  }

  // ─── deleteExecutionCollection / deleteCollection ──────────────────────────

  "deleteExecutionCollection" should "delete the execution row from WORKFLOW_EXECUTIONS" in {
    resetFixtures()
    val execution = insertExecution()

    UserQuotaResource.deleteExecutionCollection(execution.getEid)

    assert(workflowExecutionsDao.fetchOneByEid(execution.getEid) == null)
  }

  "deleteCollection (endpoint)" should "delete the execution row for the given eid" in {
    resetFixtures()
    val execution = insertExecution()

    resource.deleteCollection(execution.getEid)

    assert(workflowExecutionsDao.fetchOneByEid(execution.getEid) == null)
  }
}
