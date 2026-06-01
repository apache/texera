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

package org.apache.texera.web.service

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.common.Utils.maptoStatusCode
import org.apache.texera.amber.engine.common.executionruntimestate.ExecutionMetadataStore
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowComputingUnitDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowComputingUnit,
  WorkflowExecutions,
  WorkflowVersion
}
import org.apache.texera.web.storage.ExecutionStateStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp
import java.util.UUID

class ExecutionsMetadataPersistServiceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Randomise the seeded wid/uid so a parallel run of unrelated specs that
  // happen to seed the same ids wouldn't collide on the embedded postgres.
  private val testWid = 7000 + scala.util.Random.nextInt(1000)
  private val testUid = 7000 + scala.util.Random.nextInt(1000)

  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _
  private var userDao: UserDao = _
  private var computingUnitDao: WorkflowComputingUnitDao = _
  private var seededVid: Integer = _
  private var seededCuid: Integer = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  override protected def beforeEach(): Unit = {
    val cfg = getDSLContext.configuration()
    workflowDao = new WorkflowDao(cfg)
    workflowVersionDao = new WorkflowVersionDao(cfg)
    workflowExecutionsDao = new WorkflowExecutionsDao(cfg)
    userDao = new UserDao(cfg)
    computingUnitDao = new WorkflowComputingUnitDao(cfg)
    cleanup()

    val user = new User
    user.setUid(testUid)
    user.setName("metadata_persist_spec_user")
    user.setEmail(s"user_${UUID.randomUUID()}@example.com")
    user.setPassword("password")
    userDao.insert(user)

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName(s"wf_${UUID.randomUUID().toString.substring(0, 8)}")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(workflow)

    // Seed one version explicitly so insertNewExecution's getLatestVersion
    // takes the happy "max(existing)" branch instead of falling into the
    // back-compat "insert one for you" branch (which would drag in
    // WorkflowVersionResource.insertNewVersion's diff/aggregate logic).
    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflowVersionDao.insert(version)
    seededVid = version.getVid

    // workflow_executions.cuid has a FK to workflow_computing_unit; seed one
    // so insertNewExecution's setCuid(...) call doesn't trip the constraint.
    val cu = new WorkflowComputingUnit
    cu.setUid(testUid)
    cu.setName("test-cu")
    computingUnitDao.insert(cu)
    seededCuid = cu.getCuid
  }

  override protected def afterEach(): Unit = cleanup()

  private def cleanup(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(
        WORKFLOW_EXECUTIONS.VID.in(
          getDSLContext
            .select(WORKFLOW_VERSION.VID)
            .from(WORKFLOW_VERSION)
            .where(WORKFLOW_VERSION.WID.eq(testWid))
        )
      )
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
    getDSLContext
      .deleteFrom(WORKFLOW_COMPUTING_UNIT)
      .where(WORKFLOW_COMPUTING_UNIT.UID.eq(testUid))
      .execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.eq(testUid)).execute()
  }

  // Helper: insert an execution row tied to the seeded version and return its eid.
  private def seedExecution(status: Byte = 0): Integer = {
    val exec = new WorkflowExecutions
    exec.setVid(seededVid)
    exec.setUid(testUid)
    exec.setStatus(status)
    exec.setResult("")
    exec.setStartingTime(new Timestamp(System.currentTimeMillis()))
    exec.setBookmarked(false)
    exec.setName("seeded execution")
    exec.setEnvironmentVersion("test-env")
    workflowExecutionsDao.insert(exec)
    exec.getEid
  }

  // -- insertNewExecution -----------------------------------------------------

  "insertNewExecution" should "insert a row tied to the latest workflow version" in {
    val id = ExecutionsMetadataPersistService.insertNewExecution(
      WorkflowIdentity(testWid.toLong),
      testUid,
      executionName = "named-execution",
      environmentVersion = "env-1",
      computingUnitId = seededCuid
    )
    id should not be ExecutionIdentity(0L)

    val stored = workflowExecutionsDao.fetchOneByEid(id.id.toInt)
    stored should not be null
    stored.getVid shouldBe seededVid
    stored.getUid shouldBe testUid
    stored.getName shouldBe "named-execution"
    stored.getEnvironmentVersion shouldBe "env-1"
    stored.getCuid shouldBe seededCuid
  }

  it should "skip setName when executionName is the empty string" in {
    val id = ExecutionsMetadataPersistService.insertNewExecution(
      WorkflowIdentity(testWid.toLong),
      testUid,
      executionName = "",
      environmentVersion = "env-2",
      computingUnitId = seededCuid
    )
    val stored = workflowExecutionsDao.fetchOneByEid(id.id.toInt)
    // The DDL default for workflow_executions.name is 'Untitled Execution'
    // (sql/texera_ddl.sql). The production code path explicitly does not
    // call setName for an empty string, so the row should fall back to the
    // DDL default rather than persisting "".
    stored.getName shouldBe "Untitled Execution"
  }

  it should "let the DB NOT NULL constraint reject a null uid (surfaced as a readable error)" in {
    // No more pre-insert require: a null uid is passed straight to jOOQ and the
    // DB's NOT NULL constraint rejects it. insertNewExecution catches the
    // resulting DataAccessException (SQLState 23502) and rethrows a readable
    // IllegalArgumentException. Assert both the readable message and that the
    // original DB exception is preserved as the cause, and that nothing was
    // written.
    val before = workflowExecutionsDao.fetchByVid(seededVid).size()

    val ex = intercept[IllegalArgumentException] {
      ExecutionsMetadataPersistService.insertNewExecution(
        WorkflowIdentity(testWid.toLong),
        null, // uid is NOT NULL in the DB
        executionName = "anonymous",
        environmentVersion = "env-3",
        computingUnitId = seededCuid
      )
    }
    ex.getMessage should include("uid")
    ex.getCause shouldBe a[org.jooq.exception.DataAccessException]

    // The failed insert must not have persisted a row.
    workflowExecutionsDao.fetchByVid(seededVid).size() shouldBe before
  }

  // -- tryGetExistingExecution ------------------------------------------------

  "tryGetExistingExecution" should "return Some(row) for a known eid" in {
    val eid = seedExecution()
    val fetched = ExecutionsMetadataPersistService.tryGetExistingExecution(
      ExecutionIdentity(eid.longValue())
    )
    fetched shouldBe defined
    fetched.get.getEid shouldBe eid
  }

  it should "return None for an unknown eid" in {
    val fetched = ExecutionsMetadataPersistService.tryGetExistingExecution(
      ExecutionIdentity(-1L)
    )
    fetched shouldBe None
  }

  // -- tryUpdateExistingExecution ---------------------------------------------

  "tryUpdateExistingExecution" should "apply the update function to the stored row" in {
    val eid = seedExecution(status = 0)
    ExecutionsMetadataPersistService.tryUpdateExistingExecution(
      ExecutionIdentity(eid.longValue())
    ) { exec =>
      exec.setStatus(2.toByte) // PAUSED
      exec.setName("renamed via update")
    }
    val after = workflowExecutionsDao.fetchOneByEid(eid)
    after.getStatus shouldBe 2.toByte
    after.getName shouldBe "renamed via update"
  }

  it should "swallow the update error for an unknown eid and leave existing rows untouched" in {
    // Pin the silent-failure contract: fetchOneByEid returns null, the
    // update closure NPEs on it, the catch logs "Unable to update execution"
    // and continues. The seeded row stays untouched.
    val eid = seedExecution(status = 1)
    noException should be thrownBy
      ExecutionsMetadataPersistService.tryUpdateExistingExecution(
        ExecutionIdentity(-1L)
      ) { exec => exec.setStatus(9.toByte) }
    val after = workflowExecutionsDao.fetchOneByEid(eid)
    after.getStatus shouldBe 1.toByte
  }

  // -- ExecutionStateStore.updateWorkflowState --------------------------------
  //
  // updateWorkflowState wraps tryUpdateExistingExecution to also bump the
  // in-memory ExecutionMetadataStore's `state`. Exercising it here keeps the
  // DB-backed setup (workflow/version/execution rows) in one place; the
  // pure-logic ExecutionStateStoreSpec sibling has no DB dependency.

  "ExecutionStateStore.updateWorkflowState" should "set status via maptoStatusCode and return the metadata store with new state" in {
    val eid = seedExecution(status = 0)
    val before = workflowExecutionsDao.fetchOneByEid(eid)
    val beforeTs = before.getLastUpdateTime
    val store = ExecutionMetadataStore(
      state = WorkflowAggregatedState.UNINITIALIZED,
      executionId = ExecutionIdentity(eid.longValue())
    )
    val updated =
      ExecutionStateStore.updateWorkflowState(WorkflowAggregatedState.COMPLETED, store)
    updated.state shouldBe WorkflowAggregatedState.COMPLETED

    val after = workflowExecutionsDao.fetchOneByEid(eid)
    after.getStatus shouldBe maptoStatusCode(WorkflowAggregatedState.COMPLETED)
    // lastUpdateTime is set unconditionally to System.currentTimeMillis().
    // Asserting it advanced past the seeded null/older value catches a
    // regression that drops the setLastUpdateTime call.
    Option(beforeTs) match {
      case Some(t) => after.getLastUpdateTime.getTime should be >= t.getTime
      case None    => after.getLastUpdateTime should not be null
    }
  }

  it should "still return a metadataStore with the new state when the eid is unknown" in {
    // updateWorkflowState first calls tryUpdateExistingExecution (which
    // silently swallows the unknown-eid error) and then unconditionally
    // returns metadataStore.withState(state). Document this so a future
    // refactor that makes the failure surface (e.g. via Try / Either) has
    // a spec to migrate.
    val store = ExecutionMetadataStore(
      state = WorkflowAggregatedState.UNINITIALIZED,
      executionId = ExecutionIdentity(-1L)
    )
    val updated =
      ExecutionStateStore.updateWorkflowState(WorkflowAggregatedState.FAILED, store)
    updated.state shouldBe WorkflowAggregatedState.FAILED
  }
}
