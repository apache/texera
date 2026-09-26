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

package org.apache.texera.service.resource

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{
  USER => USER_TABLE,
  WORKFLOW,
  WORKFLOW_COMPUTING_UNIT,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_VERSION
}
import org.apache.texera.dao.jooq.generated.enums.{
  WorkflowComputingUnitTerminationReasonEnum,
  WorkflowComputingUnitTypeEnum
}
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
import org.apache.texera.service.resource.ComputingUnitManagingResource.TerminatedComputingUnitInfo
import org.apache.texera.service.util.KubernetesClient
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{doThrow, mock, never, verify, verifyNoInteractions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

class ComputingUnitIdleCleanupSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testUserId = 810000 + scala.util.Random.nextInt(10000)
  private val testWorkflowId = 820000 + scala.util.Random.nextInt(10000)
  private val now = new Timestamp(TimeUnit.DAYS.toMillis(20))
  private val idleTimeoutMinutes = 60L

  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var workflowComputingUnitDao: WorkflowComputingUnitDao = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _
  private var testVersion: WorkflowVersion = _

  override protected def beforeAll(): Unit =
    initializeDBAndReplaceDSLContext()

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    workflowComputingUnitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())

    cleanupTestData()

    val user = new User
    user.setUid(testUserId)
    user.setName("idle-cu-owner")
    user.setEmail(s"idle-cu-${UUID.randomUUID()}@example.com")
    userDao.insert(user)

    val workflow = new Workflow
    workflow.setWid(testWorkflowId)
    workflow.setName("idle-cu-workflow")
    workflow.setContent("{}")
    workflow.setCreationTime(new Timestamp(now.getTime - TimeUnit.DAYS.toMillis(2)))
    workflow.setLastModifiedTime(new Timestamp(now.getTime - TimeUnit.DAYS.toMillis(2)))
    workflowDao.insert(workflow)

    testVersion = new WorkflowVersion
    testVersion.setWid(testWorkflowId)
    testVersion.setContent("{}")
    testVersion.setCreationTime(new Timestamp(now.getTime - TimeUnit.DAYS.toMillis(2)))
    workflowVersionDao.insert(testVersion)
  }

  override protected def afterEach(): Unit =
    cleanupTestData()

  override protected def afterAll(): Unit =
    shutdownDB()

  private def cleanupTestData(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.UID.eq(testUserId))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_COMPUTING_UNIT)
      .where(WORKFLOW_COMPUTING_UNIT.UID.eq(testUserId))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWorkflowId))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWorkflowId)).execute()
    getDSLContext.deleteFrom(USER_TABLE).where(USER_TABLE.UID.eq(testUserId)).execute()
  }

  private def timestampMinutesBefore(minutes: Long): Timestamp =
    new Timestamp(now.getTime - TimeUnit.MINUTES.toMillis(minutes))

  private def insertComputingUnit(
      name: String,
      unitType: WorkflowComputingUnitTypeEnum = WorkflowComputingUnitTypeEnum.kubernetes,
      creationMinutesBefore: Long = 120,
      terminated: Boolean = false
  ): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit
    unit.setUid(testUserId)
    unit.setName(name)
    unit.setCreationTime(timestampMinutesBefore(creationMinutesBefore))
    unit.setType(unitType)
    unit.setUri("kubernetes://test")
    unit.setResource("{}")
    if (terminated) {
      unit.setTerminateTime(timestampMinutesBefore(10))
      unit.setTerminationReason(WorkflowComputingUnitTerminationReasonEnum.USER_REQUESTED)
    }
    workflowComputingUnitDao.insert(unit)
    unit
  }

  private def insertExecution(
      unit: WorkflowComputingUnit,
      status: Short,
      startingMinutesBefore: Long,
      lastUpdateMinutesBefore: Option[Long] = None
  ): Unit = {
    val execution = new WorkflowExecutions
    execution.setVid(testVersion.getVid)
    execution.setUid(testUserId)
    execution.setCuid(unit.getCuid)
    execution.setStatus(status)
    execution.setStartingTime(timestampMinutesBefore(startingMinutesBefore))
    lastUpdateMinutesBefore.foreach(minutes =>
      execution.setLastUpdateTime(timestampMinutesBefore(minutes))
    )
    execution.setBookmarked(false)
    execution.setName("execution-" + UUID.randomUUID().toString.substring(0, 8))
    execution.setEnvironmentVersion("test-env")
    workflowExecutionsDao.insert(execution)
  }

  private def sessionUser(
      uid: Integer = testUserId,
      name: String = "idle-cu-owner"
  ): SessionUser = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    new SessionUser(user)
  }

  // The sweep drives the Kubernetes client through the same by-name seam ComputingUnitHelpers
  // uses, so a stub stands in for the production singleton.
  private def stubKubernetesClient(): KubernetesClient = mock(classOf[KubernetesClient])

  "TerminatedComputingUnitInfo" should "carry the terminated unit and its owner" in {
    val terminated = TerminatedComputingUnitInfo(
      cuid = 1,
      name = "plain-unit-info",
      uid = testUserId,
      username = Some("idle-cu-owner"),
      reason = WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
    )
    terminated.cuid shouldBe 1
    terminated.username shouldBe Some("idle-cu-owner")
    terminated.reason shouldBe WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
  }

  "terminateIdleKubernetesComputingUnits" should "garbage collect only inactive Kubernetes computing units past the timeout" in {
    val stale = insertComputingUnit("stale")
    val active = insertComputingUnit("active")
    val recent = insertComputingUnit("recent")
    val local = insertComputingUnit("local", WorkflowComputingUnitTypeEnum.local)
    val alreadyTerminated = insertComputingUnit("already-terminated", terminated = true)

    insertExecution(active, status = 1, startingMinutesBefore = 180)
    insertExecution(
      recent,
      status = 3,
      startingMinutesBefore = 180,
      lastUpdateMinutesBefore = Some(5)
    )
    insertExecution(local, status = 3, startingMinutesBefore = 180)

    val k8s = stubKubernetesClient()
    val terminated = ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits(
      idleTimeoutMinutes,
      now,
      k8s
    )

    terminated.map(_.cuid) shouldBe List(stale.getCuid)
    terminated.head.username shouldBe Some("idle-cu-owner")
    terminated.head.reason shouldBe WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
    verify(k8s).deletePod(stale.getCuid)
    verify(k8s, never()).deletePod(active.getCuid)
    verify(k8s, never()).deletePod(recent.getCuid)
    verify(k8s, never()).deletePod(local.getCuid)

    val staleAfterCleanup = workflowComputingUnitDao.fetchOneByCuid(stale.getCuid)
    staleAfterCleanup.getTerminateTime shouldBe now
    staleAfterCleanup.getTerminationReason shouldBe WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
    workflowComputingUnitDao.fetchOneByCuid(active.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao.fetchOneByCuid(recent.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao.fetchOneByCuid(local.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao.fetchOneByCuid(alreadyTerminated.getCuid).getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.USER_REQUESTED
  }

  it should "never touch the cluster when no unit is idle" in {
    val active = insertComputingUnit("active-only")
    insertExecution(active, status = 1, startingMinutesBefore = 180)
    val k8s = stubKubernetesClient()

    ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits(
      idleTimeoutMinutes,
      now,
      k8s
    ) shouldBe empty

    verifyNoInteractions(k8s)
  }

  it should "roll the termination back and keep collecting other units when one pod deletion fails" in {
    val failing = insertComputingUnit("stale-delete-fails")
    val successful = insertComputingUnit("stale-delete-succeeds")
    val k8s = stubKubernetesClient()
    doThrow(new RuntimeException("pod deletion failed")).when(k8s).deletePod(failing.getCuid)

    val terminated = ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits(
      idleTimeoutMinutes,
      now,
      k8s
    )

    terminated.map(_.cuid) shouldBe List(successful.getCuid)
    // The stamp is written before the pod is deleted, so the failed delete rolls it back and the
    // unit is retried on the next sweep rather than being left as a live pod marked terminated.
    workflowComputingUnitDao.fetchOneByCuid(failing.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao.fetchOneByCuid(failing.getCuid).getTerminationReason shouldBe null
    workflowComputingUnitDao
      .fetchOneByCuid(successful.getCuid)
      .getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
  }

  it should "mark an idle unit terminated even when its pod is already gone" in {
    // Deleting an absent pod is a no-op in the Kubernetes API, so the sweep issues the delete
    // unconditionally rather than paying a second round trip to ask whether the pod exists.
    val stale = insertComputingUnit("stale-missing-pod")
    val k8s = stubKubernetesClient()

    val terminated = ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits(
      idleTimeoutMinutes,
      now,
      k8s
    )

    terminated.map(_.cuid) shouldBe List(stale.getCuid)
    verify(k8s).deletePod(stale.getCuid)
    workflowComputingUnitDao.fetchOneByCuid(stale.getCuid).getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
  }

  it should "keep units whose latest execution is in any non-terminal state running" in {
    // 0/1/2 are UNINITIALIZED-or-READY, RUNNING and PAUSED; -1 is what maptoStatusCode collapses
    // PAUSING, RESUMING, UNKNOWN and TERMINATED to. None of them may be read as idle.
    val units = Seq[Short](0, 1, 2, -1).map { status =>
      val unit = insertComputingUnit(s"active-status-$status")
      insertExecution(unit, status = status, startingMinutesBefore = 180)
      unit
    }
    val k8s = stubKubernetesClient()

    ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits(
      idleTimeoutMinutes,
      now,
      k8s
    ) shouldBe empty

    verify(k8s, never()).deletePod(anyInt())
    units.foreach(unit =>
      workflowComputingUnitDao.fetchOneByCuid(unit.getCuid).getTerminateTime shouldBe null
    )
  }

  it should "garbage collect a unit whose latest execution finished past the timeout" in {
    // 3/4/5 are COMPLETED, FAILED and KILLED -- the terminal codes, so the unit counts as idle.
    Seq[Short](3, 4, 5).foreach { status =>
      val stale = insertComputingUnit(s"stale-status-$status")
      insertExecution(
        stale,
        status = status,
        startingMinutesBefore = 180,
        lastUpdateMinutesBefore = Some(90)
      )
      val k8s = stubKubernetesClient()

      val terminated = ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits(
        idleTimeoutMinutes,
        now,
        k8s
      )

      terminated.map(_.cuid) shouldBe List(stale.getCuid)
      verify(k8s).deletePod(stale.getCuid)
      workflowComputingUnitDao
        .fetchOneByCuid(stale.getCuid)
        .getTerminationReason shouldBe
        WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
    }
  }

  it should "omit an empty owner name from terminated unit info" in {
    val user = userDao.fetchOneByUid(testUserId)
    user.setName("")
    userDao.update(user)
    insertComputingUnit("stale-empty-owner")

    val terminated = ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits(
      idleTimeoutMinutes,
      now,
      stubKubernetesClient()
    )

    terminated should have size 1
    terminated.head.username shouldBe None
  }

  // The sweep reads every candidate before terminating any of them, so the window between the scan
  // and a given unit's UPDATE is as wide as the whole sweep. These drive the two steps separately
  // and mutate the DB in between, which is what that window looks like from the unit's side.
  private def scanCandidate(unit: WorkflowComputingUnit) = {
    val cutoff = timestampMinutesBefore(idleTimeoutMinutes)
    val candidate = ComputingUnitManagingResource
      .idleKubernetesComputingUnitCandidates(cutoff)
      .find(_.unit.getCuid == unit.getCuid)
    candidate should not be empty
    (candidate.get, cutoff)
  }

  "the terminating update" should "leave a unit alone when a run starts after the scan" in {
    val unit = insertComputingUnit("raced-run-started")
    val (candidate, cutoff) = scanCandidate(unit)

    // The user starts a workflow on the unit after the scan listed it as idle.
    insertExecution(unit, status = 1, startingMinutesBefore = 0)

    val k8s = stubKubernetesClient()
    ComputingUnitManagingResource.terminateIdleKubernetesComputingUnitCandidate(
      candidate,
      cutoff,
      now,
      k8s
    ) shouldBe None

    verifyNoInteractions(k8s)
    workflowComputingUnitDao.fetchOneByCuid(unit.getCuid).getTerminateTime shouldBe null
  }

  it should "leave a unit alone when an execution finished after the scan" in {
    // Terminal status, so the status half of the guard does not fire -- only the activity
    // timestamps place the execution at or after the cutoff.
    val unit = insertComputingUnit("raced-run-finished")
    val (candidate, cutoff) = scanCandidate(unit)

    insertExecution(
      unit,
      status = 3,
      startingMinutesBefore = 1,
      lastUpdateMinutesBefore = Some(1)
    )

    val k8s = stubKubernetesClient()
    ComputingUnitManagingResource.terminateIdleKubernetesComputingUnitCandidate(
      candidate,
      cutoff,
      now,
      k8s
    ) shouldBe None

    verifyNoInteractions(k8s)
    workflowComputingUnitDao.fetchOneByCuid(unit.getCuid).getTerminateTime shouldBe null
  }

  it should "leave a unit alone when it was terminated after the scan" in {
    val unit = insertComputingUnit("raced-user-terminated")
    val (candidate, cutoff) = scanCandidate(unit)

    // Stamped directly rather than through terminateComputingUnit, whose kubernetes branch talks
    // to the real cluster singleton; what the guard reads is the row, and this is the row a user
    // termination leaves behind.
    val terminatedByUser = workflowComputingUnitDao.fetchOneByCuid(unit.getCuid)
    terminatedByUser.setTerminateTime(timestampMinutesBefore(1))
    terminatedByUser.setTerminationReason(
      WorkflowComputingUnitTerminationReasonEnum.USER_REQUESTED
    )
    workflowComputingUnitDao.update(terminatedByUser)

    val k8s = stubKubernetesClient()
    ComputingUnitManagingResource.terminateIdleKubernetesComputingUnitCandidate(
      candidate,
      cutoff,
      now,
      k8s
    ) shouldBe None

    verifyNoInteractions(k8s)
    workflowComputingUnitDao.fetchOneByCuid(unit.getCuid).getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.USER_REQUESTED
  }

  it should "terminate the unit when nothing changed since the scan" in {
    val unit = insertComputingUnit("unraced")
    val (candidate, cutoff) = scanCandidate(unit)
    // A stale execution, still entirely before the cutoff, must not trip the guard.
    insertExecution(
      unit,
      status = 3,
      startingMinutesBefore = 180,
      lastUpdateMinutesBefore = Some(90)
    )

    val k8s = stubKubernetesClient()
    ComputingUnitManagingResource
      .terminateIdleKubernetesComputingUnitCandidate(candidate, cutoff, now, k8s)
      .map(_.cuid) shouldBe Some(unit.getCuid)

    verify(k8s).deletePod(unit.getCuid)
    workflowComputingUnitDao.fetchOneByCuid(unit.getCuid).getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
  }

  "terminateComputingUnit" should "mark manual termination as user requested" in {
    val local = insertComputingUnit("manual-local", WorkflowComputingUnitTypeEnum.local)

    val response =
      new ComputingUnitManagingResource().terminateComputingUnit(local.getCuid, sessionUser())

    response.getStatus shouldBe 200
    val terminated = workflowComputingUnitDao.fetchOneByCuid(local.getCuid)
    terminated.getTerminateTime should not be null
    terminated.getTerminationReason shouldBe WorkflowComputingUnitTerminationReasonEnum.USER_REQUESTED
  }

  it should "reject manual termination from a non-owner" in {
    val local = insertComputingUnit("manual-local-non-owner", WorkflowComputingUnitTypeEnum.local)

    val response = new ComputingUnitManagingResource().terminateComputingUnit(
      local.getCuid,
      sessionUser(uid = testUserId + 1)
    )

    response.getStatus shouldBe 400
    workflowComputingUnitDao.fetchOneByCuid(local.getCuid).getTerminateTime shouldBe null
  }

  "lastComputingUnitActivityTime" should "prefer the latest execution timestamp over creation time" in {
    val unit = new WorkflowComputingUnit
    unit.setCreationTime(timestampMinutesBefore(120))

    ComputingUnitManagingResource.lastComputingUnitActivityTime(
      unit,
      latestUpdateTime = Some(timestampMinutesBefore(10)),
      latestStartTime = Some(timestampMinutesBefore(30))
    ) shouldBe timestampMinutesBefore(10)
  }

  it should "fall back to start time and then creation time" in {
    val unit = new WorkflowComputingUnit
    unit.setCreationTime(timestampMinutesBefore(120))

    ComputingUnitManagingResource.lastComputingUnitActivityTime(
      unit,
      latestUpdateTime = None,
      latestStartTime = Some(timestampMinutesBefore(30))
    ) shouldBe timestampMinutesBefore(30)

    ComputingUnitManagingResource.lastComputingUnitActivityTime(
      unit,
      latestUpdateTime = None,
      latestStartTime = None
    ) shouldBe timestampMinutesBefore(120)
  }

  "shouldTerminateIdleComputingUnit" should "require both no active execution and activity before cutoff" in {
    val cutoff = timestampMinutesBefore(60)

    ComputingUnitManagingResource.shouldTerminateIdleComputingUnit(
      hasActiveExecution = false,
      lastExecutionTime = timestampMinutesBefore(61),
      cutoff = cutoff
    ) shouldBe true
    ComputingUnitManagingResource.shouldTerminateIdleComputingUnit(
      hasActiveExecution = true,
      lastExecutionTime = timestampMinutesBefore(61),
      cutoff = cutoff
    ) shouldBe false
    ComputingUnitManagingResource.shouldTerminateIdleComputingUnit(
      hasActiveExecution = false,
      lastExecutionTime = timestampMinutesBefore(60),
      cutoff = cutoff
    ) shouldBe false
  }
}
