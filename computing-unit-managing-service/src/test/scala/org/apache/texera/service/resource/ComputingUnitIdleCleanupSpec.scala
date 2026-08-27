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
import org.apache.texera.service.resource.ComputingUnitManagingResource.{
  IdleComputingUnitCleanupConfig,
  KubernetesPodOperations,
  TerminatedComputingUnitInfo
}
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
  private val cleanupConfig =
    IdleComputingUnitCleanupConfig(enabled = true, idleTimeoutMinutes = 60)

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

  private class RecordingPodOperations(existingPods: Set[Int]) extends KubernetesPodOperations {
    var deletedPods: List[Int] = List.empty

    override val podExists: Int => Boolean = cuid => existingPods.contains(cuid)
    override val deletePod: Int => Unit = cuid => deletedPods = deletedPods :+ cuid
  }

  private class FailingPodOperations(existingPods: Set[Int], failingPod: Int)
      extends KubernetesPodOperations {
    var deletedPods: List[Int] = List.empty

    override val podExists: Int => Boolean = cuid => existingPods.contains(cuid)
    override val deletePod: Int => Unit = cuid =>
      if (cuid == failingPod) {
        throw new RuntimeException("pod deletion failed")
      } else {
        deletedPods = deletedPods :+ cuid
      }
  }

  "cleanup value objects" should "expose cleanup configuration and termination info fields" in {
    val config = IdleComputingUnitCleanupConfig(enabled = true, idleTimeoutMinutes = 60)
    config.copy(enabled = false).enabled shouldBe false
    config.copy(idleTimeoutMinutes = 30).idleTimeoutMinutes shouldBe 30

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

  "terminateIdleKubernetesComputingUnits" should "return empty without scanning when cleanup is disabled" in {
    val podOperations = new RecordingPodOperations(Set.empty)

    ComputingUnitManagingResource.runIdleKubernetesComputingUnitCleanup(
      cleanupConfig.copy(enabled = false),
      () => now,
      podOperations
    ) shouldBe empty

    podOperations.deletedPods shouldBe empty
  }

  it should "return empty when the idle timeout is disabled" in {
    val podOperations = new RecordingPodOperations(Set.empty)

    ComputingUnitManagingResource.runIdleKubernetesComputingUnitCleanup(
      cleanupConfig.copy(idleTimeoutMinutes = 0),
      () => now,
      podOperations
    ) shouldBe empty

    podOperations.deletedPods shouldBe empty
  }

  it should "garbage collect only inactive Kubernetes computing units past the timeout" in {
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

    val podOperations = new RecordingPodOperations(Set(stale.getCuid, active.getCuid))
    val terminated = ComputingUnitManagingResource.runIdleKubernetesComputingUnitCleanup(
      cleanupConfig,
      () => now,
      podOperations
    )

    terminated.map(_.cuid) shouldBe List(stale.getCuid)
    terminated.head.username shouldBe Some("idle-cu-owner")
    terminated.head.reason shouldBe WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
    podOperations.deletedPods shouldBe List(stale.getCuid)

    val staleAfterCleanup = workflowComputingUnitDao.fetchOneByCuid(stale.getCuid)
    staleAfterCleanup.getTerminateTime shouldBe now
    staleAfterCleanup.getTerminationReason shouldBe WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
    workflowComputingUnitDao.fetchOneByCuid(active.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao.fetchOneByCuid(recent.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao.fetchOneByCuid(local.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao.fetchOneByCuid(alreadyTerminated.getCuid).getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.USER_REQUESTED
  }

  it should "continue garbage collecting other idle units when one pod deletion fails" in {
    val failing = insertComputingUnit("stale-delete-fails")
    val successful = insertComputingUnit("stale-delete-succeeds")
    val podOperations = new FailingPodOperations(
      Set(failing.getCuid, successful.getCuid),
      failingPod = failing.getCuid
    )

    val terminated = ComputingUnitManagingResource.runIdleKubernetesComputingUnitCleanup(
      cleanupConfig,
      () => now,
      podOperations
    )

    terminated.map(_.cuid) shouldBe List(successful.getCuid)
    podOperations.deletedPods shouldBe List(successful.getCuid)
    workflowComputingUnitDao.fetchOneByCuid(failing.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao
      .fetchOneByCuid(successful.getCuid)
      .getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
  }

  it should "mark an idle unit terminated even when the pod is already absent" in {
    val stale = insertComputingUnit("stale-missing-pod")
    val podOperations = new RecordingPodOperations(Set.empty)

    val terminated = ComputingUnitManagingResource.runIdleKubernetesComputingUnitCleanup(
      cleanupConfig,
      () => now,
      podOperations
    )

    terminated.map(_.cuid) shouldBe List(stale.getCuid)
    podOperations.deletedPods shouldBe empty
    workflowComputingUnitDao.fetchOneByCuid(stale.getCuid).getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
  }

  it should "keep units with active status 0 or 2 running" in {
    val activeQueued = insertComputingUnit("active-status-0")
    val activeRunning = insertComputingUnit("active-status-2")
    insertExecution(activeQueued, status = 0, startingMinutesBefore = 180)
    insertExecution(activeRunning, status = 2, startingMinutesBefore = 180)
    val podOperations = new RecordingPodOperations(Set(activeQueued.getCuid, activeRunning.getCuid))

    ComputingUnitManagingResource.runIdleKubernetesComputingUnitCleanup(
      cleanupConfig,
      () => now,
      podOperations
    ) shouldBe empty

    podOperations.deletedPods shouldBe empty
    workflowComputingUnitDao.fetchOneByCuid(activeQueued.getCuid).getTerminateTime shouldBe null
    workflowComputingUnitDao.fetchOneByCuid(activeRunning.getCuid).getTerminateTime shouldBe null
  }

  it should "terminate a unit whose latest completed execution activity is past the timeout" in {
    val staleWithExecution = insertComputingUnit("stale-completed-execution")
    insertExecution(
      staleWithExecution,
      status = 3,
      startingMinutesBefore = 180,
      lastUpdateMinutesBefore = Some(90)
    )
    val podOperations = new RecordingPodOperations(Set(staleWithExecution.getCuid))

    val terminated = ComputingUnitManagingResource.runIdleKubernetesComputingUnitCleanup(
      cleanupConfig,
      () => now,
      podOperations
    )

    terminated.map(_.cuid) shouldBe List(staleWithExecution.getCuid)
    podOperations.deletedPods shouldBe List(staleWithExecution.getCuid)
    workflowComputingUnitDao
      .fetchOneByCuid(staleWithExecution.getCuid)
      .getTerminationReason shouldBe
      WorkflowComputingUnitTerminationReasonEnum.GARBAGE_COLLECTED
  }

  it should "omit an empty owner name from terminated unit info" in {
    val user = userDao.fetchOneByUid(testUserId)
    user.setName("")
    userDao.update(user)
    insertComputingUnit("stale-empty-owner")
    val podOperations = new RecordingPodOperations(Set.empty)

    val terminated = ComputingUnitManagingResource.runIdleKubernetesComputingUnitCleanup(
      cleanupConfig,
      () => now,
      podOperations
    )

    terminated should have size 1
    terminated.head.username shouldBe None
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
