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

package org.apache.texera.service.util

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowComputingUnit}
import org.apache.texera.service.resource.ComputingUnitManagingResource.WorkflowComputingUnitMetrics
import org.apache.texera.service.resource.ComputingUnitState.{
  Failed,
  Pending,
  Running,
  Terminating,
  Unknown
}
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ComputingUnitHelpersSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit =
    try shutdownDB()
    finally super.afterAll()

  private lazy val userDao = new UserDao(getDSLContext.configuration())
  private lazy val computingUnitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())

  private def makeUser(uid: Int, name: String, email: String, avatar: String): User = {
    val u = new User()
    u.setUid(uid)
    u.setName(name)
    u.setEmail(email)
    u.setRole(UserRoleEnum.REGULAR)
    u.setAvatar(avatar)
    u
  }

  private def makeUnit(
      cuid: Int,
      uid: Int,
      tpe: WorkflowComputingUnitTypeEnum
  ): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit()
    unit.setCuid(cuid)
    unit.setUid(uid)
    unit.setType(tpe)
    unit
  }

  private def localUnit(cuid: Int = 0, uid: Int = 0): WorkflowComputingUnit =
    makeUnit(cuid, uid, WorkflowComputingUnitTypeEnum.local)

  private def kubernetesUnit(cuid: Int, uid: Int = 0): WorkflowComputingUnit =
    makeUnit(cuid, uid, WorkflowComputingUnitTypeEnum.kubernetes)

  // A null-type unit (the enum has only local/kubernetes) exercises the "unknown" branch.
  private def untypedUnit(): WorkflowComputingUnit = new WorkflowComputingUnit()

  private def podWithPhase(phase: String): Pod =
    new PodBuilder().withNewStatus().withPhase(phase).endStatus().build()

  // A pod whose status has not been populated yet (getStatus == null).
  private def statuslessPod(): Pod = new PodBuilder().build()

  // ── PodBuilder recipes for the failure-state decision table ─────────
  // Each builds the minimal fabric8 Pod that a real cluster would report for
  // the scenario, then goes through the same pure fromPod transform production
  // uses, so these tests cover the snapshot extraction and the state mapping
  // together.

  private def terminatingPod(): Pod =
    new PodBuilder()
      .withNewMetadata()
      .withDeletionTimestamp("2026-08-20T00:00:00Z")
      .endMetadata()
      .withNewStatus()
      .withPhase("Running")
      .endStatus()
      .build()

  private def evictedPod(message: String): Pod =
    new PodBuilder()
      .withNewStatus()
      .withPhase("Failed")
      .withReason("Evicted")
      .withMessage(message)
      .endStatus()
      .build()

  /** A pod whose (single) container is stuck in the given waiting reason. */
  private def waitingContainerPod(
      phase: String,
      waitingReason: String,
      lastTerminatedReason: Option[String] = None,
      restartCount: Int = 0
  ): Pod = {
    val builder = new PodBuilder()
      .withNewStatus()
      .withPhase(phase)
      .addNewContainerStatus()
      .withRestartCount(restartCount)
      .withNewState()
      .withNewWaiting()
      .withReason(waitingReason)
      .endWaiting()
      .endState()
    lastTerminatedReason
      .fold(builder)(reason =>
        builder
          .withNewLastState()
          .withNewTerminated()
          .withReason(reason)
          .endTerminated()
          .endLastState()
      )
      .endContainerStatus()
      .endStatus()
      .build()
  }

  /** A running pod whose container previously terminated with the given reason. */
  private def restartedPod(lastTerminatedReason: String, restartCount: Int): Pod =
    new PodBuilder()
      .withNewStatus()
      .withPhase("Running")
      .addNewContainerStatus()
      .withRestartCount(restartCount)
      .withNewLastState()
      .withNewTerminated()
      .withReason(lastTerminatedReason)
      .endTerminated()
      .endLastState()
      .endContainerStatus()
      .endStatus()
      .build()

  private def unschedulablePod(): Pod =
    new PodBuilder()
      .withNewStatus()
      .withPhase("Pending")
      .addNewCondition()
      .withType("PodScheduled")
      .withStatus("False")
      .withReason("Unschedulable")
      .endCondition()
      .endStatus()
      .build()

  private def snapshotOf(pod: Pod): PodStatusSnapshot = PodStatusSnapshot.fromPod(pod)

  private def statusAndReasonOf(
      pod: Pod
  ): (org.apache.texera.service.resource.ComputingUnitState.ComputingUnitState, Option[String]) =
    ComputingUnitHelpers.kubernetesStatusAndReason(Some(snapshotOf(pod)))

  // The kubernetes branch does a per-unit pod lookup, so singleUnitStatusAndReason is driven
  // through a stubbed client (the public overloads bind the production singleton).
  "singleUnitStatusAndReason" should "return Running for a kubernetes unit whose pod phase is Running" in {
    val k8s = mock(classOf[KubernetesClient])
    when(k8s.generatePodName(40)).thenReturn("computing-unit-40")
    when(k8s.getPodByName("computing-unit-40")).thenReturn(Some(podWithPhase("Running")))
    ComputingUnitHelpers.singleUnitStatusAndReason(kubernetesUnit(40), k8s) shouldBe
      ((Running, None))
  }

  it should "return Pending for a kubernetes unit whose pod has no status yet" in {
    val k8s = mock(classOf[KubernetesClient])
    when(k8s.generatePodName(41)).thenReturn("computing-unit-41")
    when(k8s.getPodByName("computing-unit-41")).thenReturn(Some(statuslessPod()))
    ComputingUnitHelpers.singleUnitStatusAndReason(kubernetesUnit(41), k8s) shouldBe
      ((Pending, None))
  }

  it should "return Pending for a kubernetes unit whose pod is absent" in {
    val k8s = mock(classOf[KubernetesClient])
    when(k8s.generatePodName(42)).thenReturn("computing-unit-42")
    when(k8s.getPodByName("computing-unit-42")).thenReturn(None)
    ComputingUnitHelpers.singleUnitStatusAndReason(kubernetesUnit(42), k8s) shouldBe
      ((Pending, None))
  }

  it should "surface the failure state and reason of a dead pod" in {
    val k8s = mock(classOf[KubernetesClient])
    when(k8s.generatePodName(43)).thenReturn("computing-unit-43")
    when(k8s.getPodByName("computing-unit-43"))
      .thenReturn(Some(waitingContainerPod("Pending", "ImagePullBackOff")))
    val (state, reason) = ComputingUnitHelpers.singleUnitStatusAndReason(kubernetesUnit(43), k8s)
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit's image could not be pulled. Please recreate the unit or contact an administrator."
    )
  }

  "getComputingUnitMetrics" should "return NaN metrics for a local unit" in {
    ComputingUnitHelpers.getComputingUnitMetrics(localUnit()) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
  }

  it should "return NaN metrics for an unknown (untyped) unit" in {
    ComputingUnitHelpers.getComputingUnitMetrics(untypedUnit()) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
  }

  // ── Bulk variants resolving from pre-fetched pod maps ────────────────

  "getComputingUnitStatusAndReason(unit, podSnapshots)" should "return Running for a local unit" in {
    ComputingUnitHelpers.getComputingUnitStatusAndReason(localUnit(), Map.empty) shouldBe
      ((Running, None))
  }

  it should "return Running for a kubernetes unit whose pod phase is Running" in {
    val unit = kubernetesUnit(7)
    val podSnapshots =
      Map(KubernetesClient.generatePodName(7) -> snapshotOf(podWithPhase("Running")))
    ComputingUnitHelpers.getComputingUnitStatusAndReason(unit, podSnapshots) shouldBe
      ((Running, None))
  }

  it should "return Pending for a kubernetes unit whose pod is absent or not Running" in {
    val unit = kubernetesUnit(8)
    ComputingUnitHelpers.getComputingUnitStatusAndReason(unit, Map.empty) shouldBe ((Pending, None))
    ComputingUnitHelpers.getComputingUnitStatusAndReason(
      unit,
      Map(KubernetesClient.generatePodName(8) -> snapshotOf(podWithPhase("Pending")))
    ) shouldBe ((Pending, None))
  }

  it should "treat a null phase as not Running" in {
    val unit = kubernetesUnit(9)
    val podSnapshots = Map(KubernetesClient.generatePodName(9) -> snapshotOf(statuslessPod()))
    ComputingUnitHelpers.getComputingUnitStatusAndReason(unit, podSnapshots) shouldBe
      ((Pending, None))
  }

  it should "return Pending for an unknown (untyped) unit" in {
    ComputingUnitHelpers.getComputingUnitStatusAndReason(untypedUnit(), Map.empty) shouldBe
      ((Pending, None))
  }

  // ── kubernetesStatusAndReason: one test per decision-table row ───────

  "kubernetesStatusAndReason" should "map an absent pod to Pending with no reason" in {
    ComputingUnitHelpers.kubernetesStatusAndReason(None) shouldBe ((Pending, None))
  }

  it should "map a pod with a deletion timestamp to Terminating regardless of phase" in {
    statusAndReasonOf(terminatingPod()) shouldBe ((Terminating, None))
  }

  it should "map a status-less pod that is being deleted to Terminating, not Pending" in {
    // A pod can carry a deletion timestamp before its status is ever populated; the
    // empty-status snapshot must still honor the terminating flag.
    val pod = new PodBuilder()
      .withNewMetadata()
      .withDeletionTimestamp("2026-08-20T00:00:00Z")
      .endMetadata()
      .build()
    statusAndReasonOf(pod) shouldBe ((Terminating, None))
  }

  it should "let Terminating win over an eviction" in {
    // Deleting an already-evicted pod is the owner acting on the failure: the row should
    // show the deletion in progress, not keep explaining the eviction.
    val pod = new PodBuilder()
      .withNewMetadata()
      .withDeletionTimestamp("2026-08-20T00:00:00Z")
      .endMetadata()
      .withNewStatus()
      .withPhase("Failed")
      .withReason("Evicted")
      .withMessage("Pod ephemeral local storage usage exceeds the total limit of containers 1Gi.")
      .endStatus()
      .build()
    statusAndReasonOf(pod) shouldBe ((Terminating, None))
  }

  it should "map a disk-pressure eviction to Failed with the local-disk reason" in {
    val (state, reason) = statusAndReasonOf(
      evictedPod("Pod ephemeral local storage usage exceeds the total limit of containers 1Gi.")
    )
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit was evicted because it ran out of local disk storage. Consider " +
        "storing less data on the unit's local file system, or recreate it with more storage."
    )
  }

  it should "map any other eviction to Failed with a short version of the cluster's reason" in {
    val (state, reason) = statusAndReasonOf(
      evictedPod("The node was low on resource: memory. Container was using more than its request.")
    )
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit was evicted by the cluster (The node was low on resource: memory). " +
        "Consider recreating it."
    )
  }

  it should "fall back to the pod's Evicted reason when the eviction carries no message" in {
    val (state, reason) = statusAndReasonOf(evictedPod(null))
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit was evicted by the cluster (Evicted). Consider recreating it."
    )
  }

  it should "fall back to the pod's Evicted reason when the eviction message is only whitespace" in {
    val (state, reason) = statusAndReasonOf(evictedPod("   "))
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit was evicted by the cluster (Evicted). Consider recreating it."
    )
  }

  it should "use the whole eviction message when it contains no period" in {
    val (state, reason) = statusAndReasonOf(evictedPod("The node was under memory pressure"))
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit was evicted by the cluster (The node was under memory pressure). " +
        "Consider recreating it."
    )
  }

  it should "truncate an eviction sentence longer than 120 characters" in {
    val sentence = "The node reported " + ("x" * 120) // 138 chars, no period
    val (state, reason) = statusAndReasonOf(evictedPod(sentence))
    state shouldBe Failed
    reason shouldBe Some(
      s"The computing unit was evicted by the cluster (${sentence.take(120)}...). " +
        "Consider recreating it."
    )
  }

  it should "detect the disk-pressure wording case-insensitively" in {
    Seq(
      "EPHEMERAL storage limit exceeded",
      "The node was low on Disk space"
    ).foreach { message =>
      val (state, reason) = statusAndReasonOf(evictedPod(message))
      state shouldBe Failed
      reason shouldBe Some(
        "The computing unit was evicted because it ran out of local disk storage. Consider " +
          "storing less data on the unit's local file system, or recreate it with more storage."
      )
    }
  }

  it should "let the eviction wording win over a crash-looping container" in {
    // An evicted pod may still report a crash-looping container from before the eviction;
    // the eviction is the root cause, so its wording must take precedence.
    val pod = new PodBuilder()
      .withNewStatus()
      .withPhase("Failed")
      .withReason("Evicted")
      .withMessage("Pod ephemeral local storage usage exceeds the total limit of containers 1Gi.")
      .addNewContainerStatus()
      .withRestartCount(5)
      .withNewState()
      .withNewWaiting()
      .withReason("CrashLoopBackOff")
      .endWaiting()
      .endState()
      .endContainerStatus()
      .endStatus()
      .build()

    val (state, reason) = statusAndReasonOf(pod)
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit was evicted because it ran out of local disk storage. Consider " +
        "storing less data on the unit's local file system, or recreate it with more storage."
    )
  }

  it should "map every image-pull waiting reason to Failed with the image-pull reason" in {
    Seq("ImagePullBackOff", "ErrImagePull", "InvalidImageName").foreach { waitingReason =>
      val (state, reason) = statusAndReasonOf(waitingContainerPod("Pending", waitingReason))
      state shouldBe Failed
      reason shouldBe Some(
        "The computing unit's image could not be pulled. Please recreate the unit or contact " +
          "an administrator."
      )
    }
  }

  it should "map a crash loop after an OOM kill to Failed with the out-of-memory reason" in {
    // restartPolicy Always keeps the pod phase "Running" through an OOM crash loop, so the
    // waiting/lastState container fields are the only signal.
    val (state, reason) = statusAndReasonOf(
      waitingContainerPod(
        "Running",
        "CrashLoopBackOff",
        lastTerminatedReason = Some("OOMKilled"),
        restartCount = 4
      )
    )
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit keeps crashing because it runs out of memory. Please terminate it " +
        "and recreate it with a higher memory limit."
    )
  }

  it should "map a plain crash loop to Failed with the restart count" in {
    val (state, reason) = statusAndReasonOf(
      waitingContainerPod(
        "Running",
        "CrashLoopBackOff",
        lastTerminatedReason = Some("Error"),
        restartCount = 7
      )
    )
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit is repeatedly crashing (restarted 7 times). Please terminate and " +
        "recreate it, or contact an administrator."
    )
  }

  it should "fail the unit when any container crash-loops, not just the first one" in {
    // Multi-container pod (e.g. a sidecar): the healthy first container must not mask the
    // crash-looping second one.
    val pod = new PodBuilder()
      .withNewStatus()
      .withPhase("Running")
      .addNewContainerStatus()
      .withRestartCount(0)
      .endContainerStatus()
      .addNewContainerStatus()
      .withRestartCount(3)
      .withNewState()
      .withNewWaiting()
      .withReason("CrashLoopBackOff")
      .endWaiting()
      .endState()
      .endContainerStatus()
      .endStatus()
      .build()

    val (state, reason) = statusAndReasonOf(pod)
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit is repeatedly crashing (restarted 3 times). Please terminate and " +
        "recreate it, or contact an administrator."
    )
  }

  it should "word a crash loop by the crash-looping container's own history, not a sibling's" in {
    // Container A recovered from an OOM kill and is running; container B crash-loops for a
    // non-OOM reason. The wording (and restart count) must come from B's own lastState, so
    // A's OOM history must not upgrade the message to the out-of-memory variant.
    val pod = new PodBuilder()
      .withNewStatus()
      .withPhase("Running")
      .addNewContainerStatus()
      .withRestartCount(2)
      .withNewLastState()
      .withNewTerminated()
      .withReason("OOMKilled")
      .endTerminated()
      .endLastState()
      .endContainerStatus()
      .addNewContainerStatus()
      .withRestartCount(6)
      .withNewState()
      .withNewWaiting()
      .withReason("CrashLoopBackOff")
      .endWaiting()
      .endState()
      .withNewLastState()
      .withNewTerminated()
      .withReason("Error")
      .endTerminated()
      .endLastState()
      .endContainerStatus()
      .endStatus()
      .build()

    val (state, reason) = statusAndReasonOf(pod)
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit is repeatedly crashing (restarted 6 times). Please terminate and " +
        "recreate it, or contact an administrator."
    )
  }

  it should "let an image-pull failure win over a crash-looping sibling container" in {
    // Precedence pin: the image-pull check runs before the crash-loop one, because a unit
    // whose image cannot be pulled can only be fixed by recreating it.
    val pod = new PodBuilder()
      .withNewStatus()
      .withPhase("Pending")
      .addNewContainerStatus()
      .withRestartCount(0)
      .withNewState()
      .withNewWaiting()
      .withReason("ErrImagePull")
      .endWaiting()
      .endState()
      .endContainerStatus()
      .addNewContainerStatus()
      .withRestartCount(3)
      .withNewState()
      .withNewWaiting()
      .withReason("CrashLoopBackOff")
      .endWaiting()
      .endState()
      .endContainerStatus()
      .endStatus()
      .build()

    val (state, reason) = statusAndReasonOf(pod)
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit's image could not be pulled. Please recreate the unit or contact " +
        "an administrator."
    )
  }

  it should "map a non-evicted Failed phase to Failed with the generic reason" in {
    val (state, reason) = statusAndReasonOf(podWithPhase("Failed"))
    state shouldBe Failed
    reason shouldBe Some(
      "The computing unit stopped unexpectedly. Please terminate and recreate it, or contact " +
        "an administrator."
    )
  }

  it should "map an Unknown phase to Unknown with the unreachable-node reason" in {
    val (state, reason) = statusAndReasonOf(podWithPhase("Unknown"))
    state shouldBe Unknown
    reason shouldBe Some(
      "The state of the computing unit cannot be determined (its node may be unreachable)."
    )
  }

  it should "keep an unschedulable pod Pending but explain the wait" in {
    val (state, reason) = statusAndReasonOf(unschedulablePod())
    state shouldBe Pending
    reason shouldBe Some(
      "The computing unit is waiting for cluster resources to become available."
    )
  }

  it should "ignore a stale Unschedulable condition once the pod is Running" in {
    // The unschedulable explanation is gated on phase Pending, so a leftover
    // PodScheduled=False condition on a pod that has since started must not resurface it.
    val pod = new PodBuilder()
      .withNewStatus()
      .withPhase("Running")
      .addNewCondition()
      .withType("PodScheduled")
      .withStatus("False")
      .withReason("Unschedulable")
      .endCondition()
      .endStatus()
      .build()
    statusAndReasonOf(pod) shouldBe ((Running, None))
  }

  it should "keep a recovered OOM-killed pod Running but attach the memory warning" in {
    // The container restarted in place after the OOM kill (restartPolicy Always), so the unit
    // is usable again — the reason is a warning, not a failure.
    val (state, reason) = statusAndReasonOf(restartedPod("OOMKilled", restartCount = 2))
    state shouldBe Running
    reason shouldBe Some(
      "The last run was terminated because the computing unit ran out of memory (restarted 2 " +
        "times). Consider recreating the unit with a higher memory limit before running the " +
        "same workload."
    )
  }

  it should "not attach the memory warning to a container that restarted for another reason" in {
    statusAndReasonOf(restartedPod("Error", restartCount = 1)) shouldBe ((Running, None))
  }

  it should "let a crash-loop failure win over the recovered-OOM warning" in {
    // Both signals present: the waiting CrashLoopBackOff means the unit is NOT usable, so the
    // failure must take precedence over the phase-Running OOM warning.
    val (state, _) = statusAndReasonOf(
      waitingContainerPod(
        "Running",
        "CrashLoopBackOff",
        lastTerminatedReason = Some("OOMKilled"),
        restartCount = 3
      )
    )
    state shouldBe Failed
  }

  it should "map a plain Running phase to Running and anything else to Pending, without reasons" in {
    statusAndReasonOf(podWithPhase("Running")) shouldBe ((Running, None))
    statusAndReasonOf(podWithPhase("Pending")) shouldBe ((Pending, None))
    statusAndReasonOf(podWithPhase("SomethingNew")) shouldBe ((Pending, None))
  }

  it should "map a Succeeded phase to Pending (current behavior, pinned deliberately)" in {
    // "Succeeded" has no dedicated branch and falls through to Pending. With restartPolicy
    // Always a computing-unit pod essentially never completes, but if one ever did, its unit
    // would show as connecting indefinitely — pinned here so any future change is conscious.
    statusAndReasonOf(podWithPhase("Succeeded")) shouldBe ((Pending, None))
  }

  "getComputingUnitMetrics(unit, podMetrics)" should "return NaN metrics for a local unit" in {
    ComputingUnitHelpers.getComputingUnitMetrics(localUnit(), Map.empty) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
  }

  it should "resolve cpu/memory for a kubernetes unit from the map" in {
    val unit = kubernetesUnit(10)
    val podMetrics = Map(
      KubernetesClient.generatePodName(10) -> Map("cpu" -> "500m", "memory" -> "256Mi")
    )
    ComputingUnitHelpers.getComputingUnitMetrics(unit, podMetrics) shouldBe
      WorkflowComputingUnitMetrics("500m", "256Mi")
  }

  it should "return empty cpu/memory for a kubernetes unit absent from the map" in {
    ComputingUnitHelpers.getComputingUnitMetrics(kubernetesUnit(11), Map.empty) shouldBe
      WorkflowComputingUnitMetrics("", "")
  }

  it should "return NaN for an unknown (untyped) unit" in {
    ComputingUnitHelpers.getComputingUnitMetrics(untypedUnit(), Map.empty) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
  }

  // ── partitionLiveUnits ───────────────────────────────────────────────

  "partitionLiveUnits" should "treat local units as always live" in {
    val units = List(localUnit(cuid = 1), localUnit(cuid = 2))
    val (live, vanished) = ComputingUnitHelpers.partitionLiveUnits(units, Map.empty)
    live.map(_.getCuid) shouldBe List(1, 2)
    vanished shouldBe empty
  }

  it should "classify a kubernetes unit as live iff its pod is present in the map" in {
    val present = kubernetesUnit(20)
    val gone = kubernetesUnit(21)
    val podSnapshots =
      Map(KubernetesClient.generatePodName(20) -> snapshotOf(podWithPhase("Running")))

    val (live, vanished) =
      ComputingUnitHelpers.partitionLiveUnits(List(present, gone), podSnapshots)

    live.map(_.getCuid) shouldBe List(20)
    vanished.map(_.getCuid) shouldBe List(21)
  }

  it should "treat a unit whose pod is present but failed as live (dead, not vanished)" in {
    // Failure states must NOT change the vanish semantics: a pod that still exists in the
    // namespace — however broken — keeps its unit un-terminated so the owner can see why.
    val failed = kubernetesUnit(22)
    val podSnapshots =
      Map(KubernetesClient.generatePodName(22) -> snapshotOf(podWithPhase("Failed")))

    val (live, vanished) = ComputingUnitHelpers.partitionLiveUnits(List(failed), podSnapshots)

    live.map(_.getCuid) shouldBe List(22)
    vanished shouldBe empty
  }

  it should "treat an untyped (null-type) unit as live (never kubernetes)" in {
    val (live, vanished) = ComputingUnitHelpers.partitionLiveUnits(List(untypedUnit()), Map.empty)
    live should have size 1
    vanished shouldBe empty
  }

  // ── buildDashboardUnit ───────────────────────────────────────────────

  "buildDashboardUnit" should "populate the row from the caller flags and pre-fetched maps" in {
    val unit = kubernetesUnit(cuid = 30, uid = 100)
    val podName = KubernetesClient.generatePodName(30)

    val row = ComputingUnitHelpers.buildDashboardUnit(
      unit,
      isOwner = true,
      canViewStatusReason = true,
      accessPrivilege = PrivilegeEnum.READ,
      ownerInfo = Map((100: Integer) -> ("avatar", "owner")),
      podSnapshots = Map(podName -> snapshotOf(podWithPhase("Running"))),
      podMetrics = Map(podName -> Map("cpu" -> "100m", "memory" -> "64Mi"))
    )

    row.computingUnit.getCuid shouldBe 30
    row.isOwner shouldBe true
    row.accessPrivilege shouldBe PrivilegeEnum.READ
    row.status shouldBe "Running"
    row.statusReason shouldBe None
    row.metrics shouldBe WorkflowComputingUnitMetrics("100m", "64Mi")
    row.ownerAvatar shouldBe "avatar"
    row.ownerName shouldBe "owner"
  }

  it should "fall back to null owner info when the owner is missing from the map" in {
    val row = ComputingUnitHelpers.buildDashboardUnit(
      localUnit(cuid = 31, uid = 200),
      isOwner = false,
      canViewStatusReason = false,
      accessPrivilege = PrivilegeEnum.WRITE,
      ownerInfo = Map.empty,
      podSnapshots = Map.empty,
      podMetrics = Map.empty
    )

    row.ownerAvatar shouldBe null
    row.ownerName shouldBe null
    row.status shouldBe "Running"
    row.metrics shouldBe WorkflowComputingUnitMetrics("NaN", "NaN")
  }

  it should "include the status reason only when the caller permits it" in {
    val unit = kubernetesUnit(cuid = 32, uid = 100)
    val podName = KubernetesClient.generatePodName(32)
    val podSnapshots = Map(podName -> snapshotOf(podWithPhase("Failed")))

    def build(isOwner: Boolean, canViewStatusReason: Boolean) =
      ComputingUnitHelpers.buildDashboardUnit(
        unit,
        isOwner = isOwner,
        canViewStatusReason = canViewStatusReason,
        accessPrivilege = PrivilegeEnum.READ,
        ownerInfo = Map.empty,
        podSnapshots = podSnapshots,
        podMetrics = Map.empty
      )

    val ownerRow = build(isOwner = true, canViewStatusReason = true)
    ownerRow.status shouldBe "Failed"
    ownerRow.statusReason shouldBe Some(
      "The computing unit stopped unexpectedly. Please terminate and recreate it, or contact " +
        "an administrator."
    )

    val sharedRow = build(isOwner = false, canViewStatusReason = false)
    sharedRow.status shouldBe "Failed"
    sharedRow.statusReason shouldBe None

    val adminRow = build(isOwner = false, canViewStatusReason = true)
    adminRow.isOwner shouldBe false
    adminRow.statusReason shouldBe ownerRow.statusReason
  }

  // ── podSnapshotsFor / podMetricsFor guards ───────────────────────────

  "podSnapshotsFor" should "return empty (issuing no cluster call) when no kubernetes unit is present" in {
    ComputingUnitHelpers.podSnapshotsFor(List(localUnit(), untypedUnit())) shouldBe empty
  }

  it should "fetch all pod snapshots once when a kubernetes unit is present" in {
    val k8s = mock(classOf[KubernetesClient])
    val snapshots = Map("computing-unit-50" -> snapshotOf(podWithPhase("Running")))
    when(k8s.getAllPodStatusSnapshots).thenReturn(snapshots)
    ComputingUnitHelpers.podSnapshotsFor(List(kubernetesUnit(50)), k8s) shouldBe snapshots
  }

  "podMetricsFor" should "return empty (issuing no cluster call) when no kubernetes unit is present" in {
    ComputingUnitHelpers.podMetricsFor(List(localUnit(), untypedUnit())) shouldBe empty
  }

  it should "fetch all pod metrics once when a kubernetes unit is present" in {
    val k8s = mock(classOf[KubernetesClient])
    val metrics = Map("computing-unit-51" -> Map("cpu" -> "100m", "memory" -> "64Mi"))
    when(k8s.getAllPodMetrics).thenReturn(metrics)
    ComputingUnitHelpers.podMetricsFor(List(kubernetesUnit(51)), k8s) shouldBe metrics
  }

  // ── resolveOwnerInfo (backed by the embedded database) ───────────────

  "resolveOwnerInfo" should "resolve avatar/name and collapse blank values to null" in {
    userDao.insert(makeUser(500, "alice", "alice@example.com", "alice-avatar"))
    userDao.insert(makeUser(501, "", "bob@example.com", ""))

    val info = ComputingUnitHelpers.resolveOwnerInfo(userDao, Seq[Integer](500, 501))
    info(500) shouldBe (("alice-avatar", "alice"))
    info(501) shouldBe ((null, null))
  }

  it should "return an empty map (and issue no query) for no uids" in {
    ComputingUnitHelpers.resolveOwnerInfo(userDao, Seq.empty) shouldBe empty
  }

  // ── reconcileVanishedKubernetesUnits (backed by the embedded database) ─

  "reconcileVanishedKubernetesUnits" should "terminate vanished kubernetes units and return the live ones" in {
    userDao.insert(makeUser(600, "carol", "carol@example.com", null))

    val present = kubernetesUnit(600, 600)
    present.setName("present")
    val gone = kubernetesUnit(601, 600)
    gone.setName("gone")
    val local = localUnit(602, 600)
    local.setName("local")
    Seq(present, gone, local).foreach(computingUnitDao.insert(_))

    // Only the pod for cuid 600 exists; cuid 601's pod has vanished.
    val podSnapshots =
      Map(KubernetesClient.generatePodName(600) -> snapshotOf(podWithPhase("Running")))
    val live =
      ComputingUnitHelpers.reconcileVanishedKubernetesUnits(
        computingUnitDao,
        List(present, gone, local),
        podSnapshots
      )

    live.map(_.getCuid) should contain theSameElementsAs Seq(600, 602)
    computingUnitDao.fetchOneByCuid(601).getTerminateTime should not be null
    computingUnitDao.fetchOneByCuid(600).getTerminateTime shouldBe null
    computingUnitDao.fetchOneByCuid(602).getTerminateTime shouldBe null
  }

  it should "not terminate a unit whose pod is present but in a failure state" in {
    userDao.insert(makeUser(610, "dave", "dave@example.com", null))

    val failed = kubernetesUnit(611, 610)
    failed.setName("failed")
    computingUnitDao.insert(failed)

    val podSnapshots =
      Map(KubernetesClient.generatePodName(611) -> snapshotOf(podWithPhase("Failed")))
    val live =
      ComputingUnitHelpers.reconcileVanishedKubernetesUnits(
        computingUnitDao,
        List(failed),
        podSnapshots
      )

    live.map(_.getCuid) shouldBe List(611)
    computingUnitDao.fetchOneByCuid(611).getTerminateTime shouldBe null
  }
}
