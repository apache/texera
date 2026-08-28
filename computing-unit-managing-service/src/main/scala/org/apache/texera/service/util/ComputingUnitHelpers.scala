/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.service.util

import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import org.apache.texera.service.resource.ComputingUnitManagingResource.{
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitMetrics
}
import org.apache.texera.service.resource.ComputingUnitState.{
  ComputingUnitState,
  Failed,
  Pending,
  Running,
  Terminating,
  Unknown
}
import org.jooq.EnumType

import java.sql.Timestamp
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

object ComputingUnitHelpers {

  /**
    * Owner (avatar, name) keyed by uid, resolved in one query. Blank values collapse to `null`;
    * empty `uids` returns empty without querying.
    */
  def resolveOwnerInfo(
      userDao: UserDao,
      uids: Seq[Integer]
  ): Map[Integer, (String, String)] = {
    if (uids.isEmpty) Map.empty
    else
      userDao
        .fetchByUid(uids: _*)
        .asScala
        .map { u =>
          val avatar = Option(u.getAvatar).filter(_.nonEmpty).orNull
          val name = Option(u.getName).filter(_.nonEmpty).orNull
          u.getUid -> (avatar, name)
        }
        .toMap
  }

  /** Single-unit status plus its user-facing reason (see [[kubernetesStatusAndReason]]). */
  def getComputingUnitStatusWithReason(
      unit: WorkflowComputingUnit
  ): (ComputingUnitState, Option[String]) =
    singleUnitStatusAndReason(unit, KubernetesClient)

  /**
    * Single-unit status via a per-unit pod lookup (a targeted GET, cheaper than listing the whole
    * namespace). The client is a by-name parameter — not the global singleton — so the kubernetes
    * branch is unit-testable with a stub and the local/unknown branches never force the singleton;
    * the public overloads bind the production [[KubernetesClient]]. (Metrics has no analogous seam:
    * its per-unit lookup already fans out to the whole namespace and the bulk (unit, podMetrics)
    * overload already covers the cpu/memory resolution, so nothing there is worth pinning.)
    */
  private[util] def singleUnitStatusAndReason(
      unit: WorkflowComputingUnit,
      k8s: => KubernetesClient
  ): (ComputingUnitState, Option[String]) = {
    unit.getType match {
      // Local CUs are always “running”
      case WorkflowComputingUnitTypeEnum.local =>
        (Running, None)

      // Kubernetes CUs – resolved from the pod's status snapshot
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val client = k8s
        kubernetesStatusAndReason(
          client
            .getPodByName(client.generatePodName(unit.getCuid))
            .map(PodStatusSnapshot.fromPod)
        )

      // Any other (unknown) type is treated as pending
      case _ =>
        (Pending, None)
    }
  }

  // User-facing wording for each failure mode. Deliberately actionable prose, never a raw
  // Kubernetes dump; buildDashboardUnit withholds these unless the caller permits access.
  private val ImagePullWaitingReasons = Set("ImagePullBackOff", "ErrImagePull", "InvalidImageName")
  private val EvictedDiskReason =
    "The computing unit was evicted because it ran out of local disk storage. Consider " +
      "storing less data on the unit's local file system, or recreate it with more storage."
  private val ImagePullReason =
    "The computing unit's image could not be pulled. Please recreate the unit or contact " +
      "an administrator."
  private val CrashLoopOomReason =
    "The computing unit keeps crashing because it runs out of memory. Please terminate it " +
      "and recreate it with a higher memory limit."
  private val GenericFailedReason =
    "The computing unit stopped unexpectedly. Please terminate and recreate it, or contact " +
      "an administrator."
  private val UnknownStateReason =
    "The state of the computing unit cannot be determined (its node may be unreachable)."
  private val UnschedulableReason =
    "The computing unit is waiting for cluster resources to become available."

  private def evictedReason(podMessage: Option[String]): String = {
    val mentionsDisk =
      podMessage.exists { message =>
        val lower = message.toLowerCase
        lower.contains("ephemeral") || lower.contains("disk")
      }
    if (mentionsDisk) EvictedDiskReason
    else {
      // First sentence of the cluster's message, capped so the tooltip stays readable.
      val shortReason = podMessage
        .map(_.takeWhile(_ != '.').trim)
        .filter(_.nonEmpty)
        .map(sentence => if (sentence.length > 120) sentence.take(120).trim + "..." else sentence)
        .getOrElse("Evicted")
      s"The computing unit was evicted by the cluster ($shortReason). Consider recreating it."
    }
  }

  private def crashLoopReason(restartCount: Int): String =
    s"The computing unit is repeatedly crashing (restarted $restartCount times). Please " +
      "terminate and recreate it, or contact an administrator."

  private def recoveredOomWarning(restartCount: Int): String =
    s"The last run was terminated because the computing unit ran out of memory (restarted " +
      s"$restartCount times). Consider recreating the unit with a higher memory limit before " +
      "running the same workload."

  /**
    * Pure (snapshot -> state, reason) mapping, mirroring the Kubernetes pod lifecycle. An absent
    * pod stays Pending — exactly today's behavior — because the vanish reconciliation, not this
    * mapping, is what retires units whose pods are gone.
    *
    * Note the restartPolicy-Always subtlety: an OOM-killed container restarts in place with the
    * pod phase still "Running", so OOM kills and crash loops are read from the container-level
    * fields, and a waiting-state failure takes precedence over the recovered-OOM warning.
    */
  private[util] def kubernetesStatusAndReason(
      snapshotOpt: Option[PodStatusSnapshot]
  ): (ComputingUnitState, Option[String]) =
    snapshotOpt match {
      case None => (Pending, None)
      case Some(snapshot) =>
        val phase = snapshot.phase.getOrElse("")
        val imagePullFailed =
          snapshot.containers.exists(_.waitingReason.exists(ImagePullWaitingReasons.contains))
        val crashLooping = snapshot.containers.find(_.waitingReason.contains("CrashLoopBackOff"))
        val oomKilled = snapshot.containers.find(_.lastTerminatedReason.contains("OOMKilled"))

        if (snapshot.terminating)
          (Terminating, None)
        else if (phase == "Failed" && snapshot.podReason.contains("Evicted"))
          (Failed, Some(evictedReason(snapshot.podMessage)))
        else if (imagePullFailed)
          (Failed, Some(ImagePullReason))
        else if (crashLooping.isDefined) {
          val container = crashLooping.get
          if (container.lastTerminatedReason.contains("OOMKilled"))
            (Failed, Some(CrashLoopOomReason))
          else
            (Failed, Some(crashLoopReason(container.restartCount)))
        } else if (phase == "Failed")
          (Failed, Some(GenericFailedReason))
        else if (phase == "Unknown")
          (Unknown, Some(UnknownStateReason))
        else if (phase == "Pending" && snapshot.unschedulable)
          (Pending, Some(UnschedulableReason))
        else if (phase == "Running")
          (Running, oomKilled.map(container => recoveredOomWarning(container.restartCount)))
        else
          (Pending, None)
    }

  def getComputingUnitMetrics(unit: WorkflowComputingUnit): WorkflowComputingUnitMetrics = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val metrics = KubernetesClient.getPodMetrics(unit.getCuid)
        WorkflowComputingUnitMetrics(
          metrics.getOrElse("cpu", ""),
          metrics.getOrElse("memory", "")
        )
      case _ =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
    }
  }

  /**
    * Resolves status (and its user-facing reason) from a pre-fetched pod-snapshot map instead
    * of a per-unit cluster call, so a listing costs O(1) round trips rather than one per unit.
    */
  def getComputingUnitStatusAndReason(
      unit: WorkflowComputingUnit,
      podSnapshots: Map[String, PodStatusSnapshot]
  ): (ComputingUnitState, Option[String]) = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        (Running, None)
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        kubernetesStatusAndReason(podSnapshots.get(KubernetesClient.generatePodName(unit.getCuid)))
      case _ =>
        (Pending, None)
    }
  }

  /** Resolves metrics from a pre-fetched pod-metrics map instead of a per-unit cluster call. */
  def getComputingUnitMetrics(
      unit: WorkflowComputingUnit,
      podMetrics: Map[String, Map[String, String]]
  ): WorkflowComputingUnitMetrics = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val metrics = podMetrics
          .getOrElse(KubernetesClient.generatePodName(unit.getCuid), Map.empty[String, String])
        WorkflowComputingUnitMetrics(
          metrics.getOrElse("cpu", ""),
          metrics.getOrElse("memory", "")
        )
      case _ =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
    }
  }

  private def isKubernetes(unit: WorkflowComputingUnit): Boolean =
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.kubernetes => true
      case _                                        => false
    }

  // Pod snapshots/metrics for the namespace; skipped (empty) when no Kubernetes unit is present,
  // so a cluster-free listing issues no round trip. Same seam as singleUnitStatusAndReason: the
  // public overload binds the production singleton, passing it by-name to the private[util]
  // overload, which forces it only inside the guard's true branch — so the empty path never
  // touches the client, and tests drive the private overload with a stub.
  def podSnapshotsFor(units: Seq[WorkflowComputingUnit]): Map[String, PodStatusSnapshot] =
    podSnapshotsFor(units, KubernetesClient)

  private[util] def podSnapshotsFor(
      units: Seq[WorkflowComputingUnit],
      k8s: => KubernetesClient
  ): Map[String, PodStatusSnapshot] =
    if (units.exists(isKubernetes)) k8s.getAllPodStatusSnapshots else Map.empty

  def podMetricsFor(units: Seq[WorkflowComputingUnit]): Map[String, Map[String, String]] =
    podMetricsFor(units, KubernetesClient)

  private[util] def podMetricsFor(
      units: Seq[WorkflowComputingUnit],
      k8s: => KubernetesClient
  ): Map[String, Map[String, String]] =
    if (units.exists(isKubernetes)) k8s.getAllPodMetrics else Map.empty

  /** A Kubernetes unit whose pod is absent from `podSnapshots` (deleted or TTL GC-ed). */
  private def isVanished(
      unit: WorkflowComputingUnit,
      podSnapshots: Map[String, PodStatusSnapshot]
  ): Boolean =
    isKubernetes(unit) && !podSnapshots.contains(KubernetesClient.generatePodName(unit.getCuid))

  /** Partition into `(live, vanished)` by `podSnapshots`. Pure (no I/O), so it is unit-testable. */
  def partitionLiveUnits(
      units: List[WorkflowComputingUnit],
      podSnapshots: Map[String, PodStatusSnapshot]
  ): (List[WorkflowComputingUnit], List[WorkflowComputingUnit]) =
    units.partition(unit => !isVanished(unit, podSnapshots))

  /**
    * Stamp `terminateTime` on vanished Kubernetes units (one batched update) and return the live
    * ones. Shared by both listing endpoints so they agree on when a unit is terminated.
    */
  def reconcileVanishedKubernetesUnits(
      dao: WorkflowComputingUnitDao,
      units: List[WorkflowComputingUnit],
      podSnapshots: Map[String, PodStatusSnapshot]
  ): List[WorkflowComputingUnit] = {
    val partitioned = partitionLiveUnits(units, podSnapshots)
    val vanished = partitioned._2
    if (vanished.nonEmpty) {
      val now = new Timestamp(System.currentTimeMillis())
      vanished.foreach(_.setTerminateTime(now))
      dao.update(vanished.asJava)
    }
    partitioned._1
  }

  /**
    * Build one dashboard row; status/metrics come from the pre-fetched maps (no per-unit K8s
    * call). Shared by both listing endpoints so row shape and owner-info fallback stay identical.
    * Status-reason visibility is separate from ownership: regular-user callers permit owners,
    * while the admin listing permits every row. Shared non-admin users get a bare status and the
    * frontend shows a generic "unavailable" instead.
    */
  def buildDashboardUnit(
      unit: WorkflowComputingUnit,
      isOwner: Boolean,
      canViewStatusReason: Boolean,
      accessPrivilege: EnumType,
      ownerInfo: Map[Integer, (String, String)],
      podSnapshots: Map[String, PodStatusSnapshot],
      podMetrics: Map[String, Map[String, String]]
  ): DashboardWorkflowComputingUnit = {
    val owner = ownerInfo.getOrElse(unit.getUid, (null, null))
    val (status, statusReason) = getComputingUnitStatusAndReason(unit, podSnapshots)
    DashboardWorkflowComputingUnit(
      computingUnit = unit,
      status = status.toString,
      statusReason = if (canViewStatusReason) statusReason else None,
      metrics = getComputingUnitMetrics(unit, podMetrics),
      isOwner = isOwner,
      accessPrivilege = accessPrivilege,
      ownerAvatar = owner._1,
      ownerName = owner._2
    )
  }
}
