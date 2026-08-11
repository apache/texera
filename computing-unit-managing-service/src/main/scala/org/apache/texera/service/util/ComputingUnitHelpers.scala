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
import org.apache.texera.service.resource.ComputingUnitState.{ComputingUnitState, Pending, Running}
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

  def getComputingUnitStatus(unit: WorkflowComputingUnit): ComputingUnitState =
    singleUnitStatus(unit, KubernetesClient)

  /**
    * Single-unit status via a per-unit pod lookup (a targeted GET, cheaper than listing the whole
    * namespace). The client is a by-name parameter — not the global singleton — so the kubernetes
    * branch is unit-testable with a stub and the local/unknown branches never force the singleton;
    * the public overload binds the production [[KubernetesClient]]. (Metrics has no analogous seam:
    * its per-unit lookup already fans out to the whole namespace and the bulk (unit, podMetrics)
    * overload already covers the cpu/memory resolution, so nothing there is worth pinning.)
    */
  private[util] def singleUnitStatus(
      unit: WorkflowComputingUnit,
      k8s: => KubernetesClient
  ): ComputingUnitState = {
    unit.getType match {
      // Local CUs are always “running”
      case WorkflowComputingUnitTypeEnum.local =>
        Running

      // Kubernetes CUs – only explicit “Running” counts as running
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        // Guard the pod status the same way the bulk getAllPodPhases does: a pod with no
        // status yet has a null getStatus, so map through Option to avoid an NPE.
        val client = k8s
        val phaseOpt = client
          .getPodByName(client.generatePodName(unit.getCuid))
          .flatMap(pod => Option(pod.getStatus).map(_.getPhase))

        if (phaseOpt.contains("Running")) Running else Pending

      // Any other (unknown) type is treated as pending
      case _ =>
        Pending
    }
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
    * Resolves status from a pre-fetched pod-phase map instead of a per-unit cluster call, so a
    * listing costs O(1) round trips rather than one per unit.
    */
  def getComputingUnitStatus(
      unit: WorkflowComputingUnit,
      podPhases: Map[String, String]
  ): ComputingUnitState = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        Running
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        // A missing entry or null phase both count as not-Running.
        if (podPhases.get(KubernetesClient.generatePodName(unit.getCuid)).contains("Running"))
          Running
        else Pending
      case _ =>
        Pending
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

  // Pod phases/metrics for the namespace; skipped (empty) when no Kubernetes unit is present, so a
  // cluster-free listing issues no round trip. Same seam as singleUnitStatus: the public overload
  // binds the production singleton, passing it by-name to the private[util] overload, which forces
  // it only inside the guard's true branch — so the empty path never touches the client, and tests
  // drive the private overload with a stub.
  def podPhasesFor(units: Seq[WorkflowComputingUnit]): Map[String, String] =
    podPhasesFor(units, KubernetesClient)

  private[util] def podPhasesFor(
      units: Seq[WorkflowComputingUnit],
      k8s: => KubernetesClient
  ): Map[String, String] =
    if (units.exists(isKubernetes)) k8s.getAllPodPhases else Map.empty

  def podMetricsFor(units: Seq[WorkflowComputingUnit]): Map[String, Map[String, String]] =
    podMetricsFor(units, KubernetesClient)

  private[util] def podMetricsFor(
      units: Seq[WorkflowComputingUnit],
      k8s: => KubernetesClient
  ): Map[String, Map[String, String]] =
    if (units.exists(isKubernetes)) k8s.getAllPodMetrics else Map.empty

  /** A Kubernetes unit whose pod is absent from `podPhases` (deleted or TTL GC-ed). */
  private def isVanished(unit: WorkflowComputingUnit, podPhases: Map[String, String]): Boolean =
    isKubernetes(unit) && !podPhases.contains(KubernetesClient.generatePodName(unit.getCuid))

  /** Partition into `(live, vanished)` by `podPhases`. Pure (no I/O), so it is unit-testable. */
  def partitionLiveUnits(
      units: List[WorkflowComputingUnit],
      podPhases: Map[String, String]
  ): (List[WorkflowComputingUnit], List[WorkflowComputingUnit]) =
    units.partition(unit => !isVanished(unit, podPhases))

  /**
    * Stamp `terminateTime` on vanished Kubernetes units (one batched update) and return the live
    * ones. Shared by both listing endpoints so they agree on when a unit is terminated.
    */
  def reconcileVanishedKubernetesUnits(
      dao: WorkflowComputingUnitDao,
      units: List[WorkflowComputingUnit],
      podPhases: Map[String, String]
  ): List[WorkflowComputingUnit] = {
    val partitioned = partitionLiveUnits(units, podPhases)
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
    */
  def buildDashboardUnit(
      unit: WorkflowComputingUnit,
      isOwner: Boolean,
      accessPrivilege: EnumType,
      ownerInfo: Map[Integer, (String, String)],
      podPhases: Map[String, String],
      podMetrics: Map[String, Map[String, String]]
  ): DashboardWorkflowComputingUnit = {
    val owner = ownerInfo.getOrElse(unit.getUid, (null, null))
    DashboardWorkflowComputingUnit(
      computingUnit = unit,
      status = getComputingUnitStatus(unit, podPhases).toString,
      metrics = getComputingUnitMetrics(unit, podMetrics),
      isOwner = isOwner,
      accessPrivilege = accessPrivilege,
      ownerGoogleAvatar = owner._1,
      ownerName = owner._2
    )
  }
}
