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
    * Resolve owner display info for the given owner uids in a single `fetchByUid` call,
    * keyed by uid. Blank avatars/names collapse to `null` so callers can pass them straight
    * into the dashboard shape. Returns an empty map when `uids` is empty (no query issued).
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
          val avatar = Option(u.getGoogleAvatar).filter(_.nonEmpty).orNull
          val name = Option(u.getName).filter(_.nonEmpty).orNull
          u.getUid -> (avatar, name)
        }
        .toMap
  }

  def getComputingUnitStatus(unit: WorkflowComputingUnit): ComputingUnitState = {
    unit.getType match {
      // Local CUs are always “running”
      case WorkflowComputingUnitTypeEnum.local =>
        Running

      // Kubernetes CUs – only explicit “Running” counts as running
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val phaseOpt = KubernetesClient
          .getPodByName(KubernetesClient.generatePodName(unit.getCuid))
          .map(_.getStatus.getPhase)

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
    * Bulk variant of [[getComputingUnitStatus]]: resolves a unit's status from a pre-fetched
    * map of pod name -> phase (see [[KubernetesClient.getAllPodPhases]]) instead of issuing a
    * per-unit Kubernetes call. Used by listings that resolve many units at once so the number
    * of cluster round trips is O(1) rather than O(units).
    */
  def getComputingUnitStatus(
      unit: WorkflowComputingUnit,
      podPhases: Map[String, String]
  ): ComputingUnitState = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        Running
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        // A missing entry or a null phase both yield `false` here (null != "Running").
        if (podPhases.get(KubernetesClient.generatePodName(unit.getCuid)).contains("Running"))
          Running
        else Pending
      case _ =>
        Pending
    }
  }

  /**
    * Bulk variant of [[getComputingUnitMetrics]]: resolves a unit's metrics from a pre-fetched
    * map of pod name -> (metric -> value) (see [[KubernetesClient.getAllPodMetrics]]) instead of
    * re-fetching the whole namespace metrics list per unit.
    */
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

  /**
    * A Kubernetes unit is considered "vanished" when its pod is absent from the pre-fetched
    * `podPhases` map (manually deleted or TTL GC-ed by the cluster). Local/other units always
    * count as present.
    */
  private def isVanished(unit: WorkflowComputingUnit, podPhases: Map[String, String]): Boolean =
    unit.getType == WorkflowComputingUnitTypeEnum.kubernetes &&
      !podPhases.contains(KubernetesClient.generatePodName(unit.getCuid))

  /**
    * Split `units` into `(live, vanished)` using a pre-fetched pod-phase map (see
    * [[KubernetesClient.getAllPodPhases]]). Pure — does no I/O — so it can be unit-tested.
    */
  def partitionLiveUnits(
      units: List[WorkflowComputingUnit],
      podPhases: Map[String, String]
  ): (List[WorkflowComputingUnit], List[WorkflowComputingUnit]) = {
    val (vanished, live) = units.partition(isVanished(_, podPhases))
    (live, vanished)
  }

  /**
    * Reconcile a set of units against the cluster: any Kubernetes unit whose pod has vanished is
    * stamped with `terminateTime` and persisted in a single batched update, then the surviving
    * (live) units are returned. Shared by the per-user and admin listing endpoints so both agree
    * on when a unit is treated as terminated.
    */
  def reconcileVanishedKubernetesUnits(
      dao: WorkflowComputingUnitDao,
      units: List[WorkflowComputingUnit],
      podPhases: Map[String, String]
  ): List[WorkflowComputingUnit] = {
    val (live, vanished) = partitionLiveUnits(units, podPhases)
    if (vanished.nonEmpty) {
      val now = new Timestamp(System.currentTimeMillis())
      vanished.foreach(_.setTerminateTime(now))
      dao.update(vanished.asJava)
    }
    live
  }

  /**
    * Assemble a single dashboard row from a unit, its caller-relative ownership/privilege, and
    * the pre-fetched owner-info/pod maps. Kubernetes status and metrics are resolved from the
    * maps (no per-unit Kubernetes call). Shared by both listing endpoints so the row shape and
    * owner-info fallback stay identical.
    */
  def buildDashboardUnit(
      unit: WorkflowComputingUnit,
      isOwner: Boolean,
      accessPrivilege: EnumType,
      ownerInfo: Map[Integer, (String, String)],
      podPhases: Map[String, String],
      podMetrics: Map[String, Map[String, String]]
  ): DashboardWorkflowComputingUnit = {
    val (avatar, name) = ownerInfo.getOrElse(unit.getUid, (null, null))
    DashboardWorkflowComputingUnit(
      computingUnit = unit,
      status = getComputingUnitStatus(unit, podPhases).toString,
      metrics = getComputingUnitMetrics(unit, podMetrics),
      isOwner = isOwner,
      accessPrivilege = accessPrivilege,
      ownerGoogleAvatar = avatar,
      ownerName = name
    )
  }
}
