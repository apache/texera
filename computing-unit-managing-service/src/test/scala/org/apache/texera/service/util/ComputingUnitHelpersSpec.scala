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

import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, WorkflowComputingUnitTypeEnum}
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import org.apache.texera.service.resource.ComputingUnitManagingResource.WorkflowComputingUnitMetrics
import org.apache.texera.service.resource.ComputingUnitState.{Pending, Running}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ComputingUnitHelpersSpec extends AnyFlatSpec with Matchers {

  private def localUnit(cuid: Int = 0, uid: Int = 0): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit()
    unit.setCuid(cuid)
    unit.setUid(uid)
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    unit
  }

  private def kubernetesUnit(cuid: Int, uid: Int = 0): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit()
    unit.setCuid(cuid)
    unit.setUid(uid)
    unit.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    unit
  }

  // WorkflowComputingUnitTypeEnum only defines `local` and `kubernetes`, so an
  // untyped unit (getType == null) is what exercises the pure "unknown" branch.
  private def untypedUnit(): WorkflowComputingUnit = new WorkflowComputingUnit()

  "getComputingUnitStatus" should "return Running for a local unit" in {
    ComputingUnitHelpers.getComputingUnitStatus(localUnit()) shouldBe Running
  }

  it should "return Pending for an unknown (untyped) unit" in {
    ComputingUnitHelpers.getComputingUnitStatus(untypedUnit()) shouldBe Pending
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

  "getComputingUnitStatus(unit, podPhases)" should "return Running for a local unit" in {
    ComputingUnitHelpers.getComputingUnitStatus(localUnit(), Map.empty) shouldBe Running
  }

  it should "return Running for a kubernetes unit whose pod phase is Running" in {
    val unit = kubernetesUnit(7)
    val podPhases = Map(KubernetesClient.generatePodName(7) -> "Running")
    ComputingUnitHelpers.getComputingUnitStatus(unit, podPhases) shouldBe Running
  }

  it should "return Pending for a kubernetes unit whose pod is absent or not Running" in {
    val unit = kubernetesUnit(8)
    ComputingUnitHelpers.getComputingUnitStatus(unit, Map.empty) shouldBe Pending
    ComputingUnitHelpers.getComputingUnitStatus(
      unit,
      Map(KubernetesClient.generatePodName(8) -> "Pending")
    ) shouldBe Pending
  }

  it should "treat a null phase as not Running" in {
    val unit = kubernetesUnit(9)
    val podPhases = Map(KubernetesClient.generatePodName(9) -> (null: String))
    ComputingUnitHelpers.getComputingUnitStatus(unit, podPhases) shouldBe Pending
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
    val podPhases = Map(KubernetesClient.generatePodName(20) -> "Running")

    val (live, vanished) = ComputingUnitHelpers.partitionLiveUnits(List(present, gone), podPhases)

    live.map(_.getCuid) shouldBe List(20)
    vanished.map(_.getCuid) shouldBe List(21)
  }

  // ── buildDashboardUnit ───────────────────────────────────────────────

  "buildDashboardUnit" should "populate the row from the caller flags and pre-fetched maps" in {
    val unit = kubernetesUnit(cuid = 30, uid = 100)
    val podName = KubernetesClient.generatePodName(30)

    val row = ComputingUnitHelpers.buildDashboardUnit(
      unit,
      isOwner = true,
      accessPrivilege = PrivilegeEnum.READ,
      ownerInfo = Map((100: Integer) -> ("avatar", "owner")),
      podPhases = Map(podName -> "Running"),
      podMetrics = Map(podName -> Map("cpu" -> "100m", "memory" -> "64Mi"))
    )

    row.computingUnit.getCuid shouldBe 30
    row.isOwner shouldBe true
    row.accessPrivilege shouldBe PrivilegeEnum.READ
    row.status shouldBe "Running"
    row.metrics shouldBe WorkflowComputingUnitMetrics("100m", "64Mi")
    row.ownerGoogleAvatar shouldBe "avatar"
    row.ownerName shouldBe "owner"
  }

  it should "fall back to null owner info when the owner is missing from the map" in {
    val row = ComputingUnitHelpers.buildDashboardUnit(
      localUnit(cuid = 31, uid = 200),
      isOwner = false,
      accessPrivilege = PrivilegeEnum.WRITE,
      ownerInfo = Map.empty,
      podPhases = Map.empty,
      podMetrics = Map.empty
    )

    row.ownerGoogleAvatar shouldBe null
    row.ownerName shouldBe null
    row.status shouldBe "Running"
    row.metrics shouldBe WorkflowComputingUnitMetrics("NaN", "NaN")
  }
}
