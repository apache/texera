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

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowComputingUnit}
import org.apache.texera.service.resource.ComputingUnitManagingResource.WorkflowComputingUnitMetrics
import org.apache.texera.service.resource.ComputingUnitState.{Pending, Running}
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
    u.setPassword("password")
    u.setGoogleAvatar(avatar)
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

  // ── Bulk variants: unknown (untyped) branch ──────────────────────────

  "getComputingUnitStatus(unit, podPhases)" should "return Pending for an unknown (untyped) unit" in {
    ComputingUnitHelpers.getComputingUnitStatus(untypedUnit(), Map.empty) shouldBe Pending
  }

  "getComputingUnitMetrics(unit, podMetrics)" should "return NaN for an unknown (untyped) unit" in {
    ComputingUnitHelpers.getComputingUnitMetrics(untypedUnit(), Map.empty) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
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
    val podPhases = Map(KubernetesClient.generatePodName(600) -> "Running")
    val live =
      ComputingUnitHelpers.reconcileVanishedKubernetesUnits(
        computingUnitDao,
        List(present, gone, local),
        podPhases
      )

    live.map(_.getCuid) should contain theSameElementsAs Seq(600, 602)
    computingUnitDao.fetchOneByCuid(601).getTerminateTime should not be null
    computingUnitDao.fetchOneByCuid(600).getTerminateTime shouldBe null
    computingUnitDao.fetchOneByCuid(602).getTerminateTime shouldBe null
  }
}
