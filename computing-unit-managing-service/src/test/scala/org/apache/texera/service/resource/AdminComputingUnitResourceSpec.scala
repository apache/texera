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

import jakarta.annotation.security.RolesAllowed
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowComputingUnit}
import org.apache.texera.service.util.KubernetesClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class AdminComputingUnitResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()

  override protected def afterAll(): Unit = shutdownDB()

  private def makeUser(uid: Int, name: String): User = {
    val u = new User()
    u.setUid(uid)
    u.setName(name)
    u.setEmail(s"user$uid@example.com")
    u.setRole(UserRoleEnum.ADMIN)
    u.setPassword("password")
    u.setGoogleAvatar(s"avatar-$uid")
    u
  }

  private def makeUnit(
      cuid: Int,
      uid: Int,
      name: String,
      tpe: WorkflowComputingUnitTypeEnum
  ): WorkflowComputingUnit = {
    val u = new WorkflowComputingUnit()
    u.setCuid(cuid)
    u.setUid(uid)
    u.setName(name)
    u.setType(tpe)
    u
  }

  private def localUnit(cuid: Int, uid: Int, name: String): WorkflowComputingUnit =
    makeUnit(cuid, uid, name, WorkflowComputingUnitTypeEnum.local)

  private def kubernetesUnit(cuid: Int, uid: Int, name: String): WorkflowComputingUnit =
    makeUnit(cuid, uid, name, WorkflowComputingUnitTypeEnum.kubernetes)

  // The class-level @RolesAllowed(Array("ADMIN")) is what makes Jersey's
  // RolesAllowedDynamicFeature return 403 for any non-ADMIN (e.g. REGULAR) caller.
  "AdminComputingUnitResource" should "only permit the ADMIN role (non-ADMIN callers are rejected)" in {
    val annotation = classOf[AdminComputingUnitResource].getAnnotation(classOf[RolesAllowed])
    annotation should not be null
    annotation.value.toSeq shouldBe Seq("ADMIN")
  }

  "buildDashboardUnits" should "render active units owned by multiple users with their owner info" in {
    val units = List(
      localUnit(cuid = 1, uid = 100, name = "alice-cu"),
      localUnit(cuid = 2, uid = 200, name = "bob-cu")
    )
    val ownerInfo: Map[Integer, (String, String)] = Map(
      (100: Integer) -> ("alice-avatar", "alice"),
      (200: Integer) -> (null, "bob")
    )

    // caller is an admin who owns none of these units
    val result = AdminComputingUnitResource.buildDashboardUnits(
      units,
      ownerInfo,
      callerUid = 999,
      podPhases = Map.empty,
      podMetrics = Map.empty
    )

    result.map(_.computingUnit.getCuid) should contain theSameElementsAs Seq(1, 2)
    result.map(_.computingUnit.getUid).distinct should contain theSameElementsAs Seq(100, 200)

    val byCuid = result.map(u => u.computingUnit.getCuid.intValue() -> u).toMap
    byCuid(1).ownerName shouldBe "alice"
    byCuid(1).ownerGoogleAvatar shouldBe "alice-avatar"
    byCuid(2).ownerName shouldBe "bob"
    byCuid(2).ownerGoogleAvatar shouldBe null

    // local units report Running/NaN and the admin caller does not own them
    all(result.map(_.status)) shouldBe "Running"
    all(result.map(_.isOwner)) shouldBe false
    all(result.map(_.accessPrivilege)) shouldBe PrivilegeEnum.WRITE
  }

  it should "set isOwner only for units owned by the caller" in {
    val units = List(
      localUnit(cuid = 1, uid = 100, name = "owned-by-caller"),
      localUnit(cuid = 2, uid = 200, name = "owned-by-other")
    )

    val result = AdminComputingUnitResource
      .buildDashboardUnits(
        units,
        ownerInfo = Map.empty,
        callerUid = 100,
        podPhases = Map.empty,
        podMetrics = Map.empty
      )
      .map(u => u.computingUnit.getCuid.intValue() -> u.isOwner)
      .toMap

    result(1) shouldBe true
    result(2) shouldBe false
  }

  it should "fall back to null owner info when the owner is missing from the map" in {
    val units = List(localUnit(cuid = 1, uid = 100, name = "orphan-cu"))

    val result = AdminComputingUnitResource.buildDashboardUnits(
      units,
      ownerInfo = Map.empty,
      callerUid = 999,
      podPhases = Map.empty,
      podMetrics = Map.empty
    )

    result.head.ownerName shouldBe null
    result.head.ownerGoogleAvatar shouldBe null
  }

  it should "resolve kubernetes status and metrics from the pre-fetched maps" in {
    val unit = kubernetesUnit(cuid = 5, uid = 300, name = "k8s-cu")
    val podName = KubernetesClient.generatePodName(5)

    val podPhases = Map(podName -> "Running")
    val podMetrics = Map(podName -> Map("cpu" -> "250m", "memory" -> "128Mi"))

    val result = AdminComputingUnitResource.buildDashboardUnits(
      List(unit),
      ownerInfo = Map((300: Integer) -> ("k8s-avatar", "k8s-owner")),
      callerUid = 999,
      podPhases = podPhases,
      podMetrics = podMetrics
    )

    result.head.status shouldBe "Running"
    result.head.metrics.cpuUsage shouldBe "250m"
    result.head.metrics.memoryUsage shouldBe "128Mi"
  }

  it should "report a kubernetes unit as Pending with empty metrics when its pod is absent from the maps" in {
    val unit = kubernetesUnit(cuid = 6, uid = 300, name = "k8s-no-pod")

    val result = AdminComputingUnitResource.buildDashboardUnits(
      List(unit),
      ownerInfo = Map.empty,
      callerUid = 999,
      podPhases = Map.empty,
      podMetrics = Map.empty
    )

    result.head.status shouldBe "Pending"
    result.head.metrics.cpuUsage shouldBe ""
    result.head.metrics.memoryUsage shouldBe ""
  }

  "listAllComputingUnits" should "return every non-terminated unit across users, marked WRITE" in {
    val userDao = new UserDao(getDSLContext.configuration())
    val unitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    val admin = makeUser(700, "admin")
    userDao.insert(admin)
    userDao.insert(makeUser(701, "other"))
    unitDao.insert(localUnit(cuid = 700, uid = 700, name = "admin-cu"))
    unitDao.insert(localUnit(cuid = 701, uid = 701, name = "other-cu"))
    // A terminated unit must be excluded by the SQL filter.
    val terminated = localUnit(cuid = 702, uid = 701, name = "terminated-cu")
    terminated.setTerminateTime(new Timestamp(0L))
    unitDao.insert(terminated)

    val result = new AdminComputingUnitResource().listAllComputingUnits(new SessionUser(admin))

    result.map(_.computingUnit.getCuid.intValue()) should contain theSameElementsAs Seq(700, 701)
    all(result.map(_.accessPrivilege)) shouldBe PrivilegeEnum.WRITE
    all(result.map(_.status)) shouldBe "Running" // local units
    val byCuid = result.map(r => r.computingUnit.getCuid.intValue() -> r).toMap
    byCuid(700).isOwner shouldBe true // owned by the requesting admin
    byCuid(700).ownerName shouldBe "admin"
    byCuid(701).isOwner shouldBe false
    byCuid(701).ownerName shouldBe "other"
  }
}
