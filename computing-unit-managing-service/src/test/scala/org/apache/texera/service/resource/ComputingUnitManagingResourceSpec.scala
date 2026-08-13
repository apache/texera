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

import jakarta.ws.rs.{BadRequestException, ForbiddenException, NotFoundException}
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.KubernetesConfig.maxNumOfRunningComputingUnitsPerUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  ComputingUnitUserAccessDao,
  UserDao,
  WorkflowComputingUnitDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  ComputingUnitUserAccess,
  User,
  WorkflowComputingUnit
}
import org.apache.texera.service.resource.ComputingUnitManagingResource.{
  WorkflowComputingUnitCreationParams,
  WorkflowComputingUnitMetrics,
  WorkflowComputingUnitResourceLimit
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.CollectionHasAsScala

// Drives the per-user computing-unit endpoints against the embedded database using
// local units (so no Kubernetes calls are made).
class ComputingUnitManagingResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val uid = 800
  private lazy val user: SessionUser = {
    val u = new User()
    u.setUid(uid)
    u.setName("owner")
    u.setEmail("owner@example.com")
    u.setRole(UserRoleEnum.REGULAR)
    u.setAvatar("owner-avatar")
    new SessionUser(u)
  }

  // Fixtures for the terminate tests. The units acted on are owned by "victim" (uid 901), not by
  // `user` (uid 800), so they never show up in listComputingUnits(user)'s exact-set assertion.
  private def makeUser(id: Int, name: String, role: UserRoleEnum): User = {
    val u = new User()
    u.setUid(id)
    u.setName(name)
    u.setEmail(s"$name@example.com")
    u.setRole(role)
    // Credentials live in auth_provider now, and this spec exercises computing-unit ownership
    // rather than login, so the user needs none.
    u
  }
  private lazy val adminUser: SessionUser =
    new SessionUser(makeUser(900, "admin", UserRoleEnum.ADMIN))
  private lazy val strangerUser: SessionUser =
    new SessionUser(makeUser(902, "stranger", UserRoleEnum.REGULAR))
  // Dedicated owner for the create tests, so the units they create never show up in
  // listComputingUnits(user)'s exact-set assertion.
  private lazy val creatorUser: SessionUser =
    new SessionUser(makeUser(903, "creator", UserRoleEnum.REGULAR))

  private def localUnit(cuid: Int, name: String): WorkflowComputingUnit =
    localUnitOwnedBy(cuid, uid, name)

  private def localUnitOwnedBy(cuid: Int, ownerUid: Int, name: String): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit()
    unit.setCuid(cuid)
    unit.setUid(ownerUid)
    unit.setName(name)
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    unit
  }

  private def insertLocalUnit(cuid: Int, ownerUid: Int, name: String): Unit =
    new WorkflowComputingUnitDao(getDSLContext.configuration())
      .insert(localUnitOwnedBy(cuid, ownerUid, name))

  private def isTerminated(cuid: Int): Boolean =
    new WorkflowComputingUnitDao(getDSLContext.configuration())
      .fetchOneByCuid(cuid)
      .getTerminateTime != null

  private def unitName(cuid: Int): String =
    new WorkflowComputingUnitDao(getDSLContext.configuration())
      .fetchOneByCuid(cuid)
      .getName

  private def runningUnitCountOwnedBy(ownerUid: Int): Int =
    new WorkflowComputingUnitDao(getDSLContext.configuration())
      .fetchByUid(ownerUid)
      .asScala
      .count(_.getTerminateTime == null)

  /** Inserts an access row directly, bypassing the grant endpoint, to set up the rename tests. */
  private def grantAccess(cuid: Int, granteeUid: Int, privilege: PrivilegeEnum): Unit = {
    val access = new ComputingUnitUserAccess
    access.setCuid(cuid)
    access.setUid(granteeUid)
    access.setPrivilege(privilege)
    new ComputingUnitUserAccessDao(getDSLContext.configuration()).insert(access)
  }

  // The cpu/memory/gpu/jvm/shm quantities are ignored for local units, so the create tests only
  // vary the fields the local branch actually reads: name, unitType and uri.
  private def localCreationParams(
      name: String,
      uri: Option[String]
  ): WorkflowComputingUnitCreationParams =
    WorkflowComputingUnitCreationParams(
      name = name,
      unitType = "local",
      cpuLimit = "NaN",
      memoryLimit = "NaN",
      gpuLimit = "NaN",
      jvmMemorySize = "NaN",
      shmSize = "NaN",
      uri = uri
    )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(user.getUser)
    userDao.insert(adminUser.getUser)
    userDao.insert(makeUser(901, "victim", UserRoleEnum.REGULAR))
    userDao.insert(strangerUser.getUser)
    userDao.insert(creatorUser.getUser)
    val unitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    unitDao.insert(localUnit(800, "cu-a"))
    unitDao.insert(localUnit(801, "cu-b"))
  }

  override protected def afterAll(): Unit =
    try shutdownDB()
    finally super.afterAll()

  private val resource = new ComputingUnitManagingResource

  "getComputingUnitInfo" should "return the owner's local unit with WRITE access and Running status" in {
    val info = resource.getComputingUnitInfo(800, user)

    info.computingUnit.getCuid shouldBe 800
    info.status shouldBe "Running"
    info.metrics shouldBe WorkflowComputingUnitMetrics("NaN", "NaN")
    info.isOwner shouldBe true
    info.accessPrivilege shouldBe PrivilegeEnum.WRITE
    info.ownerName shouldBe "owner"
    info.ownerGoogleAvatar shouldBe "owner-avatar"
  }

  it should "report READ access for a grantee holding READ access" in {
    insertLocalUnit(cuid = 950, ownerUid = 901, name = "shared-info-cu")
    grantAccess(950, granteeUid = 902, PrivilegeEnum.READ)

    val info = resource.getComputingUnitInfo(950, strangerUser)

    info.computingUnit.getCuid.intValue() shouldBe 950
    info.isOwner shouldBe false
    info.accessPrivilege shouldBe PrivilegeEnum.READ
    info.ownerName shouldBe "victim"
  }

  it should "throw NotFoundException for a nonexistent unit" in {
    a[NotFoundException] should be thrownBy resource.getComputingUnitInfo(99999, user)
  }

  "getComputingUnitMetricsEndpoint" should "return NaN metrics for an owned local unit" in {
    resource.getComputingUnitMetricsEndpoint("800", user) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
  }

  it should "reject a non-owner with BadRequestException" in {
    a[BadRequestException] should be thrownBy
      resource.getComputingUnitMetricsEndpoint("800", strangerUser)
  }

  "listComputingUnits" should "return the caller's owned, non-terminated units" in {
    val result = resource.listComputingUnits(user)

    result.map(_.computingUnit.getCuid.intValue()) should contain theSameElementsAs Seq(800, 801)
    all(result.map(_.isOwner)) shouldBe true
    all(result.map(_.accessPrivilege)) shouldBe PrivilegeEnum.WRITE
    all(result.map(_.status)) shouldBe "Running"
  }

  "terminateComputingUnit" should "let an admin terminate a unit it does not own" in {
    insertLocalUnit(cuid = 910, ownerUid = 901, name = "victim-cu")

    val response = resource.terminateComputingUnit(910, adminUser)

    response.getStatus shouldBe 200
    isTerminated(910) shouldBe true
  }

  it should "reject a non-admin acting on a unit it does not own with 400 and not terminate it" in {
    insertLocalUnit(cuid = 911, ownerUid = 901, name = "victim-cu-2")

    val response = resource.terminateComputingUnit(911, strangerUser)

    response.getStatus shouldBe 400
    isTerminated(911) shouldBe false
  }

  it should "let an owner terminate its own unit" in {
    insertLocalUnit(cuid = 912, ownerUid = 902, name = "stranger-own-cu")

    val response = resource.terminateComputingUnit(912, strangerUser)

    response.getStatus shouldBe 200
    isTerminated(912) shouldBe true
  }

  it should "reject a WRITE grantee with 400 and not terminate the unit" in {
    // Unlike rename, terminate requires strict ownership (or the ADMIN role), so
    // even a WRITE grantee is rejected.
    insertLocalUnit(cuid = 913, ownerUid = 901, name = "grantee-cannot-terminate")
    grantAccess(913, granteeUid = 902, PrivilegeEnum.WRITE)

    val response = resource.terminateComputingUnit(913, strangerUser)

    response.getStatus shouldBe 400
    isTerminated(913) shouldBe false
  }

  it should "return 404 when an admin terminates a nonexistent unit" in {
    a[NotFoundException] should be thrownBy resource.terminateComputingUnit(99999, adminUser)
  }

  // The kubernetes type is disabled in the test JVM (kubernetes.conf's enabled flag defaults to
  // false and is a load-time val), so createWorkflowComputingUnit rejects it at the supported-type
  // gate. The kubernetes-only validation behind that gate (cpu/memory/gpu limit options, shm
  // parsing and bounds, JVM-heap bound, and the running-unit quota throw) is unreachable here.

  "createWorkflowComputingUnit" should "create a local unit and report it as owned, Running and writable" in {
    val uri = "http://localhost:8085"

    val created =
      resource.createWorkflowComputingUnit(localCreationParams("local-cu", Some(uri)), creatorUser)

    val cuid = created.computingUnit.getCuid
    cuid.intValue() should be > 0
    created.computingUnit.getUid.intValue() shouldBe 903
    created.computingUnit.getName shouldBe "local-cu"
    created.computingUnit.getType shouldBe WorkflowComputingUnitTypeEnum.local
    created.computingUnit.getUri shouldBe uri
    created.computingUnit.getResource should include(s""""nodeAddresses":["$uri"]""")
    created.computingUnit.getResource should include(""""cpuLimit":"NaN"""")
    created.status shouldBe "Running"
    created.metrics shouldBe WorkflowComputingUnitMetrics("NaN", "NaN")
    created.isOwner shouldBe true
    created.accessPrivilege shouldBe PrivilegeEnum.WRITE
    created.ownerName shouldBe "creator"
    // makeUser sets no avatar, so the owner-avatar lookup resolves to null
    created.ownerGoogleAvatar shouldBe null

    // The unit is persisted, not just echoed back
    unitName(cuid) shouldBe "local-cu"
  }

  it should "reject a whitespace-only name with ForbiddenException and store nothing" in {
    val before = runningUnitCountOwnedBy(903)

    val ex = the[ForbiddenException] thrownBy
      resource.createWorkflowComputingUnit(
        localCreationParams("   ", Some("http://localhost:8085")),
        creatorUser
      )

    ex.getMessage should include("name cannot be empty")
    runningUnitCountOwnedBy(903) shouldBe before
  }

  it should "reject an unknown unit type with ForbiddenException" in {
    val params = localCreationParams("unknown-type-cu", Some("http://localhost:8085"))
      .copy(unitType = "quantum")

    val ex = the[ForbiddenException] thrownBy
      resource.createWorkflowComputingUnit(params, creatorUser)

    ex.getMessage should include("Unit type 'quantum' is not allowed")
  }

  it should "reject the kubernetes type while it is disabled in the configuration" in {
    val params = localCreationParams("k8s-cu", None).copy(unitType = "kubernetes")

    val ex = the[ForbiddenException] thrownBy
      resource.createWorkflowComputingUnit(params, creatorUser)

    ex.getMessage should include("Unit type 'kubernetes' is not allowed")
  }

  it should "reject a local unit without a URI" in {
    val ex = the[ForbiddenException] thrownBy
      resource.createWorkflowComputingUnit(localCreationParams("no-uri-cu", None), creatorUser)

    ex.getMessage should include("URI is required")
  }

  it should "reject a local unit whose URI is blank" in {
    val ex = the[ForbiddenException] thrownBy
      resource.createWorkflowComputingUnit(
        localCreationParams("blank-uri-cu", Some("   ")),
        creatorUser
      )

    ex.getMessage should include("URI is required")
  }

  it should "not apply the kubernetes running-unit quota to local units" in {
    // Fill the creator's quota with directly inserted local units (cuids in the 930s)
    val base = runningUnitCountOwnedBy(903)
    ((base + 1) to maxNumOfRunningComputingUnitsPerUser).foreach { i =>
      insertLocalUnit(cuid = 930 + i, ownerUid = 903, name = s"quota-filler-$i")
    }
    runningUnitCountOwnedBy(903) should be >= maxNumOfRunningComputingUnitsPerUser

    val created = resource.createWorkflowComputingUnit(
      localCreationParams("over-quota-local", Some("http://localhost:8085")),
      creatorUser
    )

    created.computingUnit.getName shouldBe "over-quota-local"
    unitName(created.computingUnit.getCuid) shouldBe "over-quota-local"
  }

  // Fixtures for the rename tests live in the 920s and are owned by "victim" (901) or
  // "stranger" (902), so they never show up in listComputingUnits(user)'s exact-set assertion.

  "renameComputingUnit" should "let the owner rename its unit" in {
    insertLocalUnit(cuid = 920, ownerUid = 902, name = "before-rename")

    val response = resource.renameComputingUnit(920, "after-rename", strangerUser)

    response.getStatus shouldBe 200
    unitName(920) shouldBe "after-rename"
  }

  it should "reject a non-owner without granted access with 403 and keep the name" in {
    insertLocalUnit(cuid = 921, ownerUid = 901, name = "victim-name")

    val response = resource.renameComputingUnit(921, "hijacked", strangerUser)

    response.getStatus shouldBe 403
    unitName(921) shouldBe "victim-name"
  }

  it should "reject a grantee holding only READ access with 403 and keep the name" in {
    insertLocalUnit(cuid = 922, ownerUid = 901, name = "read-only-name")
    grantAccess(922, granteeUid = 902, PrivilegeEnum.READ)

    val response = resource.renameComputingUnit(922, "hijacked", strangerUser)

    response.getStatus shouldBe 403
    unitName(922) shouldBe "read-only-name"
  }

  it should "let a grantee holding WRITE access rename the unit" in {
    insertLocalUnit(cuid = 923, ownerUid = 901, name = "shared-name")
    grantAccess(923, granteeUid = 902, PrivilegeEnum.WRITE)

    val response = resource.renameComputingUnit(923, "renamed-by-grantee", strangerUser)

    response.getStatus shouldBe 200
    unitName(923) shouldBe "renamed-by-grantee"
  }

  it should "reject a blank name with 400 and keep the name" in {
    insertLocalUnit(cuid = 924, ownerUid = 902, name = "keep-me")

    val response = resource.renameComputingUnit(924, "   ", strangerUser)

    response.getStatus shouldBe 400
    unitName(924) shouldBe "keep-me"
  }

  it should "reject an admin who neither owns nor was granted access with 403" in {
    // Unlike terminate, rename has no ADMIN bypass: the role is never consulted.
    insertLocalUnit(cuid = 951, ownerUid = 901, name = "admin-cannot-rename")

    val response = resource.renameComputingUnit(951, "hijacked", adminUser)

    response.getStatus shouldBe 403
    unitName(951) shouldBe "admin-cannot-rename"
  }

  it should "return 404 for a nonexistent unit" in {
    a[NotFoundException] should be thrownBy
      resource.renameComputingUnit(99999, "new-name", strangerUser)
  }

  it should "propagate a database failure and keep the name" in {
    insertLocalUnit(cuid = 925, ownerUid = 902, name = "short-name")
    val oversized = "x" * 200 // the name column is VARCHAR(128)

    // The endpoint's `return Response(500)` inside the withTransaction lambda is a Scala
    // non-local return (a ControlThrowable, not a RuntimeException), so jOOQ's transaction
    // wraps it in DataAccessException("Rollback caused") and the intended 500 response is
    // never produced; this pins the actual behavior.
    a[org.jooq.exception.DataAccessException] should be thrownBy
      resource.renameComputingUnit(925, oversized, strangerUser)

    unitName(925) shouldBe "short-name"
  }

  "getComputingUnitTypes" should "list exactly the local type while kubernetes is disabled" in {
    resource.getComputingUnitTypes(user).typeOptions shouldBe List("local")
  }

  "getComputingUnitLimitOptions" should "return the configured cpu, memory and gpu options" in {
    // The values are kubernetes.conf's defaults; the test JVM does not override them.
    val options = resource.getComputingUnitLimitOptions(user)

    options.cpuLimitOptions shouldBe List("1", "2", "4")
    options.memoryLimitOptions shouldBe List("1Gi", "2Gi", "4Gi")
    options.gpuLimitOptions shouldBe List("0", "1", "2")
  }

  "getComputingUnitResourceLimit" should "return NaN limits for an owned local unit" in {
    resource.getComputingUnitResourceLimit("800", user) shouldBe
      WorkflowComputingUnitResourceLimit("NaN", "NaN", "NaN")
  }

  it should "reject a non-owner with BadRequestException" in {
    a[BadRequestException] should be thrownBy
      resource.getComputingUnitResourceLimit("800", strangerUser)
  }

  it should "throw NotFoundException for a nonexistent unit" in {
    a[NotFoundException] should be thrownBy
      resource.getComputingUnitResourceLimit("99999", user)
  }
}
