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

import jakarta.ws.rs.core.Response
import jakarta.ws.rs.{BadRequestException, ForbiddenException}
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jooq.{DSLContext, Record}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

/**
  * The access cases [[DatasetAccessResourceSpec]] and [[ModelAccessResourceSpec]] share.
  *
  * Datasets and models apply the same access rules through [[ResourceAccess]], but each exposes
  * its own helpers and endpoints. A subclass seeds its resource and binds every case to those
  * helpers and endpoints, so both APIs stay covered case by case. Explicit grants are seeded,
  * read and cleared through the resource's [[ResourceTables]]. Test names keep each resource's
  * own helper names (`isDatasetPublic`, `getOwnerEmailOfModel`, ...), as the separate specs did.
  *
  * @param tables the resource's tables
  */
abstract class AccessResourceBehaviors[R <: Record, A <: Record](tables: ResourceTables[R, A])
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val label = tables.label
  private val Label = label.capitalize

  /** Inserts one resource owned by `ownerUid` and returns its id. */
  protected def insertResource(
      name: String,
      description: String,
      isPublic: Boolean,
      ownerUid: Integer
  ): Integer

  // the resource's privilege helpers
  protected def resourceIsPublic(ctx: DSLContext, id: Integer): Boolean
  protected def userOwnsResource(ctx: DSLContext, id: Integer, uid: Integer): Boolean
  protected def userPrivilege(ctx: DSLContext, id: Integer, uid: Integer): PrivilegeEnum
  protected def userCanRead(ctx: DSLContext, id: Integer, uid: Integer): Boolean
  protected def userCanWrite(ctx: DSLContext, id: Integer, uid: Integer): Boolean
  protected def resourceOwner(ctx: DSLContext, id: Integer): User

  // the resource's endpoints
  protected def grant(id: Integer, email: String, privilege: String, user: SessionUser): Response
  protected def revoke(id: Integer, email: String, user: SessionUser): Response
  protected def accessListOf(
      id: Integer,
      user: SessionUser
  ): java.util.List[ResourceAccess.AccessEntry]
  protected def ownerEmail(id: Integer, user: SessionUser): String

  private def regularUser(name: String): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val ownerUser = regularUser(s"${label}_owner")
  private val readGranteeUser = regularUser("read_grantee")
  private val writeGranteeUser = regularUser("write_grantee")
  private val strangerUser = regularUser("stranger")

  private var privateId: Integer = _
  private var publicId: Integer = _
  private val nonExistentId: Integer = 999999

  private lazy val ownerSession = new SessionUser(ownerUser)
  private lazy val writeGranteeSession = new SessionUser(writeGranteeUser)
  private lazy val readGranteeSession = new SessionUser(readGranteeUser)
  private lazy val strangerSession = new SessionUser(strangerUser)

  private def grantDirectly(id: Integer, uid: Integer, privilege: PrivilegeEnum): Unit =
    getDSLContext
      .insertInto(tables.accessTable)
      .set(tables.accessIdField, id)
      .set(tables.accessUidField, uid)
      .set(tables.privilegeField, privilege)
      .execute()

  /** The privilege on the user's explicit access row. */
  private def explicitPrivilege(id: Integer, uid: Integer): PrivilegeEnum =
    getDSLContext
      .select(tables.privilegeField)
      .from(tables.accessTable)
      .where(tables.accessIdField.eq(id).and(tables.accessUidField.eq(uid)))
      .fetchOneInto(classOf[PrivilegeEnum])

  private def accessList(
      id: Integer,
      user: SessionUser = ownerSession
  ): List[ResourceAccess.AccessEntry] =
    accessListOf(id, user).asScala.toList

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(readGranteeUser)
    userDao.insert(writeGranteeUser)
    userDao.insert(strangerUser)

    privateId = insertResource(
      s"private-$label",
      s"private $label for access tests",
      isPublic = false,
      ownerUser.getUid
    )
    publicId = insertResource(
      s"public-$label",
      s"public $label for access tests",
      isPublic = true,
      ownerUser.getUid
    )
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // every test starts with no explicit grants
    getDSLContext.deleteFrom(tables.accessTable).execute()
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  // ===========================================================================
  // Privilege helpers
  // ===========================================================================

  s"is${Label}Public" should s"be true for a public $label and false for a private one" in {
    resourceIsPublic(getDSLContext, publicId) shouldBe true
    resourceIsPublic(getDSLContext, privateId) shouldBe false
  }

  s"userOwn$Label" should "be true only for the owner" in {
    userOwnsResource(getDSLContext, privateId, ownerUser.getUid) shouldBe true
    userOwnsResource(getDSLContext, privateId, strangerUser.getUid) shouldBe false
  }

  s"get${Label}UserAccessPrivilege" should "return NONE for a user without an explicit grant" in {
    userPrivilege(getDSLContext, privateId, strangerUser.getUid) shouldEqual PrivilegeEnum.NONE
  }

  it should "return the granted privilege for a grantee" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)
    grantDirectly(privateId, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    userPrivilege(getDSLContext, privateId, readGranteeUser.getUid) shouldEqual PrivilegeEnum.READ
    userPrivilege(getDSLContext, privateId, writeGranteeUser.getUid) shouldEqual PrivilegeEnum.WRITE
  }

  "the owner" should s"have both read and write access to the $label" in {
    userCanRead(getDSLContext, privateId, ownerUser.getUid) shouldBe true
    userCanWrite(getDSLContext, privateId, ownerUser.getUid) shouldBe true
  }

  "a READ grantee" should "have read but not write access" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    userCanRead(getDSLContext, privateId, readGranteeUser.getUid) shouldBe true
    userCanWrite(getDSLContext, privateId, readGranteeUser.getUid) shouldBe false
  }

  "a WRITE grantee" should "have both read and write access" in {
    grantDirectly(privateId, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    userCanRead(getDSLContext, privateId, writeGranteeUser.getUid) shouldBe true
    userCanWrite(getDSLContext, privateId, writeGranteeUser.getUid) shouldBe true
  }

  "a user with no grant" should s"have no access to a private $label" in {
    userCanRead(getDSLContext, privateId, strangerUser.getUid) shouldBe false
    userCanWrite(getDSLContext, privateId, strangerUser.getUid) shouldBe false
  }

  it should s"have read but not write access to a public $label" in {
    userCanRead(getDSLContext, publicId, strangerUser.getUid) shouldBe true
    userCanWrite(getDSLContext, publicId, strangerUser.getUid) shouldBe false
  }

  it should s"have no explicit privilege row on a public $label" in {
    // public read access comes from is_public, not from a <resource>_user_access row
    userPrivilege(getDSLContext, publicId, strangerUser.getUid) shouldEqual PrivilegeEnum.NONE
  }

  s"an explicit WRITE grant on a public $label" should "give a non-owner write access" in {
    grantDirectly(publicId, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    userCanWrite(getDSLContext, publicId, writeGranteeUser.getUid) shouldBe true
  }

  "the privilege helpers" should s"treat a nonexistent $label as private, unowned, and ungranted" in {
    resourceIsPublic(getDSLContext, nonExistentId) shouldBe false
    userOwnsResource(getDSLContext, nonExistentId, ownerUser.getUid) shouldBe false
    userPrivilege(getDSLContext, nonExistentId, ownerUser.getUid) shouldEqual PrivilegeEnum.NONE
    userCanRead(getDSLContext, nonExistentId, ownerUser.getUid) shouldBe false
    userCanWrite(getDSLContext, nonExistentId, ownerUser.getUid) shouldBe false
  }

  "getOwner" should "return the owning user" in {
    resourceOwner(getDSLContext, privateId).getEmail shouldEqual ownerUser.getEmail
  }

  it should s"return null for a nonexistent $label" in {
    resourceOwner(getDSLContext, nonExistentId) shouldBe null
  }

  // ===========================================================================
  // grantAccess / getAccessList
  // ===========================================================================

  "grantAccess" should "add a grantee that appears in the access list with the granted privilege" in {
    val response = grant(privateId, readGranteeUser.getEmail, "READ", ownerSession)
    response.getStatus shouldEqual 200

    val entries = accessList(privateId)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
    entries.head.name shouldEqual readGranteeUser.getName
    entries.head.privilege shouldEqual PrivilegeEnum.READ
  }

  it should "reject granting to a placeholder account" in {
    val placeholderEmail = s"$label-placeholder@test.com"
    val placeholder = new User
    placeholder.setName(s"${label}_placeholder")
    placeholder.setEmail(placeholderEmail)
    placeholder.setRole(UserRoleEnum.INACTIVE)
    placeholder.setIsPlaceholder(true)
    new UserDao(getDSLContext.configuration()).insert(placeholder)

    assertThrows[BadRequestException] {
      grant(privateId, placeholderEmail, "READ", ownerSession)
    }
    accessList(privateId) shouldBe empty
  }

  it should "reject granting to an unknown email" in {
    assertThrows[BadRequestException] {
      grant(privateId, "nobody@test.com", "READ", ownerSession)
    }
    accessList(privateId) shouldBe empty
  }

  it should "update the privilege in place when re-granting with a different privilege" in {
    grant(privateId, readGranteeUser.getEmail, "READ", ownerSession)
    grant(privateId, readGranteeUser.getEmail, "WRITE", ownerSession)

    val entries = accessList(privateId)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
    entries.head.privilege shouldEqual PrivilegeEnum.WRITE
  }

  it should s"allow a WRITE grantee to share the $label" in {
    grantDirectly(privateId, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    val response = grant(privateId, strangerUser.getEmail, "READ", writeGranteeSession)
    response.getStatus shouldEqual 200

    userCanRead(getDSLContext, privateId, strangerUser.getUid) shouldBe true
  }

  it should "refuse to change the owner's privilege" in {
    grantDirectly(privateId, ownerUser.getUid, PrivilegeEnum.WRITE)
    grantDirectly(privateId, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    val ex = intercept[ForbiddenException] {
      grant(privateId, ownerUser.getEmail, "READ", writeGranteeSession)
    }
    ex.getResponse.getStatus shouldEqual 403
    explicitPrivilege(privateId, ownerUser.getUid) shouldEqual PrivilegeEnum.WRITE
  }

  it should "be forbidden for a user without write access" in {
    val ex = intercept[ForbiddenException] {
      grant(privateId, readGranteeUser.getEmail, "READ", strangerSession)
    }
    ex.getResponse.getStatus shouldEqual 403
    ex.getMessage should include(s"You do not have permission to modify $label $privateId")
  }

  it should "be forbidden for a READ grantee" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    assertThrows[ForbiddenException] {
      grant(privateId, strangerUser.getEmail, "READ", readGranteeSession)
    }
  }

  "getAccessList" should "return an empty list when no access has been granted" in {
    accessList(privateId) shouldBe empty
  }

  it should "not include the owner's own access row" in {
    // even if the owner somehow has an explicit access row, the list only shows other users
    grantDirectly(privateId, ownerUser.getUid, PrivilegeEnum.WRITE)
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    val entries = accessList(privateId)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
  }

  it should "list multiple grantees with their respective privileges" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)
    grantDirectly(privateId, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    val entries = accessList(privateId)
    entries should have size 2
    val privilegeByEmail = entries.map(entry => entry.email -> entry.privilege).toMap
    privilegeByEmail(readGranteeUser.getEmail) shouldEqual PrivilegeEnum.READ
    privilegeByEmail(writeGranteeUser.getEmail) shouldEqual PrivilegeEnum.WRITE
  }

  // ===========================================================================
  // revokeAccess
  // ===========================================================================

  "revokeAccess" should "remove the grantee from the access list and drop their access" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    val response = revoke(privateId, readGranteeUser.getEmail, ownerSession)
    response.getStatus shouldEqual 200

    accessList(privateId) shouldBe empty
    userPrivilege(getDSLContext, privateId, readGranteeUser.getUid) shouldEqual PrivilegeEnum.NONE
    userCanRead(getDSLContext, privateId, readGranteeUser.getUid) shouldBe false
  }

  it should "allow a WRITE grantee to revoke another user's access" in {
    grantDirectly(privateId, writeGranteeUser.getUid, PrivilegeEnum.WRITE)
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    val response = revoke(privateId, readGranteeUser.getEmail, writeGranteeSession)
    response.getStatus shouldEqual 200

    userCanRead(getDSLContext, privateId, readGranteeUser.getUid) shouldBe false
  }

  it should "succeed as a no-op when the target user has no explicit grant" in {
    val response = revoke(privateId, strangerUser.getEmail, ownerSession)
    response.getStatus shouldEqual 200
    accessList(privateId) shouldBe empty
  }

  it should "refuse to revoke the owner's access" in {
    grantDirectly(privateId, ownerUser.getUid, PrivilegeEnum.WRITE)
    grantDirectly(privateId, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    val ex = intercept[ForbiddenException] {
      revoke(privateId, ownerUser.getEmail, writeGranteeSession)
    }
    ex.getResponse.getStatus shouldEqual 403
    explicitPrivilege(privateId, ownerUser.getUid) shouldEqual PrivilegeEnum.WRITE
  }

  it should "be forbidden for a user without write access" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    assertThrows[ForbiddenException] {
      revoke(privateId, readGranteeUser.getEmail, strangerSession)
    }
  }

  // ===========================================================================
  // getOwnerEmailOf<Resource>
  // ===========================================================================

  s"getOwnerEmailOf$Label" should "return the owner's email" in {
    ownerEmail(privateId, ownerSession) shouldEqual ownerUser.getEmail
  }

  it should "be readable by a READ grantee" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    ownerEmail(privateId, readGranteeSession) shouldEqual ownerUser.getEmail
  }

  it should s"be forbidden for a user with no access to a private $label" in {
    assertThrows[ForbiddenException] {
      ownerEmail(privateId, strangerSession)
    }
  }

  it should s"be readable by anyone for a public $label" in {
    ownerEmail(publicId, strangerSession) shouldEqual ownerUser.getEmail
  }

  it should s"be forbidden for a nonexistent $label" in {
    assertThrows[ForbiddenException] {
      ownerEmail(nonExistentId, ownerSession)
    }
  }

  // ===========================================================================
  // getAccessList -- read guard
  // ===========================================================================

  "getAccessList" should s"be forbidden for a user with no access to a private $label" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    assertThrows[ForbiddenException] {
      accessList(privateId, strangerSession)
    }
  }

  it should "be readable by a READ grantee" in {
    grantDirectly(privateId, readGranteeUser.getUid, PrivilegeEnum.READ)

    accessList(privateId, readGranteeSession).map(_.email) should contain(
      readGranteeUser.getEmail
    )
  }
}
