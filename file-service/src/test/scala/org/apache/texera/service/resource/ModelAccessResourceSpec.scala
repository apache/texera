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

import jakarta.ws.rs.{BadRequestException, ForbiddenException}
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.ModelUserAccess.MODEL_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.daos.{ModelDao, ModelUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Model, ModelUserAccess, User}
import org.apache.texera.service.resource.ModelAccessResource.{
  getModelUserAccessPrivilege,
  getOwner,
  isModelPublic,
  userHasReadAccess,
  userHasWriteAccess,
  userOwnModel
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

class ModelAccessResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val ownerUser: User = {
    val user = new User
    user.setName("model_owner")
    user.setEmail("model_owner@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val readGranteeUser: User = {
    val user = new User
    user.setName("read_grantee")
    user.setEmail("read_grantee@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val writeGranteeUser: User = {
    val user = new User
    user.setName("write_grantee")
    user.setEmail("write_grantee@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val strangerUser: User = {
    val user = new User
    user.setName("stranger")
    user.setEmail("stranger@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val privateModel: Model = {
    val model = new Model
    model.setName("private-model")
    model.setRepositoryName("private-model")
    model.setIsPublic(false)
    model.setIsDownloadable(true)
    model.setDescription("private model for access tests")
    model.setFramework("pytorch")
    model
  }

  private val publicModel: Model = {
    val model = new Model
    model.setName("public-model")
    model.setRepositoryName("public-model")
    model.setIsPublic(true)
    model.setIsDownloadable(true)
    model.setDescription("public model for access tests")
    model.setFramework("pytorch")
    model
  }

  private val nonExistentMid: Integer = 999999

  lazy val accessResource = new ModelAccessResource()

  lazy val ownerSession = new SessionUser(ownerUser)
  lazy val writeGranteeSession = new SessionUser(writeGranteeUser)
  lazy val readGranteeSession = new SessionUser(readGranteeUser)
  lazy val strangerSession = new SessionUser(strangerUser)

  private def grantDirectly(mid: Integer, uid: Integer, privilege: PrivilegeEnum): Unit = {
    new ModelUserAccessDao(getDSLContext.configuration())
      .insert(new ModelUserAccess(mid, uid, privilege))
  }

  private def accessList(mid: Integer): List[ModelAccessResource.AccessEntry] =
    accessResource.getAccessList(mid).asScala.toList

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(readGranteeUser)
    userDao.insert(writeGranteeUser)
    userDao.insert(strangerUser)

    privateModel.setOwnerUid(ownerUser.getUid)
    publicModel.setOwnerUid(ownerUser.getUid)
    val modelDao = new ModelDao(getDSLContext.configuration())
    modelDao.insert(privateModel)
    modelDao.insert(publicModel)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // every test starts with no explicit grants
    getDSLContext.deleteFrom(MODEL_USER_ACCESS).execute()
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  // ===========================================================================
  // Privilege helpers
  // ===========================================================================

  "isModelPublic" should "be true for a public model and false for a private one" in {
    isModelPublic(getDSLContext, publicModel.getMid) shouldBe true
    isModelPublic(getDSLContext, privateModel.getMid) shouldBe false
  }

  "userOwnModel" should "be true only for the owner" in {
    userOwnModel(getDSLContext, privateModel.getMid, ownerUser.getUid) shouldBe true
    userOwnModel(getDSLContext, privateModel.getMid, strangerUser.getUid) shouldBe false
  }

  "getModelUserAccessPrivilege" should "return NONE for a user without an explicit grant" in {
    getModelUserAccessPrivilege(
      getDSLContext,
      privateModel.getMid,
      strangerUser.getUid
    ) shouldEqual PrivilegeEnum.NONE
  }

  it should "return the granted privilege for a grantee" in {
    grantDirectly(privateModel.getMid, readGranteeUser.getUid, PrivilegeEnum.READ)
    grantDirectly(privateModel.getMid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    getModelUserAccessPrivilege(
      getDSLContext,
      privateModel.getMid,
      readGranteeUser.getUid
    ) shouldEqual PrivilegeEnum.READ
    getModelUserAccessPrivilege(
      getDSLContext,
      privateModel.getMid,
      writeGranteeUser.getUid
    ) shouldEqual PrivilegeEnum.WRITE
  }

  "the owner" should "have both read and write access to the model" in {
    userHasReadAccess(getDSLContext, privateModel.getMid, ownerUser.getUid) shouldBe true
    userHasWriteAccess(getDSLContext, privateModel.getMid, ownerUser.getUid) shouldBe true
  }

  "a READ grantee" should "have read but not write access" in {
    grantDirectly(privateModel.getMid, readGranteeUser.getUid, PrivilegeEnum.READ)

    userHasReadAccess(getDSLContext, privateModel.getMid, readGranteeUser.getUid) shouldBe true
    userHasWriteAccess(getDSLContext, privateModel.getMid, readGranteeUser.getUid) shouldBe false
  }

  "a WRITE grantee" should "have both read and write access" in {
    grantDirectly(privateModel.getMid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    userHasReadAccess(getDSLContext, privateModel.getMid, writeGranteeUser.getUid) shouldBe true
    userHasWriteAccess(getDSLContext, privateModel.getMid, writeGranteeUser.getUid) shouldBe true
  }

  "a user with no grant" should "have no access to a private model" in {
    userHasReadAccess(getDSLContext, privateModel.getMid, strangerUser.getUid) shouldBe false
    userHasWriteAccess(getDSLContext, privateModel.getMid, strangerUser.getUid) shouldBe false
  }

  it should "have read but not write access to a public model" in {
    userHasReadAccess(getDSLContext, publicModel.getMid, strangerUser.getUid) shouldBe true
    userHasWriteAccess(getDSLContext, publicModel.getMid, strangerUser.getUid) shouldBe false
  }

  it should "have no explicit privilege row on a public model" in {
    // public read access comes from is_public, not from a model_user_access row
    getModelUserAccessPrivilege(
      getDSLContext,
      publicModel.getMid,
      strangerUser.getUid
    ) shouldEqual PrivilegeEnum.NONE
  }

  "an explicit WRITE grant on a public model" should "give a non-owner write access" in {
    grantDirectly(publicModel.getMid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    userHasWriteAccess(getDSLContext, publicModel.getMid, writeGranteeUser.getUid) shouldBe true
  }

  "the privilege helpers" should "treat a nonexistent model as private, unowned, and ungranted" in {
    isModelPublic(getDSLContext, nonExistentMid) shouldBe false
    userOwnModel(getDSLContext, nonExistentMid, ownerUser.getUid) shouldBe false
    getModelUserAccessPrivilege(
      getDSLContext,
      nonExistentMid,
      ownerUser.getUid
    ) shouldEqual PrivilegeEnum.NONE
    userHasReadAccess(getDSLContext, nonExistentMid, ownerUser.getUid) shouldBe false
    userHasWriteAccess(getDSLContext, nonExistentMid, ownerUser.getUid) shouldBe false
  }

  "getOwner" should "return the owning user" in {
    getOwner(getDSLContext, privateModel.getMid).getEmail shouldEqual ownerUser.getEmail
  }

  it should "return null for a nonexistent model" in {
    getOwner(getDSLContext, nonExistentMid) shouldBe null
  }

  // ===========================================================================
  // grantAccess / getAccessList
  // ===========================================================================

  "grantAccess" should "add a grantee that appears in the access list with the granted privilege" in {
    val response = accessResource.grantAccess(
      privateModel.getMid,
      readGranteeUser.getEmail,
      "READ",
      ownerSession
    )
    response.getStatus shouldEqual 200

    val entries = accessList(privateModel.getMid)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
    entries.head.name shouldEqual readGranteeUser.getName
    entries.head.privilege shouldEqual PrivilegeEnum.READ
  }

  it should "reject granting to a placeholder account" in {
    val placeholder = new User
    placeholder.setName("model_placeholder")
    placeholder.setEmail("model-placeholder@test.com")
    placeholder.setRole(UserRoleEnum.INACTIVE)
    placeholder.setIsPlaceholder(true)
    new UserDao(getDSLContext.configuration()).insert(placeholder)

    assertThrows[BadRequestException] {
      accessResource.grantAccess(
        privateModel.getMid,
        "model-placeholder@test.com",
        "READ",
        ownerSession
      )
    }
    accessList(privateModel.getMid) shouldBe empty
  }

  it should "reject granting to an unknown email" in {
    assertThrows[BadRequestException] {
      accessResource.grantAccess(
        privateModel.getMid,
        "nobody@test.com",
        "READ",
        ownerSession
      )
    }
    accessList(privateModel.getMid) shouldBe empty
  }

  it should "update the privilege in place when re-granting with a different privilege" in {
    accessResource.grantAccess(
      privateModel.getMid,
      readGranteeUser.getEmail,
      "READ",
      ownerSession
    )
    accessResource.grantAccess(
      privateModel.getMid,
      readGranteeUser.getEmail,
      "WRITE",
      ownerSession
    )

    val entries = accessList(privateModel.getMid)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
    entries.head.privilege shouldEqual PrivilegeEnum.WRITE
  }

  it should "allow a WRITE grantee to share the model" in {
    grantDirectly(privateModel.getMid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    val response = accessResource.grantAccess(
      privateModel.getMid,
      strangerUser.getEmail,
      "READ",
      writeGranteeSession
    )
    response.getStatus shouldEqual 200

    userHasReadAccess(getDSLContext, privateModel.getMid, strangerUser.getUid) shouldBe true
  }

  it should "be forbidden for a user without write access" in {
    val ex = intercept[ForbiddenException] {
      accessResource.grantAccess(
        privateModel.getMid,
        readGranteeUser.getEmail,
        "READ",
        strangerSession
      )
    }
    ex.getResponse.getStatus shouldEqual 403
    ex.getMessage should include(
      s"You do not have permission to modify model ${privateModel.getMid}"
    )
  }

  it should "be forbidden for a READ grantee" in {
    grantDirectly(privateModel.getMid, readGranteeUser.getUid, PrivilegeEnum.READ)

    assertThrows[ForbiddenException] {
      accessResource.grantAccess(
        privateModel.getMid,
        strangerUser.getEmail,
        "READ",
        readGranteeSession
      )
    }
  }

  "getAccessList" should "return an empty list when no access has been granted" in {
    accessList(privateModel.getMid) shouldBe empty
  }

  it should "not include the owner's own access row" in {
    // even if the owner somehow has an explicit access row, the list only shows other users
    grantDirectly(privateModel.getMid, ownerUser.getUid, PrivilegeEnum.WRITE)
    grantDirectly(privateModel.getMid, readGranteeUser.getUid, PrivilegeEnum.READ)

    val entries = accessList(privateModel.getMid)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
  }

  it should "list multiple grantees with their respective privileges" in {
    grantDirectly(privateModel.getMid, readGranteeUser.getUid, PrivilegeEnum.READ)
    grantDirectly(privateModel.getMid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    val entries = accessList(privateModel.getMid)
    entries should have size 2
    val privilegeByEmail = entries.map(entry => entry.email -> entry.privilege).toMap
    privilegeByEmail(readGranteeUser.getEmail) shouldEqual PrivilegeEnum.READ
    privilegeByEmail(writeGranteeUser.getEmail) shouldEqual PrivilegeEnum.WRITE
  }

  // ===========================================================================
  // revokeAccess
  // ===========================================================================

  "revokeAccess" should "remove the grantee from the access list and drop their access" in {
    grantDirectly(privateModel.getMid, readGranteeUser.getUid, PrivilegeEnum.READ)

    val response = accessResource.revokeAccess(
      privateModel.getMid,
      readGranteeUser.getEmail,
      ownerSession
    )
    response.getStatus shouldEqual 200

    accessList(privateModel.getMid) shouldBe empty
    getModelUserAccessPrivilege(
      getDSLContext,
      privateModel.getMid,
      readGranteeUser.getUid
    ) shouldEqual PrivilegeEnum.NONE
    userHasReadAccess(getDSLContext, privateModel.getMid, readGranteeUser.getUid) shouldBe false
  }

  it should "allow a WRITE grantee to revoke another user's access" in {
    grantDirectly(privateModel.getMid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)
    grantDirectly(privateModel.getMid, readGranteeUser.getUid, PrivilegeEnum.READ)

    val response = accessResource.revokeAccess(
      privateModel.getMid,
      readGranteeUser.getEmail,
      writeGranteeSession
    )
    response.getStatus shouldEqual 200

    userHasReadAccess(getDSLContext, privateModel.getMid, readGranteeUser.getUid) shouldBe false
  }

  it should "succeed as a no-op when the target user has no explicit grant" in {
    val response = accessResource.revokeAccess(
      privateModel.getMid,
      strangerUser.getEmail,
      ownerSession
    )
    response.getStatus shouldEqual 200
    accessList(privateModel.getMid) shouldBe empty
  }

  it should "be forbidden for a user without write access" in {
    grantDirectly(privateModel.getMid, readGranteeUser.getUid, PrivilegeEnum.READ)

    assertThrows[ForbiddenException] {
      accessResource.revokeAccess(
        privateModel.getMid,
        readGranteeUser.getEmail,
        strangerSession
      )
    }
  }

  // ===========================================================================
  // getOwnerEmailOfModel
  // ===========================================================================

  "getOwnerEmailOfModel" should "return the owner's email" in {
    accessResource.getOwnerEmailOfModel(privateModel.getMid) shouldEqual ownerUser.getEmail
  }

  it should "return an empty string for a nonexistent model" in {
    accessResource.getOwnerEmailOfModel(nonExistentMid) shouldEqual ""
  }
}
