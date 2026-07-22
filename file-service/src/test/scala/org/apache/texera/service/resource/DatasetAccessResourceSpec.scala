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

import jakarta.ws.rs.ForbiddenException
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, DatasetUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetUserAccess, User}
import org.apache.texera.service.resource.DatasetAccessResource.{
  getDatasetUserAccessPrivilege,
  getOwner,
  isDatasetPublic,
  userHasReadAccess,
  userHasWriteAccess,
  userOwnDataset
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

class DatasetAccessResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val ownerUser: User = {
    val user = new User
    user.setName("dataset_owner")
    user.setPassword("123")
    user.setEmail("dataset_owner@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val readGranteeUser: User = {
    val user = new User
    user.setName("read_grantee")
    user.setPassword("123")
    user.setEmail("read_grantee@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val writeGranteeUser: User = {
    val user = new User
    user.setName("write_grantee")
    user.setPassword("123")
    user.setEmail("write_grantee@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val strangerUser: User = {
    val user = new User
    user.setName("stranger")
    user.setPassword("123")
    user.setEmail("stranger@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val privateDataset: Dataset = {
    val dataset = new Dataset
    dataset.setName("private-dataset")
    dataset.setRepositoryName("private-dataset")
    dataset.setIsPublic(false)
    dataset.setIsDownloadable(true)
    dataset.setDescription("private dataset for access tests")
    dataset
  }

  private val publicDataset: Dataset = {
    val dataset = new Dataset
    dataset.setName("public-dataset")
    dataset.setRepositoryName("public-dataset")
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    dataset.setDescription("public dataset for access tests")
    dataset
  }

  private val nonExistentDid: Integer = 999999

  lazy val accessResource = new DatasetAccessResource()

  lazy val ownerSession = new SessionUser(ownerUser)
  lazy val writeGranteeSession = new SessionUser(writeGranteeUser)
  lazy val readGranteeSession = new SessionUser(readGranteeUser)
  lazy val strangerSession = new SessionUser(strangerUser)

  private def grantDirectly(did: Integer, uid: Integer, privilege: PrivilegeEnum): Unit = {
    new DatasetUserAccessDao(getDSLContext.configuration())
      .insert(new DatasetUserAccess(did, uid, privilege))
  }

  private def accessList(did: Integer): List[DatasetAccessResource.AccessEntry] =
    accessResource.getAccessList(did).asScala.toList

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(readGranteeUser)
    userDao.insert(writeGranteeUser)
    userDao.insert(strangerUser)

    privateDataset.setOwnerUid(ownerUser.getUid)
    publicDataset.setOwnerUid(ownerUser.getUid)
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    datasetDao.insert(privateDataset)
    datasetDao.insert(publicDataset)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // every test starts with no explicit grants
    getDSLContext.deleteFrom(DATASET_USER_ACCESS).execute()
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  // ===========================================================================
  // Privilege helpers
  // ===========================================================================

  "isDatasetPublic" should "be true for a public dataset and false for a private one" in {
    isDatasetPublic(getDSLContext, publicDataset.getDid) shouldBe true
    isDatasetPublic(getDSLContext, privateDataset.getDid) shouldBe false
  }

  "userOwnDataset" should "be true only for the owner" in {
    userOwnDataset(getDSLContext, privateDataset.getDid, ownerUser.getUid) shouldBe true
    userOwnDataset(getDSLContext, privateDataset.getDid, strangerUser.getUid) shouldBe false
  }

  "getDatasetUserAccessPrivilege" should "return NONE for a user without an explicit grant" in {
    getDatasetUserAccessPrivilege(
      getDSLContext,
      privateDataset.getDid,
      strangerUser.getUid
    ) shouldEqual PrivilegeEnum.NONE
  }

  it should "return the granted privilege for a grantee" in {
    grantDirectly(privateDataset.getDid, readGranteeUser.getUid, PrivilegeEnum.READ)
    grantDirectly(privateDataset.getDid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    getDatasetUserAccessPrivilege(
      getDSLContext,
      privateDataset.getDid,
      readGranteeUser.getUid
    ) shouldEqual PrivilegeEnum.READ
    getDatasetUserAccessPrivilege(
      getDSLContext,
      privateDataset.getDid,
      writeGranteeUser.getUid
    ) shouldEqual PrivilegeEnum.WRITE
  }

  "the owner" should "have both read and write access to the dataset" in {
    userHasReadAccess(getDSLContext, privateDataset.getDid, ownerUser.getUid) shouldBe true
    userHasWriteAccess(getDSLContext, privateDataset.getDid, ownerUser.getUid) shouldBe true
  }

  "a READ grantee" should "have read but not write access" in {
    grantDirectly(privateDataset.getDid, readGranteeUser.getUid, PrivilegeEnum.READ)

    userHasReadAccess(getDSLContext, privateDataset.getDid, readGranteeUser.getUid) shouldBe true
    userHasWriteAccess(getDSLContext, privateDataset.getDid, readGranteeUser.getUid) shouldBe false
  }

  "a WRITE grantee" should "have both read and write access" in {
    grantDirectly(privateDataset.getDid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    userHasReadAccess(getDSLContext, privateDataset.getDid, writeGranteeUser.getUid) shouldBe true
    userHasWriteAccess(getDSLContext, privateDataset.getDid, writeGranteeUser.getUid) shouldBe true
  }

  "a user with no grant" should "have no access to a private dataset" in {
    userHasReadAccess(getDSLContext, privateDataset.getDid, strangerUser.getUid) shouldBe false
    userHasWriteAccess(getDSLContext, privateDataset.getDid, strangerUser.getUid) shouldBe false
  }

  it should "have read but not write access to a public dataset" in {
    userHasReadAccess(getDSLContext, publicDataset.getDid, strangerUser.getUid) shouldBe true
    userHasWriteAccess(getDSLContext, publicDataset.getDid, strangerUser.getUid) shouldBe false
  }

  it should "have no explicit privilege row on a public dataset" in {
    // public read access comes from is_public, not from a dataset_user_access row
    getDatasetUserAccessPrivilege(
      getDSLContext,
      publicDataset.getDid,
      strangerUser.getUid
    ) shouldEqual PrivilegeEnum.NONE
  }

  "an explicit WRITE grant on a public dataset" should "give a non-owner write access" in {
    grantDirectly(publicDataset.getDid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    userHasWriteAccess(getDSLContext, publicDataset.getDid, writeGranteeUser.getUid) shouldBe true
  }

  "the privilege helpers" should "treat a nonexistent dataset as private, unowned, and ungranted" in {
    isDatasetPublic(getDSLContext, nonExistentDid) shouldBe false
    userOwnDataset(getDSLContext, nonExistentDid, ownerUser.getUid) shouldBe false
    getDatasetUserAccessPrivilege(
      getDSLContext,
      nonExistentDid,
      ownerUser.getUid
    ) shouldEqual PrivilegeEnum.NONE
    userHasReadAccess(getDSLContext, nonExistentDid, ownerUser.getUid) shouldBe false
    userHasWriteAccess(getDSLContext, nonExistentDid, ownerUser.getUid) shouldBe false
  }

  "getOwner" should "return the owning user" in {
    getOwner(getDSLContext, privateDataset.getDid).getEmail shouldEqual ownerUser.getEmail
  }

  it should "return null for a nonexistent dataset" in {
    getOwner(getDSLContext, nonExistentDid) shouldBe null
  }

  // ===========================================================================
  // grantAccess / getAccessList
  // ===========================================================================

  "grantAccess" should "add a grantee that appears in the access list with the granted privilege" in {
    val response = accessResource.grantAccess(
      privateDataset.getDid,
      readGranteeUser.getEmail,
      "READ",
      ownerSession
    )
    response.getStatus shouldEqual 200

    val entries = accessList(privateDataset.getDid)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
    entries.head.name shouldEqual readGranteeUser.getName
    entries.head.privilege shouldEqual PrivilegeEnum.READ
  }

  it should "update the privilege in place when re-granting with a different privilege" in {
    accessResource.grantAccess(
      privateDataset.getDid,
      readGranteeUser.getEmail,
      "READ",
      ownerSession
    )
    accessResource.grantAccess(
      privateDataset.getDid,
      readGranteeUser.getEmail,
      "WRITE",
      ownerSession
    )

    val entries = accessList(privateDataset.getDid)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
    entries.head.privilege shouldEqual PrivilegeEnum.WRITE
  }

  it should "allow a WRITE grantee to share the dataset" in {
    grantDirectly(privateDataset.getDid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    val response = accessResource.grantAccess(
      privateDataset.getDid,
      strangerUser.getEmail,
      "READ",
      writeGranteeSession
    )
    response.getStatus shouldEqual 200

    userHasReadAccess(getDSLContext, privateDataset.getDid, strangerUser.getUid) shouldBe true
  }

  it should "be forbidden for a user without write access" in {
    val ex = intercept[ForbiddenException] {
      accessResource.grantAccess(
        privateDataset.getDid,
        readGranteeUser.getEmail,
        "READ",
        strangerSession
      )
    }
    ex.getResponse.getStatus shouldEqual 403
    ex.getMessage should include(
      s"You do not have permission to modify dataset ${privateDataset.getDid}"
    )
  }

  it should "be forbidden for a READ grantee" in {
    grantDirectly(privateDataset.getDid, readGranteeUser.getUid, PrivilegeEnum.READ)

    assertThrows[ForbiddenException] {
      accessResource.grantAccess(
        privateDataset.getDid,
        strangerUser.getEmail,
        "READ",
        readGranteeSession
      )
    }
  }

  "getAccessList" should "return an empty list when no access has been granted" in {
    accessList(privateDataset.getDid) shouldBe empty
  }

  it should "not include the owner's own access row" in {
    // even if the owner somehow has an explicit access row, the list only shows other users
    grantDirectly(privateDataset.getDid, ownerUser.getUid, PrivilegeEnum.WRITE)
    grantDirectly(privateDataset.getDid, readGranteeUser.getUid, PrivilegeEnum.READ)

    val entries = accessList(privateDataset.getDid)
    entries should have size 1
    entries.head.email shouldEqual readGranteeUser.getEmail
  }

  it should "list multiple grantees with their respective privileges" in {
    grantDirectly(privateDataset.getDid, readGranteeUser.getUid, PrivilegeEnum.READ)
    grantDirectly(privateDataset.getDid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)

    val entries = accessList(privateDataset.getDid)
    entries should have size 2
    val privilegeByEmail = entries.map(entry => entry.email -> entry.privilege).toMap
    privilegeByEmail(readGranteeUser.getEmail) shouldEqual PrivilegeEnum.READ
    privilegeByEmail(writeGranteeUser.getEmail) shouldEqual PrivilegeEnum.WRITE
  }

  // ===========================================================================
  // revokeAccess
  // ===========================================================================

  "revokeAccess" should "remove the grantee from the access list and drop their access" in {
    grantDirectly(privateDataset.getDid, readGranteeUser.getUid, PrivilegeEnum.READ)

    val response = accessResource.revokeAccess(
      privateDataset.getDid,
      readGranteeUser.getEmail,
      ownerSession
    )
    response.getStatus shouldEqual 200

    accessList(privateDataset.getDid) shouldBe empty
    getDatasetUserAccessPrivilege(
      getDSLContext,
      privateDataset.getDid,
      readGranteeUser.getUid
    ) shouldEqual PrivilegeEnum.NONE
    userHasReadAccess(getDSLContext, privateDataset.getDid, readGranteeUser.getUid) shouldBe false
  }

  it should "allow a WRITE grantee to revoke another user's access" in {
    grantDirectly(privateDataset.getDid, writeGranteeUser.getUid, PrivilegeEnum.WRITE)
    grantDirectly(privateDataset.getDid, readGranteeUser.getUid, PrivilegeEnum.READ)

    val response = accessResource.revokeAccess(
      privateDataset.getDid,
      readGranteeUser.getEmail,
      writeGranteeSession
    )
    response.getStatus shouldEqual 200

    userHasReadAccess(getDSLContext, privateDataset.getDid, readGranteeUser.getUid) shouldBe false
  }

  it should "succeed as a no-op when the target user has no explicit grant" in {
    val response = accessResource.revokeAccess(
      privateDataset.getDid,
      strangerUser.getEmail,
      ownerSession
    )
    response.getStatus shouldEqual 200
    accessList(privateDataset.getDid) shouldBe empty
  }

  it should "be forbidden for a user without write access" in {
    grantDirectly(privateDataset.getDid, readGranteeUser.getUid, PrivilegeEnum.READ)

    assertThrows[ForbiddenException] {
      accessResource.revokeAccess(
        privateDataset.getDid,
        readGranteeUser.getEmail,
        strangerSession
      )
    }
  }

  // ===========================================================================
  // getOwnerEmailOfDataset
  // ===========================================================================

  "getOwnerEmailOfDataset" should "return the owner's email" in {
    accessResource.getOwnerEmailOfDataset(privateDataset.getDid) shouldEqual ownerUser.getEmail
  }

  it should "return an empty string for a nonexistent dataset" in {
    accessResource.getOwnerEmailOfDataset(nonExistentDid) shouldEqual ""
  }
}
