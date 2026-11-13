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

package org.apache.texera.service.resource

import jakarta.ws.rs.ForbiddenException
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, DatasetUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetUserAccess, User}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

/** The owner always has WRITE, whatever their own access row says, and nobody can change that row. */
class ResourceAccessSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val resource = ResourceTables.Dataset

  private def newUser(name: String): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val owner = newUser("owner")
  private val grantee = newUser("grantee")
  private val stranger = newUser("stranger")

  private def newDataset(name: String, isPublic: Boolean): Dataset = {
    val dataset = new Dataset
    dataset.setName(name)
    dataset.setRepositoryName(name)
    dataset.setIsPublic(isPublic)
    dataset.setIsDownloadable(true)
    dataset.setDescription("")
    dataset
  }

  private val privateDataset = newDataset("private-dataset", isPublic = false)
  private val publicDataset = newDataset("public-dataset", isPublic = true)

  private def did: Integer = privateDataset.getDid

  private def grantRow(did: Integer, uid: Integer, privilege: PrivilegeEnum): Unit =
    new DatasetUserAccessDao(getDSLContext.configuration())
      .insert(new DatasetUserAccess(did, uid, privilege))

  private def rowOf(uid: Integer): Option[PrivilegeEnum] =
    Option(
      getDSLContext
        .select(DATASET_USER_ACCESS.PRIVILEGE)
        .from(DATASET_USER_ACCESS)
        .where(DATASET_USER_ACCESS.DID.eq(did).and(DATASET_USER_ACCESS.UID.eq(uid)))
        .fetchOneInto(classOf[PrivilegeEnum])
    )

  /** (dataset name, privilege, isOwner) for every dataset `uid` may see. */
  private def visibleTo(
      uid: Integer,
      includePublic: Boolean = true
  ): List[(String, PrivilegeEnum, Boolean)] =
    ResourceAccess.listVisible(
      getDSLContext,
      resource,
      uid,
      classOf[Dataset],
      (d: Dataset) => d.getDid,
      includePublic
    )(
      (d, _, privilege, isOwner) => Some((d.getName, privilege, isOwner)),
      (d, _) => Some((d.getName, PrivilegeEnum.READ, false))
    )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    Seq(owner, grantee, stranger).foreach(userDao.insert(_))
    privateDataset.setOwnerUid(owner.getUid)
    publicDataset.setOwnerUid(owner.getUid)
    new DatasetDao(getDSLContext.configuration()).insert(privateDataset, publicDataset)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // Every test starts with no access rows, so the owner has none either.
    getDSLContext.deleteFrom(DATASET_USER_ACCESS).execute()
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  "privilegeOf" should "give the owner WRITE without an access row" in {
    ResourceAccess.privilegeOf(
      getDSLContext,
      resource,
      did,
      owner.getUid
    ) shouldBe PrivilegeEnum.WRITE
  }

  it should "give the owner WRITE even when their access row says READ" in {
    grantRow(did, owner.getUid, PrivilegeEnum.READ)

    ResourceAccess.privilegeOf(
      getDSLContext,
      resource,
      did,
      owner.getUid
    ) shouldBe PrivilegeEnum.WRITE
  }

  it should "read a grantee's privilege from their access row, and NONE without one" in {
    grantRow(did, grantee.getUid, PrivilegeEnum.READ)

    ResourceAccess.privilegeOf(
      getDSLContext,
      resource,
      did,
      grantee.getUid
    ) shouldBe PrivilegeEnum.READ
    ResourceAccess.privilegeOf(
      getDSLContext,
      resource,
      did,
      stranger.getUid
    ) shouldBe PrivilegeEnum.NONE
  }

  "userHasWriteAccess" should "hold for the owner and a WRITE grantee, not a READ grantee" in {
    grantRow(did, grantee.getUid, PrivilegeEnum.READ)

    ResourceAccess.userHasWriteAccess(getDSLContext, resource, did, owner.getUid) shouldBe true
    ResourceAccess.userHasWriteAccess(getDSLContext, resource, did, grantee.getUid) shouldBe false
  }

  "userHasReadAccess" should "hold for the owner, a grantee and anyone on a public dataset, not a stranger" in {
    grantRow(did, grantee.getUid, PrivilegeEnum.READ)

    ResourceAccess.userHasReadAccess(getDSLContext, resource, did, owner.getUid) shouldBe true
    ResourceAccess.userHasReadAccess(getDSLContext, resource, did, grantee.getUid) shouldBe true
    ResourceAccess.userHasReadAccess(getDSLContext, resource, did, stranger.getUid) shouldBe false
    ResourceAccess.userHasReadAccess(
      getDSLContext,
      resource,
      publicDataset.getDid,
      stranger.getUid
    ) shouldBe true
  }

  "listVisible" should "list the owner's private dataset as WRITE without an access row" in {
    visibleTo(owner.getUid, includePublic = false) should contain(
      ("private-dataset", PrivilegeEnum.WRITE, true)
    )
  }

  it should "list the owner's dataset as WRITE even when their access row says READ" in {
    grantRow(did, owner.getUid, PrivilegeEnum.READ)

    visibleTo(owner.getUid, includePublic = false) should contain(
      ("private-dataset", PrivilegeEnum.WRITE, true)
    )
  }

  it should "list a public dataset its owner holds no row on once, as the owner's" in {
    visibleTo(owner.getUid).filter(_._1 == "public-dataset") shouldBe List(
      ("public-dataset", PrivilegeEnum.WRITE, true)
    )
  }

  it should "list a grantee's dataset with their granted privilege" in {
    grantRow(did, grantee.getUid, PrivilegeEnum.READ)

    visibleTo(grantee.getUid, includePublic = false) shouldBe List(
      ("private-dataset", PrivilegeEnum.READ, false)
    )
  }

  it should "show a stranger only public datasets" in {
    visibleTo(stranger.getUid).map(_._1) shouldBe List("public-dataset")
    visibleTo(stranger.getUid, includePublic = false) shouldBe empty
  }

  "ownerEmailsVisibleTo" should "include the owner's own email without an access row" in {
    ResourceAccess
      .ownerEmailsVisibleTo(getDSLContext, resource, owner.getUid)
      .asScala shouldBe List(owner.getEmail)
  }

  it should "include the owner of a dataset shared with the caller, and nothing for a stranger" in {
    grantRow(did, grantee.getUid, PrivilegeEnum.READ)

    ResourceAccess
      .ownerEmailsVisibleTo(getDSLContext, resource, grantee.getUid)
      .asScala shouldBe List(owner.getEmail)
    ResourceAccess
      .ownerEmailsVisibleTo(getDSLContext, resource, stranger.getUid)
      .asScala shouldBe empty
  }

  "grant" should "refuse to change the owner's privilege, leaving their row as it was" in {
    grantRow(did, owner.getUid, PrivilegeEnum.WRITE)
    grantRow(did, grantee.getUid, PrivilegeEnum.WRITE)

    val ex = intercept[ForbiddenException] {
      ResourceAccess.grant(getDSLContext, resource, did, owner.getEmail, "READ", grantee.getUid)
    }
    ex.getResponse.getStatus shouldBe 403
    rowOf(owner.getUid) shouldBe Some(PrivilegeEnum.WRITE)
  }

  it should "still grant to anyone else" in {
    ResourceAccess
      .grant(getDSLContext, resource, did, grantee.getEmail, "READ", owner.getUid)
      .getStatus shouldBe 200
    rowOf(grantee.getUid) shouldBe Some(PrivilegeEnum.READ)
  }

  "revoke" should "refuse to remove the owner's access row" in {
    grantRow(did, owner.getUid, PrivilegeEnum.WRITE)
    grantRow(did, grantee.getUid, PrivilegeEnum.WRITE)

    val ex = intercept[ForbiddenException] {
      ResourceAccess.revoke(getDSLContext, resource, did, owner.getEmail, grantee.getUid)
    }
    ex.getResponse.getStatus shouldBe 403
    rowOf(owner.getUid) shouldBe Some(PrivilegeEnum.WRITE)
  }

  it should "still revoke anyone else" in {
    grantRow(did, grantee.getUid, PrivilegeEnum.READ)

    ResourceAccess
      .revoke(getDSLContext, resource, did, grantee.getEmail, owner.getUid)
      .getStatus shouldBe 200
    rowOf(grantee.getUid) shouldBe None
  }
}
