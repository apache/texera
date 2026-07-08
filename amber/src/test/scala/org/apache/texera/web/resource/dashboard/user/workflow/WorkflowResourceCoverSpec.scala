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

package org.apache.texera.web.resource.dashboard.user.workflow

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowOfUser,
  WorkflowUserAccess
}
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowResource.CoverImageRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp
import javax.ws.rs.{BadRequestException, ForbiddenException, NotFoundException}

class WorkflowResourceCoverSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val ownerUid = 1000 + scala.util.Random.nextInt(1000)
  private val readerUid = 2000 + scala.util.Random.nextInt(1000)
  private val strangerUid = 3000 + scala.util.Random.nextInt(1000)
  private val testWid = 5000 + scala.util.Random.nextInt(1000)

  private val sampleImage = "data:image/jpeg;base64,/9j/4AAQSkZJRg=="

  private var owner: User = _
  private var reader: User = _
  private var stranger: User = _

  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _
  private var resource: WorkflowResource = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())
    resource = new WorkflowResource()

    owner = makeUser(ownerUid, "cover_owner")
    reader = makeUser(readerUid, "cover_reader")
    stranger = makeUser(strangerUid, "cover_stranger")

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName("cover_test_workflow")
    workflow.setContent("{}")
    workflow.setDescription("desc")
    workflow.setIsPublic(false)
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))

    cleanupTestData()

    userDao.insert(owner)
    userDao.insert(reader)
    userDao.insert(stranger)
    workflowDao.insert(workflow)

    val ownership = new WorkflowOfUser
    ownership.setUid(ownerUid)
    ownership.setWid(testWid)
    workflowOfUserDao.insert(ownership)

    grantAccess(ownerUid, PrivilegeEnum.WRITE)
    grantAccess(readerUid, PrivilegeEnum.READ)
  }

  override protected def afterEach(): Unit = cleanupTestData()

  private def makeUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user.setPassword("password")
    user
  }

  private def grantAccess(uid: Int, privilege: PrivilegeEnum): Unit = {
    val access = new WorkflowUserAccess
    access.setUid(uid)
    access.setWid(testWid)
    access.setPrivilege(privilege)
    workflowUserAccessDao.insert(access)
  }

  private def session(user: User): SessionUser = new SessionUser(user)

  private def cleanupTestData(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_COVER_IMAGE)
      .where(WORKFLOW_COVER_IMAGE.WID.eq(testWid))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWid))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_OF_USER)
      .where(WORKFLOW_OF_USER.WID.eq(testWid))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW)
      .where(WORKFLOW.WID.eq(testWid))
      .execute()
    getDSLContext
      .deleteFrom(USER)
      .where(USER.UID.in(ownerUid, readerUid, strangerUid))
      .execute()
  }

  "getCoverImage" should "throw NotFoundException when no cover is set" in {
    assertThrows[NotFoundException] {
      resource.getCoverImage(testWid, session(owner))
    }
  }

  it should "return the stored cover after it is set" in {
    resource.setCoverImage(testWid, CoverImageRequest(sampleImage), session(owner))
    resource.getCoverImage(testWid, session(owner)).image shouldBe sampleImage
  }

  it should "be readable by a user with read access" in {
    resource.setCoverImage(testWid, CoverImageRequest(sampleImage), session(owner))
    resource.getCoverImage(testWid, session(reader)).image shouldBe sampleImage
  }

  it should "throw ForbiddenException for a user without read access" in {
    resource.setCoverImage(testWid, CoverImageRequest(sampleImage), session(owner))
    assertThrows[ForbiddenException] {
      resource.getCoverImage(testWid, session(stranger))
    }
  }

  "setCoverImage" should "replace an existing cover (upsert)" in {
    resource.setCoverImage(testWid, CoverImageRequest(sampleImage), session(owner))
    val replacement = "data:image/png;base64,iVBORw0KGgo="
    resource.setCoverImage(testWid, CoverImageRequest(replacement), session(owner))
    resource.getCoverImage(testWid, session(owner)).image shouldBe replacement
  }

  it should "throw BadRequestException for an empty or blank image" in {
    assertThrows[BadRequestException] {
      resource.setCoverImage(testWid, CoverImageRequest(""), session(owner))
    }
    assertThrows[BadRequestException] {
      resource.setCoverImage(testWid, CoverImageRequest("   "), session(owner))
    }
  }

  it should "throw BadRequestException for a null image" in {
    assertThrows[BadRequestException] {
      resource.setCoverImage(testWid, CoverImageRequest(null), session(owner))
    }
  }

  it should "throw BadRequestException when the value is not an image data URL" in {
    assertThrows[BadRequestException] {
      resource.setCoverImage(
        testWid,
        CoverImageRequest("https://example.com/a.png"),
        session(owner)
      )
    }
  }

  it should "throw BadRequestException when the data URL is too large" in {
    val tooLarge = "data:image/jpeg;base64," + ("a" * (4 * 1024 * 1024 + 1))
    assertThrows[BadRequestException] {
      resource.setCoverImage(testWid, CoverImageRequest(tooLarge), session(owner))
    }
  }

  it should "throw ForbiddenException for a user with only read access" in {
    assertThrows[ForbiddenException] {
      resource.setCoverImage(testWid, CoverImageRequest(sampleImage), session(reader))
    }
  }

  it should "not persist a cover when validation fails" in {
    assertThrows[BadRequestException] {
      resource.setCoverImage(testWid, CoverImageRequest("not-a-data-url"), session(owner))
    }
    assertThrows[NotFoundException] {
      resource.getCoverImage(testWid, session(owner))
    }
  }

  "deleteCoverImage" should "remove an existing cover" in {
    resource.setCoverImage(testWid, CoverImageRequest(sampleImage), session(owner))
    resource.deleteCoverImage(testWid, session(owner))
    assertThrows[NotFoundException] {
      resource.getCoverImage(testWid, session(owner))
    }
  }

  it should "be idempotent when no cover is set" in {
    noException should be thrownBy resource.deleteCoverImage(testWid, session(owner))
  }

  it should "throw ForbiddenException for a user with only read access" in {
    resource.setCoverImage(testWid, CoverImageRequest(sampleImage), session(owner))
    assertThrows[ForbiddenException] {
      resource.deleteCoverImage(testWid, session(reader))
    }
    // The cover must still be present after the rejected delete.
    resource.getCoverImage(testWid, session(owner)).image shouldBe sampleImage
  }
}
