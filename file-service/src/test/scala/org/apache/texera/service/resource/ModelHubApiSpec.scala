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

import jakarta.ws.rs._
import jakarta.ws.rs.core.Response
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{ModelDao, ModelUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{ModelUserAccess, ModelVersion, User}
import org.apache.texera.service.MockLakeFS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}

import java.io.ByteArrayInputStream
import java.util.Optional

// Covers what a model page needs beyond the owner's own dashboard: the cover image
// a card renders, and the anonymous halves of version browsing. Without the latter a
// logged-out visitor on a public model page gets metadata and then an error.
class ModelHubApiSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with ResourceTestHelpers {

  private def mkUser(name: String): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val ownerUser: User = mkUser("model_hub_owner")
  private val readerUser: User = mkUser("model_hub_reader")
  private val strangerUser: User = mkUser("model_hub_stranger")

  lazy val modelResource = new ModelResource()
  lazy val modelDao = new ModelDao(getDSLContext.configuration())
  lazy val ownerSession = new SessionUser(ownerUser)
  lazy val readerSession = new SessionUser(readerUser)
  lazy val strangerSession = new SessionUser(strangerUser)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    Seq(ownerUser, readerUser, strangerUser).foreach(userDao.insert)
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  // ---------- helpers ----------
  private def newModel(isPublic: Boolean): ModelResource.DashboardModel =
    modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = uniqueName("hub-model"),
        modelDescription = "for model hub api tests",
        isModelPublic = isPublic,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      ownerSession
    )

  private def upload(mid: Integer, path: String, bytes: Array[Byte]): Unit =
    modelResource
      .uploadOneFileToModel(
        mid,
        urlEnc(path),
        "upload",
        new ByteArrayInputStream(bytes),
        mkHeaders(bytes.length.toLong),
        ownerSession
      )
      .getStatus shouldEqual 200

  /** Commits `cover.jpg` plus a weights file, and returns the generated version. */
  private def commitVersionWithCover(mid: Integer): ModelVersion = {
    upload(mid, "cover.jpg", Array.fill[Byte](512)(0xff.toByte))
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))
    modelResource.createModelVersion("init", mid, ownerSession).modelVersion
  }

  private def grantRead(mid: Integer, uid: Integer): Unit =
    new ModelUserAccessDao(getDSLContext.configuration())
      .insert(new ModelUserAccess(mid, uid, PrivilegeEnum.READ))

  private def urlOf(response: Response): Option[String] =
    Option(
      response.getEntity.asInstanceOf[Map[String, String]]("url")
    )

  private def setPublic(mid: Integer, isPublic: Boolean): Unit = {
    val model = modelDao.fetchOneByMid(mid)
    model.setIsPublic(isPublic)
    modelDao.update(model)
  }

  // ===========================================================================
  // cover image: setting it
  // ===========================================================================
  "updateModelCoverImage" should "store the normalized path and echo it back" in {
    val mid = newModel(isPublic = false).model.getMid
    val version = commitVersionWithCover(mid)
    val coverPath = s"${version.getName}/cover.jpg"

    val response =
      modelResource.updateModelCoverImage(
        mid,
        ModelResource.CoverImageRequest(coverPath),
        ownerSession
      )

    response.getStatus shouldEqual 200
    response.getEntity.asInstanceOf[Map[String, String]]("coverImage") shouldEqual coverPath
    modelDao.fetchOneByMid(mid).getCoverImage shouldEqual coverPath
  }

  it should "reject a non-image extension" in {
    val mid = newModel(isPublic = false).model.getMid
    val version = commitVersionWithCover(mid)

    assertThrows[BadRequestException] {
      modelResource.updateModelCoverImage(
        mid,
        ModelResource.CoverImageRequest(s"${version.getName}/model.pt"),
        ownerSession
      )
    }
  }

  it should "reject an empty path" in {
    val mid = newModel(isPublic = false).model.getMid

    assertThrows[BadRequestException] {
      modelResource.updateModelCoverImage(mid, ModelResource.CoverImageRequest(""), ownerSession)
    }
  }

  it should "reject a caller with only read access" in {
    val mid = newModel(isPublic = false).model.getMid
    val version = commitVersionWithCover(mid)
    grantRead(mid, readerUser.getUid)

    assertThrows[ForbiddenException] {
      modelResource.updateModelCoverImage(
        mid,
        ModelResource.CoverImageRequest(s"${version.getName}/cover.jpg"),
        readerSession
      )
    }
  }

  // ===========================================================================
  // cover image: reading it
  // ===========================================================================
  "getModelCover" should "redirect a logged-out visitor to the presigned URL of a public model" in {
    val mid = newModel(isPublic = true).model.getMid
    val version = commitVersionWithCover(mid)
    modelResource.updateModelCoverImage(
      mid,
      ModelResource.CoverImageRequest(s"${version.getName}/cover.jpg"),
      ownerSession
    )

    val response = modelResource.getModelCover(mid, Optional.empty())

    response.getStatus shouldEqual 307
    response.getHeaderString("Location") should not be null
  }

  it should "return 404 when no cover image is set" in {
    val mid = newModel(isPublic = true).model.getMid

    assertThrows[NotFoundException] {
      modelResource.getModelCover(mid, Optional.of(ownerSession))
    }
  }

  it should "forbid a logged-out visitor on a private model" in {
    val mid = newModel(isPublic = false).model.getMid

    assertThrows[ForbiddenException] {
      modelResource.getModelCover(mid, Optional.empty())
    }
  }

  it should "forbid a signed-in user with no grant on a private model" in {
    val mid = newModel(isPublic = false).model.getMid

    assertThrows[ForbiddenException] {
      modelResource.getModelCover(mid, Optional.of(strangerSession))
    }
  }

  "getModelCoverUrl" should "return the presigned URL to the owner of a private model" in {
    val mid = newModel(isPublic = false).model.getMid
    val version = commitVersionWithCover(mid)
    modelResource.updateModelCoverImage(
      mid,
      ModelResource.CoverImageRequest(s"${version.getName}/cover.jpg"),
      ownerSession
    )

    val response = modelResource.getModelCoverUrl(mid, Optional.of(ownerSession))

    response.getStatus shouldEqual 200
    urlOf(response) shouldBe defined
  }

  it should "return the presigned URL to a read-grantee of a private model" in {
    val mid = newModel(isPublic = false).model.getMid
    val version = commitVersionWithCover(mid)
    modelResource.updateModelCoverImage(
      mid,
      ModelResource.CoverImageRequest(s"${version.getName}/cover.jpg"),
      ownerSession
    )
    grantRead(mid, readerUser.getUid)

    urlOf(modelResource.getModelCoverUrl(mid, Optional.of(readerSession))) shouldBe defined
  }

  it should "return a null url rather than 404 when no cover image is set" in {
    val mid = newModel(isPublic = true).model.getMid

    val response = modelResource.getModelCoverUrl(mid, Optional.of(ownerSession))

    response.getStatus shouldEqual 200
    urlOf(response) shouldBe empty
  }

  it should "forbid a signed-in user with no grant on a private model" in {
    val mid = newModel(isPublic = false).model.getMid

    assertThrows[ForbiddenException] {
      modelResource.getModelCoverUrl(mid, Optional.of(strangerSession))
    }
  }

  // ===========================================================================
  // anonymous version browsing
  // ===========================================================================
  "getPublicModelVersionList" should "list versions of a public model without authentication" in {
    val mid = newModel(isPublic = true).model.getMid
    val version = commitVersionWithCover(mid)

    modelResource.getPublicModelVersionList(mid).map(_.getMvid) shouldEqual List(version.getMvid)
  }

  it should "return an empty list for a public model with no versions" in {
    modelResource.getPublicModelVersionList(newModel(isPublic = true).model.getMid) shouldBe empty
  }

  it should "forbid listing versions of a private model" in {
    val mid = newModel(isPublic = false).model.getMid

    assertThrows[ForbiddenException] {
      modelResource.getPublicModelVersionList(mid)
    }
  }

  it should "reject an unknown model id" in {
    assertThrows[NotFoundException] {
      modelResource.getPublicModelVersionList(999999)
    }
  }

  it should "stop listing once the model is unpublished" in {
    val mid = newModel(isPublic = true).model.getMid
    commitVersionWithCover(mid)
    modelResource.getPublicModelVersionList(mid) should have size 1

    setPublic(mid, isPublic = false)

    assertThrows[ForbiddenException] {
      modelResource.getPublicModelVersionList(mid)
    }
  }

  "retrievePublicModelVersionRootFileNodes" should "return the file tree of a public model version" in {
    val mid = newModel(isPublic = true).model.getMid
    val version = commitVersionWithCover(mid)

    val response = modelResource.retrievePublicModelVersionRootFileNodes(mid, version.getMvid)

    response.fileNodes.map(_.getName) should contain allOf ("cover.jpg", "model.pt")
    response.size shouldEqual 512 + 64
  }

  it should "forbid the file tree of a private model version" in {
    val mid = newModel(isPublic = false).model.getMid
    val version = commitVersionWithCover(mid)

    assertThrows[ForbiddenException] {
      modelResource.retrievePublicModelVersionRootFileNodes(mid, version.getMvid)
    }
  }

  it should "reject an unknown version id on a public model" in {
    val mid = newModel(isPublic = true).model.getMid

    assertThrows[NotFoundException] {
      modelResource.retrievePublicModelVersionRootFileNodes(mid, 999999)
    }
  }

  it should "agree with the authenticated endpoint on the same version" in {
    val mid = newModel(isPublic = true).model.getMid
    val version = commitVersionWithCover(mid)

    val anonymous = modelResource.retrievePublicModelVersionRootFileNodes(mid, version.getMvid)
    val authenticated =
      modelResource.retrieveModelVersionRootFileNodes(mid, version.getMvid, ownerSession)

    anonymous.fileNodes.map(_.getName).sorted shouldEqual
      authenticated.fileNodes.map(_.getName).sorted
    anonymous.size shouldEqual authenticated.size
  }
}
