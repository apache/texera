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
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.service.MockLakeFS
import org.apache.texera.service.`type`.{ExistingUploadFile, ExistingUploadFilesRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters._

// Covers the model endpoints the UI needs beyond upload and download: the staged
// (uncommitted) change list and its per-file reset, the "already uploaded" probe
// that lets a client skip re-sending bytes, the owner facet for the list page,
// and the framework/format metadata labels.
class ModelApiForUiSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ResourceTestHelpers {

  private def mkUser(name: String): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val ownerUser: User = mkUser("model_ui_owner")
  private val strangerUser: User = mkUser("model_ui_stranger")

  lazy val modelResource = new ModelResource()
  lazy val sessionUser = new SessionUser(ownerUser)
  lazy val strangerSession = new SessionUser(strangerUser)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(strangerUser)
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  // ---------- helpers ----------
  private def newModel(
      framework: String = "pytorch",
      format: String = null,
      isPublic: Boolean = false
  ): ModelResource.DashboardModel =
    modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = uniqueName("ui-model"),
        modelDescription = "for model-api-for-ui tests",
        isModelPublic = isPublic,
        isModelDownloadable = true,
        framework = framework,
        format = format
      ),
      sessionUser
    )

  private def upload(mid: Integer, path: String, bytes: Array[Byte]): Unit =
    modelResource
      .uploadOneFileToModel(
        mid,
        urlEnc(path),
        "upload",
        new ByteArrayInputStream(bytes),
        mkHeaders(bytes.length.toLong),
        sessionUser
      )
      .getStatus shouldEqual 200

  private def matchedPaths(response: core.Response): List[String] =
    response.getEntity
      .asInstanceOf[Map[String, java.util.List[String]]]("filePaths")
      .asScala
      .toList

  // ===========================================================================
  // staged changes
  // ===========================================================================
  "getModelDiff" should "list an uploaded-but-uncommitted file as added" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))

    val diffs = modelResource.getModelDiff(mid, sessionUser)

    diffs.map(_.path) should contain("model.pt")
    val diff = diffs.find(_.path == "model.pt").get
    diff.diffType shouldEqual "added"
    diff.sizeBytes shouldEqual Some(64L)
  }

  it should "report nothing once the changes are committed" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))
    modelResource.createModelVersion("v1", mid, sessionUser)

    modelResource.getModelDiff(mid, sessionUser) shouldBe empty
  }

  it should "report a deletion of a committed file as a staged change" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))
    modelResource.createModelVersion("v1", mid, sessionUser)
    modelResource.deleteModelFile(mid, urlEnc("model.pt"), sessionUser).getStatus shouldEqual 200

    modelResource.getModelDiff(mid, sessionUser).map(_.diffType) should contain("removed")
  }

  it should "refuse a caller with no access to the model" in {
    val mid = newModel().model.getMid

    assertThrows[ForbiddenException] {
      modelResource.getModelDiff(mid, strangerSession)
    }
  }

  "resetModelFileDiff" should "drop a staged upload" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))

    modelResource
      .resetModelFileDiff(mid, urlEnc("model.pt"), sessionUser)
      .getStatus shouldEqual 200

    modelResource.getModelDiff(mid, sessionUser) shouldBe empty
  }

  it should "restore a committed file whose deletion was staged" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))
    modelResource.createModelVersion("v1", mid, sessionUser)
    modelResource.deleteModelFile(mid, urlEnc("model.pt"), sessionUser)

    modelResource.resetModelFileDiff(mid, urlEnc("model.pt"), sessionUser)

    modelResource.getModelDiff(mid, sessionUser) shouldBe empty
  }

  it should "refuse a caller with no write access to the model" in {
    val mid = newModel(isPublic = true).model.getMid

    assertThrows[ForbiddenException] {
      modelResource.resetModelFileDiff(mid, urlEnc("model.pt"), strangerSession)
    }
  }

  // ===========================================================================
  // existing-upload-files
  // ===========================================================================
  "findExistingUploadFiles" should "match a committed file of the same size" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))
    modelResource.createModelVersion("v1", mid, sessionUser)

    val response = modelResource.findExistingUploadFiles(
      mid,
      ExistingUploadFilesRequest(List(ExistingUploadFile("model.pt", 64L))),
      sessionUser
    )

    matchedPaths(response) shouldEqual List("model.pt")
  }

  it should "match a staged file that has not been committed yet" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))

    val response = modelResource.findExistingUploadFiles(
      mid,
      ExistingUploadFilesRequest(List(ExistingUploadFile("model.pt", 64L))),
      sessionUser
    )

    matchedPaths(response) shouldEqual List("model.pt")
  }

  it should "not match a file whose size differs" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))
    modelResource.createModelVersion("v1", mid, sessionUser)

    val response = modelResource.findExistingUploadFiles(
      mid,
      ExistingUploadFilesRequest(List(ExistingUploadFile("model.pt", 65L))),
      sessionUser
    )

    matchedPaths(response) shouldBe empty
  }

  // A staged deletion does not withdraw the committed file from the match set. This is
  // the dataset endpoint's behaviour, preserved by the extraction rather than chosen here.
  it should "still match a committed file whose deletion is staged" in {
    val mid = newModel().model.getMid
    upload(mid, "model.pt", Array.fill[Byte](64)(0x1))
    modelResource.createModelVersion("v1", mid, sessionUser)
    modelResource.deleteModelFile(mid, urlEnc("model.pt"), sessionUser)

    val response = modelResource.findExistingUploadFiles(
      mid,
      ExistingUploadFilesRequest(List(ExistingUploadFile("model.pt", 64L))),
      sessionUser
    )

    matchedPaths(response) shouldEqual List("model.pt")
  }

  it should "report matches for a model with no version yet as empty" in {
    val mid = newModel().model.getMid

    val response = modelResource.findExistingUploadFiles(
      mid,
      ExistingUploadFilesRequest(List(ExistingUploadFile("model.pt", 64L))),
      sessionUser
    )

    matchedPaths(response) shouldBe empty
  }

  it should "reject a negative size" in {
    val mid = newModel().model.getMid

    assertThrows[BadRequestException] {
      modelResource.findExistingUploadFiles(
        mid,
        ExistingUploadFilesRequest(List(ExistingUploadFile("model.pt", -1L))),
        sessionUser
      )
    }
  }

  it should "reject a path that escapes the repository root" in {
    val mid = newModel().model.getMid

    assertThrows[BadRequestException] {
      modelResource.findExistingUploadFiles(
        mid,
        ExistingUploadFilesRequest(List(ExistingUploadFile("../secret.pt", 64L))),
        sessionUser
      )
    }
  }

  it should "refuse a caller with no write access to the model" in {
    val mid = newModel(isPublic = true).model.getMid

    assertThrows[ForbiddenException] {
      modelResource.findExistingUploadFiles(
        mid,
        ExistingUploadFilesRequest(List(ExistingUploadFile("model.pt", 64L))),
        strangerSession
      )
    }
  }

  // ===========================================================================
  // owner facet
  // ===========================================================================
  "retrieveOwners" should "include the caller once they own a model" in {
    newModel()

    modelResource.retrieveOwners(sessionUser).asScala should contain(ownerUser.getEmail)
  }

  it should "not leak the owner of a private model to a stranger" in {
    newModel(isPublic = false)

    modelResource.retrieveOwners(strangerSession).asScala should not contain ownerUser.getEmail
  }

  // The facet is scoped by explicit grant, not by readability: a public model the caller
  // was never granted does not put its owner in their filter list. Matches datasets.
  it should "not surface the owner of a public model the caller holds no grant on" in {
    newModel(isPublic = true)

    modelResource.retrieveOwners(strangerSession).asScala should not contain ownerUser.getEmail
  }

  // ===========================================================================
  // framework / format labels
  // ===========================================================================
  "createModel" should "accept every supported framework" in {
    ModelResource.SUPPORTED_FRAMEWORKS.foreach { framework =>
      newModel(framework = framework).model.getFramework shouldEqual framework
    }
  }

  it should "accept every supported format" in {
    ModelResource.SUPPORTED_FORMATS.foreach { format =>
      newModel(format = format).model.getFormat shouldEqual format
    }
  }

  it should "reject an unsupported framework" in {
    val ex = intercept[BadRequestException](newModel(framework = "caffe"))
    ex.getMessage should include("Unsupported framework 'caffe'")
  }

  it should "reject an unsupported format" in {
    assertThrows[BadRequestException](newModel(format = "gguf"))
  }

  it should "fall back to the default framework when none is given" in {
    newModel(framework = null).model.getFramework shouldEqual ModelResource.DEFAULT_FRAMEWORK
  }

  it should "treat a blank framework as absent rather than invalid" in {
    newModel(framework = "   ").model.getFramework shouldEqual ModelResource.DEFAULT_FRAMEWORK
  }

  it should "leave the format unset when none is given" in {
    newModel(format = null).model.getFormat shouldBe null
  }

  // ===========================================================================
  // listing sizes
  // ===========================================================================
  "listModels" should "report the repository size of an owned model" in {
    val model = newModel()
    upload(model.model.getMid, "sized.pt", Array.fill[Byte](4096)(0x6))
    modelResource.createModelVersion("v1", model.model.getMid, sessionUser)

    val listed = modelResource
      .listModels(sessionUser)
      .find(_.model.getMid == model.model.getMid)
      .getOrElse(fail("the owned model should be listed"))

    listed.isOwner shouldBe true
    listed.size should be >= 4096L
  }

  it should "still list a model whose repository size cannot be read" in {
    // An unreadable size degrades to 0 rather than dropping the row.
    val model = newModel()
    LakeFSStorageClient.deleteRepo(model.model.getRepositoryName)

    val listed = modelResource.listModels(sessionUser).find(_.model.getMid == model.model.getMid)

    listed should not be empty
    listed.get.size shouldEqual 0L
  }
}
