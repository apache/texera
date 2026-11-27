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
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.ModelDao
import org.apache.texera.dao.jooq.generated.tables.pojos.{Model, User}

class ModelResourceSpec extends ModelResourceTestSupport("model_user") {

  private val otherUser: User = mkUser("model_user2")
  override protected def extraUsers: Seq[User] = Seq(otherUser)

  private val baseModel: Model = {
    val model = new Model
    model.setName("test-model")
    model.setRepositoryName("test-model")
    model.setIsPublic(true)
    model.setIsDownloadable(true)
    model.setDescription("model for test")
    model.setFramework("pytorch")
    model
  }

  private lazy val modelDao = new ModelDao(getDSLContext.configuration())

  lazy val sessionUser2 = new SessionUser(otherUser)

  private def assertStatus(ex: WebApplicationException, status: Int): Unit =
    ex.getResponse.getStatus shouldEqual status

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    baseModel.setOwnerUid(ownerUser.getUid)
    modelDao.insert(baseModel)
  }

  // ===========================================================================
  // createModel
  // ===========================================================================
  "createModel" should "create a model successfully if the user has no model with the same name" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "new-model",
      modelDescription = "description for new model",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "pytorch",
      format = "torchscript"
    )

    val created = modelResource.createModel(request, sessionUser)
    created.model.getName shouldEqual "new-model"
    created.model.getDescription shouldEqual "description for new model"
    created.model.getIsPublic shouldBe false
    created.model.getIsDownloadable shouldBe true
    created.model.getFramework shouldEqual "pytorch"
    created.model.getFormat shouldEqual "torchscript"
    // the LakeFS repository is named after the created model's id
    created.model.getRepositoryName shouldEqual s"model-${created.model.getMid}"
  }

  it should "default the framework to pytorch when none is provided" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "framework-default-model",
      modelDescription = "no framework provided",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "",
      format = null
    )

    val created = modelResource.createModel(request, sessionUser)
    created.model.getFramework shouldEqual "pytorch"
  }

  it should "refuse to create a model if the user already has one with the same name" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "test-model",
      modelDescription = "duplicate name",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "pytorch",
      format = null
    )

    assertThrows[BadRequestException] {
      modelResource.createModel(request, sessionUser)
    }
  }

  it should "create a model successfully if another user has one with the same name" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "test-model",
      modelDescription = "same name, different owner",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "pytorch",
      format = null
    )

    val created = modelResource.createModel(request, sessionUser2)
    created.model.getName shouldEqual "test-model"
  }

  it should "reject an invalid model name" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "bad name!",
      modelDescription = "invalid name",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "pytorch",
      format = null
    )

    assertThrows[BadRequestException] {
      modelResource.createModel(request, sessionUser)
    }
  }

  it should "return a DashboardModel with owner email, WRITE privilege, isOwner=true and size 0" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "dashboard-model",
      modelDescription = "dashboard properties",
      isModelPublic = true,
      isModelDownloadable = false,
      framework = "pytorch",
      format = null
    )

    val dashboard = modelResource.createModel(request, sessionUser)
    dashboard.ownerEmail shouldEqual ownerUser.getEmail
    dashboard.accessPrivilege shouldEqual PrivilegeEnum.WRITE
    dashboard.isOwner shouldBe true
    dashboard.size shouldEqual 0
  }

  // ===========================================================================
  // getModel / listModels
  // ===========================================================================
  "getModel" should "return the dashboard model including its LakeFS repository size" in {
    val created = createModelFixture("get-model", "for get")

    val dashboard = modelResource.getModel(created.model.getMid, sessionUser)
    dashboard.model.getMid shouldEqual created.model.getMid
    dashboard.size should be >= 0L
  }

  it should "forbid a stranger from getting a private model" in {
    val created = createModelFixture("private-get-model", "private", isPublic = false)

    assertThrows[ForbiddenException] {
      modelResource.getModel(created.model.getMid, sessionUser2)
    }
  }

  "listModels" should "include models the user owns" in {
    val created = createModelFixture("listed-model", "for list")

    val listed = modelResource.listModels(sessionUser)
    listed.map(_.model.getMid) should contain(created.model.getMid)
  }

  // ===========================================================================
  // deleteModel
  // ===========================================================================
  "deleteModel" should "delete a model successfully if the user owns it" in {
    val created = createModelFixture("delete-model", "for delete")

    val response = modelResource.deleteModel(created.model.getMid, sessionUser)
    response.getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid) shouldBe null
  }

  it should "refuse to delete a model not owned by the user" in {
    val created =
      createModelFixture("forbidden-delete-model", "for forbidden delete", isPublic = true)

    assertThrows[ForbiddenException] {
      modelResource.deleteModel(created.model.getMid, sessionUser2)
    }
    modelDao.fetchOneByMid(created.model.getMid) should not be null
  }

  it should "surface a LakeFS 404 as NotFoundException when deleting a model whose repo is missing" in {
    val model = new Model
    model.setName("delete-model-no-repo")
    model.setRepositoryName("delete-model-no-repo")
    model.setDescription("for lakefs 404 mapping test")
    model.setOwnerUid(ownerUser.getUid)
    model.setIsPublic(true)
    model.setIsDownloadable(true)
    model.setFramework("pytorch")
    modelDao.insert(model)
    // intentionally no repo created in LakeFS

    val ex = intercept[NotFoundException] {
      modelResource.deleteModel(model.getMid, sessionUser)
    }
    assertStatus(ex, 404)
  }

  // ===========================================================================
  // update name / description / publicity / downloadable
  // ===========================================================================
  "updateModelName" should "rename a model the user can write to" in {
    val created = createModelFixture("rename-me", "d")

    val response = modelResource.updateModelName(
      ModelResource.ModelNameModification(created.model.getMid, "renamed"),
      sessionUser
    )
    response.getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid).getName shouldEqual "renamed"
  }

  "updateModelDescription" should "update the description of a model the user can write to" in {
    val created = createModelFixture("describe-me", "old")

    val response = modelResource.updateModelDescription(
      ModelResource.ModelDescriptionModification(created.model.getMid, "new description"),
      sessionUser
    )
    response.getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid).getDescription shouldEqual "new description"
  }

  "toggleModelPublicity" should "flip the public flag for a writer" in {
    val created = createModelFixture("publicity-model", "d", isPublic = false)

    modelResource.toggleModelPublicity(created.model.getMid, sessionUser).getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid).getIsPublic shouldBe true
  }

  "toggleModelDownloadable" should "flip the downloadable flag for the owner" in {
    val created = createModelFixture("downloadable-model", "d", isDownloadable = true)

    modelResource
      .toggleModelDownloadable(created.model.getMid, sessionUser)
      .getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid).getIsDownloadable shouldBe false
  }

  it should "forbid a non-owner from toggling downloadable" in {
    val created = createModelFixture("downloadable-forbidden-model", "d", isPublic = true)

    assertThrows[ForbiddenException] {
      modelResource.toggleModelDownloadable(created.model.getMid, sessionUser2)
    }
  }

  it should "refuse to rename a model to a name the owner already uses" in {
    createModelFixture("dup-target", "d")
    val second = createModelFixture("dup-source", "d")

    assertThrows[BadRequestException] {
      modelResource.updateModelName(
        ModelResource.ModelNameModification(second.model.getMid, "dup-target"),
        sessionUser
      )
    }
  }

  it should "forbid a user without write access from renaming or re-describing a model" in {
    val created = createModelFixture("no-write-updates", "d", isPublic = true)

    assertThrows[ForbiddenException] {
      modelResource.updateModelName(
        ModelResource.ModelNameModification(created.model.getMid, "hijacked"),
        sessionUser2
      )
    }
    assertThrows[ForbiddenException] {
      modelResource.updateModelDescription(
        ModelResource.ModelDescriptionModification(created.model.getMid, "hijacked"),
        sessionUser2
      )
    }
  }

  // ===========================================================================
  // getPublicModel / listModels public merge
  // ===========================================================================
  "getPublicModel" should "return a public model without authentication" in {
    val created = createModelFixture("public-get-model", "d", isPublic = true)

    val dashboard = modelResource.getPublicModel(created.model.getMid)
    dashboard.model.getMid shouldEqual created.model.getMid
  }

  it should "forbid access to a private model" in {
    val created = createModelFixture("public-get-private-model", "d", isPublic = false)

    assertThrows[ForbiddenException] {
      modelResource.getPublicModel(created.model.getMid)
    }
  }

  "listModels" should "omit public models owned by another user" in {
    val othersPublic =
      createModelFixture("others-public-model", "d", isPublic = true, user = sessionUser2)

    // /model/list backs the "Your Work" page, so it lists only what the caller was granted.
    // Public models are discovered through the hub and fetched with getPublicModel.
    modelResource
      .listModels(sessionUser)
      .find(_.model.getMid == othersPublic.model.getMid) shouldBe empty
    modelResource
      .getPublicModel(othersPublic.model.getMid)
      .model
      .getMid shouldEqual othersPublic.model.getMid
  }
}
