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
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{ModelDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Model, User}
import org.apache.texera.service.MockLakeFS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

class ModelResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val ownerUser: User = {
    val user = new User
    user.setName("model_user")
    user.setPassword("123")
    user.setEmail("model_user@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val otherUser: User = {
    val user = new User
    user.setName("model_user2")
    user.setPassword("123")
    user.setEmail("model_user2@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

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

  lazy val modelResource = new ModelResource()

  lazy val sessionUser = new SessionUser(ownerUser)
  lazy val sessionUser2 = new SessionUser(otherUser)

  private def assertStatus(ex: WebApplicationException, status: Int): Unit =
    ex.getResponse.getStatus shouldEqual status

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(otherUser)

    baseModel.setOwnerUid(ownerUser.getUid)
    modelDao.insert(baseModel)
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
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
    val request = ModelResource.CreateModelRequest(
      modelName = "get-model",
      modelDescription = "for get",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "pytorch",
      format = null
    )
    val created = modelResource.createModel(request, sessionUser)

    val dashboard = modelResource.getModel(created.model.getMid, sessionUser)
    dashboard.model.getMid shouldEqual created.model.getMid
    dashboard.size should be >= 0L
  }

  it should "forbid a stranger from getting a private model" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "private-get-model",
      modelDescription = "private",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "pytorch",
      format = null
    )
    val created = modelResource.createModel(request, sessionUser)

    assertThrows[ForbiddenException] {
      modelResource.getModel(created.model.getMid, sessionUser2)
    }
  }

  "listModels" should "include models the user owns" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "listed-model",
      modelDescription = "for list",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "pytorch",
      format = null
    )
    val created = modelResource.createModel(request, sessionUser)

    val listed = modelResource.listModels(sessionUser)
    listed.map(_.model.getMid) should contain(created.model.getMid)
  }

  // ===========================================================================
  // deleteModel
  // ===========================================================================
  "deleteModel" should "delete a model successfully if the user owns it" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "delete-model",
      modelDescription = "for delete",
      isModelPublic = false,
      isModelDownloadable = true,
      framework = "pytorch",
      format = null
    )
    val created = modelResource.createModel(request, sessionUser)

    val response = modelResource.deleteModel(created.model.getMid, sessionUser)
    response.getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid) shouldBe null
  }

  it should "refuse to delete a model not owned by the user" in {
    val request = ModelResource.CreateModelRequest(
      modelName = "forbidden-delete-model",
      modelDescription = "for forbidden delete",
      isModelPublic = true,
      isModelDownloadable = true,
      framework = "pytorch",
      format = null
    )
    val created = modelResource.createModel(request, sessionUser)

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
    val created = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "rename-me",
        modelDescription = "d",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

    val response = modelResource.updateModelName(
      ModelResource.ModelNameModification(created.model.getMid, "renamed"),
      sessionUser
    )
    response.getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid).getName shouldEqual "renamed"
  }

  "updateModelDescription" should "update the description of a model the user can write to" in {
    val created = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "describe-me",
        modelDescription = "old",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

    val response = modelResource.updateModelDescription(
      ModelResource.ModelDescriptionModification(created.model.getMid, "new description"),
      sessionUser
    )
    response.getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid).getDescription shouldEqual "new description"
  }

  "toggleModelPublicity" should "flip the public flag for a writer" in {
    val created = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "publicity-model",
        modelDescription = "d",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

    modelResource.toggleModelPublicity(created.model.getMid, sessionUser).getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid).getIsPublic shouldBe true
  }

  "toggleModelDownloadable" should "flip the downloadable flag for the owner" in {
    val created = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "downloadable-model",
        modelDescription = "d",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

    modelResource
      .toggleModelDownloadable(created.model.getMid, sessionUser)
      .getStatus shouldEqual 200
    modelDao.fetchOneByMid(created.model.getMid).getIsDownloadable shouldBe false
  }

  it should "forbid a non-owner from toggling downloadable" in {
    val created = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "downloadable-forbidden-model",
        modelDescription = "d",
        isModelPublic = true,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

    assertThrows[ForbiddenException] {
      modelResource.toggleModelDownloadable(created.model.getMid, sessionUser2)
    }
  }

  it should "refuse to rename a model to a name the owner already uses" in {
    modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "dup-target",
        modelDescription = "d",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )
    val second = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "dup-source",
        modelDescription = "d",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

    assertThrows[BadRequestException] {
      modelResource.updateModelName(
        ModelResource.ModelNameModification(second.model.getMid, "dup-target"),
        sessionUser
      )
    }
  }

  it should "forbid a user without write access from renaming or re-describing a model" in {
    val created = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "no-write-updates",
        modelDescription = "d",
        isModelPublic = true,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

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
    val created = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "public-get-model",
        modelDescription = "d",
        isModelPublic = true,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

    val dashboard = modelResource.getPublicModel(created.model.getMid)
    dashboard.model.getMid shouldEqual created.model.getMid
  }

  it should "forbid access to a private model" in {
    val created = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "public-get-private-model",
        modelDescription = "d",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

    assertThrows[ForbiddenException] {
      modelResource.getPublicModel(created.model.getMid)
    }
  }

  "listModels" should "include public models owned by another user" in {
    val othersPublic = modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "others-public-model",
        modelDescription = "d",
        isModelPublic = true,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser2
    )

    val listed = modelResource.listModels(sessionUser)
    val entry = listed.find(_.model.getMid == othersPublic.model.getMid)
    entry should not be empty
    entry.get.isOwner shouldBe false
    entry.get.accessPrivilege shouldEqual PrivilegeEnum.READ
  }

  private def createWith(
      name: String,
      framework: String,
      format: String
  ): ModelResource.DashboardModel =
    modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = name,
        modelDescription = "label checks",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = framework,
        format = format
      ),
      sessionUser
    )

  "createModel" should "accept every supported framework" in {
    ModelResource.SUPPORTED_FRAMEWORKS.zipWithIndex.foreach {
      case (framework, i) =>
        createWith(s"framework-ok-$i", framework, null).model.getFramework shouldEqual framework
    }
  }

  it should "accept every supported format" in {
    ModelResource.SUPPORTED_FORMATS.zipWithIndex.foreach {
      case (format, i) =>
        createWith(s"format-ok-$i", "pytorch", format).model.getFormat shouldEqual format
    }
  }

  it should "reject an unrecognised framework" in {
    val ex = intercept[BadRequestException] {
      createWith("framework-bad", "banana", null)
    }
    ex.getMessage should include("Unsupported framework")
    ex.getMessage should include("pytorch")
  }

  it should "reject an unrecognised format" in {
    intercept[BadRequestException] {
      createWith("format-bad", "pytorch", "docx")
    }.getMessage should include("Unsupported format")
  }

  it should "treat a blank format as unspecified rather than invalid" in {
    createWith("format-blank", "pytorch", "   ").model.getFormat shouldBe null
  }

  it should "trim surrounding whitespace off a label before validating it" in {
    createWith("framework-padded", "  pytorch  ", null).model.getFramework shouldEqual "pytorch"
  }

  // ===========================================================================
  // user-model-owners
  // ===========================================================================
  "retrieveOwners" should "return the distinct owners of the models the user can access" in {
    createWith("owned-by-me-1", "pytorch", null)
    createWith("owned-by-me-2", "pytorch", null)

    val owners = modelResource.retrieveOwners(sessionUser).asScala.toList
    owners should contain(ownerUser.getEmail)
    owners.distinct shouldEqual owners
  }

  it should "not list the owner of a model the user was never granted access to" in {
    modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = "private-to-other-user",
        modelDescription = "d",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser2
    )

    // Access is granted per model, so the other user only shows up once the
    // requester actually holds a model_user_access row for one of their models.
    modelResource.retrieveOwners(sessionUser).asScala.toList should not contain otherUser.getEmail
  }
}
