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

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import jakarta.annotation.security.{PermitAll, RolesAllowed}
import jakarta.ws.rs._
import jakarta.ws.rs.core._
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.Model.MODEL
import org.apache.texera.dao.jooq.generated.tables.daos.{ModelDao, ModelUserAccessDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Model, ModelUserAccess}
import org.apache.texera.service.resource.ManagedResource.{Model => MODEL_RESOURCE}
import org.apache.texera.service.resource.ModelAccessResource._
import org.apache.texera.service.resource.ModelResource.{context, _}
import org.apache.texera.service.util.S3StorageClient
import org.apache.texera.service.util.LakeFSExceptionHandler.withLakeFSErrorHandling
import org.jooq.{DSLContext, EnumType}

object ModelResource {

  // MVP supports a single framework; stored on the model so later frameworks can be added.
  private val DEFAULT_FRAMEWORK = "pytorch"

  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  /**
    * Helper function to get the model from DB using mid
    */
  private def getModelByID(ctx: DSLContext, mid: Integer): Model = {
    val modelDao = new ModelDao(ctx.configuration())
    val model = modelDao.fetchOneByMid(mid)
    if (model == null) {
      throw new NotFoundException(f"Model $mid not found")
    }
    model
  }

  case class DashboardModel(
      model: Model,
      ownerEmail: String,
      accessPrivilege: EnumType,
      isOwner: Boolean,
      size: Long
  )

  case class CreateModelRequest(
      modelName: String,
      modelDescription: String,
      isModelPublic: Boolean,
      isModelDownloadable: Boolean,
      framework: String,
      format: String
  )

  case class ModelDescriptionModification(mid: Integer, description: String)

  case class ModelNameModification(mid: Integer, name: String)
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/model")
class ModelResource extends LazyLogging {
  private val ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE = "User has no access to this model"

  /**
    * Helper function to get the model from DB with additional information including
    * user access privilege and owner email
    */
  private def getDashboardModel(
      ctx: DSLContext,
      mid: Integer,
      requesterUid: Option[Integer]
  ): DashboardModel = {
    val targetModel = getModelByID(ctx, mid)

    if (requesterUid.isEmpty && !targetModel.getIsPublic) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
    } else if (requesterUid.exists(uid => !userHasReadAccess(ctx, mid, uid))) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
    }

    val userAccessPrivilege = requesterUid
      .map(uid => getModelUserAccessPrivilege(ctx, mid, uid))
      .getOrElse(PrivilegeEnum.READ)

    val isOwner = requesterUid.contains(targetModel.getOwnerUid)

    DashboardModel(
      targetModel,
      getOwner(ctx, mid).getEmail,
      userAccessPrivilege,
      isOwner,
      withLakeFSErrorHandling(s"retrieving the size of model '${targetModel.getName}'") {
        LakeFSStorageClient.retrieveRepositorySize(targetModel.getRepositoryName)
      }
    )
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/create")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def createModel(
      request: CreateModelRequest,
      @Auth user: SessionUser
  ): DashboardModel = {

    withTransaction(context) { ctx =>
      val uid = user.getUid
      val modelUserAccessDao: ModelUserAccessDao = new ModelUserAccessDao(ctx.configuration())

      val modelName = request.modelName
      val modelDescription = request.modelDescription
      val isModelPublic = request.isModelPublic
      val isModelDownloadable = request.isModelDownloadable

      ResourceNaming.validateName(MODEL_RESOURCE.label, modelName)
      ResourceNaming.requireNameAvailable(ctx, MODEL_RESOURCE, uid, modelName)

      // insert the model into the database
      val model = new Model()
      model.setName(modelName)
      model.setDescription(modelDescription)
      model.setIsPublic(isModelPublic)
      model.setIsDownloadable(isModelDownloadable)
      model.setOwnerUid(uid)
      model.setFramework(Option(request.framework).filter(_.nonEmpty).getOrElse(DEFAULT_FRAMEWORK))
      model.setFormat(request.format)

      // insert record and get created model with mid
      val createdModel = ResourceNaming.failOnDuplicateName(MODEL_RESOURCE.label) {
        ctx
          .insertInto(MODEL)
          .set(ctx.newRecord(MODEL, model))
          .returning()
          .fetchOne()
      }

      // Initialize the repository in LakeFS
      val repositoryName = s"model-${createdModel.getMid}"
      try {
        withLakeFSErrorHandling(s"creating the repository of model '${model.getName}'") {
          LakeFSStorageClient.initRepo(repositoryName)
        }
      } catch {
        case e: Exception =>
          // roll back the model record so a failed LakeFS init leaves no orphan row
          ctx
            .deleteFrom(MODEL)
            .where(MODEL.MID.eq(createdModel.getMid))
            .execute()
          e match {
            case web: WebApplicationException => throw web
            case other =>
              throw new WebApplicationException(
                s"Failed to create the model: ${other.getMessage}"
              )
          }
      }

      // update repository name of the created model
      createdModel.setRepositoryName(repositoryName)
      createdModel.update()

      // Insert the requester as the WRITE access user for this model
      val modelUserAccess = new ModelUserAccess()
      modelUserAccess.setMid(createdModel.getMid)
      modelUserAccess.setUid(uid)
      modelUserAccess.setPrivilege(PrivilegeEnum.WRITE)
      modelUserAccessDao.insert(modelUserAccess)

      DashboardModel(
        createdModel.into(classOf[Model]),
        user.getEmail,
        PrivilegeEnum.WRITE,
        isOwner = true,
        0
      )
    }
  }

  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}")
  def deleteModel(@PathParam("mid") mid: Integer, @Auth user: SessionUser): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      val modelDao = new ModelDao(ctx.configuration())
      val model = getModelByID(ctx, mid)
      if (!userOwnModel(ctx, model.getMid, uid)) {
        // throw the exception that user has no access to certain model
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      withLakeFSErrorHandling(s"deleting the repository of model '${model.getName}'") {
        LakeFSStorageClient.deleteRepo(model.getRepositoryName)
      }
      // delete the directory on S3
      if (
        S3StorageClient.directoryExists(StorageConfig.lakefsBucketName, model.getRepositoryName)
      ) {
        S3StorageClient.deleteDirectory(StorageConfig.lakefsBucketName, model.getRepositoryName)
      }

      // delete the model from the DB
      modelDao.deleteById(model.getMid)

      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/description")
  def updateModelDescription(
      modificator: ModelDescriptionModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val uid = sessionUser.getUid
      val modelDao = new ModelDao(ctx.configuration())
      val model = getModelByID(ctx, modificator.mid)
      if (!userHasWriteAccess(ctx, modificator.mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      model.setDescription(modificator.description)
      modelDao.update(model)
      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/name")
  def updateModelName(
      modificator: ModelNameModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val uid = sessionUser.getUid
      val modelDao = new ModelDao(ctx.configuration())
      val model = getModelByID(ctx, modificator.mid)
      if (!userHasWriteAccess(ctx, modificator.mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      ResourceNaming.validateName(MODEL_RESOURCE.label, modificator.name)
      ResourceNaming.requireNameAvailable(
        ctx,
        MODEL_RESOURCE,
        model.getOwnerUid,
        modificator.name,
        excludingId = Some(model.getMid)
      )

      model.setName(modificator.name)
      ResourceNaming.failOnDuplicateName(MODEL_RESOURCE.label) {
        modelDao.update(model)
      }
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/update/publicity")
  def toggleModelPublicity(
      @PathParam("mid") mid: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val modelDao = new ModelDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val existedModel = getModelByID(ctx, mid)
      val newPublicStatus = !existedModel.getIsPublic
      existedModel.setIsPublic(newPublicStatus)

      modelDao.update(existedModel)
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/update/downloadable")
  def toggleModelDownloadable(
      @PathParam("mid") mid: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val modelDao = new ModelDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userOwnModel(ctx, mid, uid)) {
        throw new ForbiddenException("Only model owners can modify download permissions")
      }

      val existedModel = getModelByID(ctx, mid)
      val newDownloadableStatus = !existedModel.getIsDownloadable

      existedModel.setIsDownloadable(newDownloadableStatus)

      modelDao.update(existedModel)
      Response.ok().build()
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/list")
  def listModels(
      @Auth user: SessionUser
  ): List[DashboardModel] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      ResourceAccess.listVisible(
        ctx,
        MODEL_RESOURCE,
        uid,
        classOf[Model],
        (model: Model) => model.getMid
      )(
        fromGrant = (model, ownerEmail, privilege, isOwner) =>
          Some(
            DashboardModel(
              isOwner = isOwner,
              model = model,
              accessPrivilege = privilege,
              ownerEmail = ownerEmail,
              size = 0
            )
          ),
        fromPublic = (model, ownerEmail) =>
          try {
            Some(
              DashboardModel(
                isOwner = false,
                model = model,
                accessPrivilege = PrivilegeEnum.READ,
                ownerEmail = ownerEmail,
                size = LakeFSStorageClient.retrieveRepositorySize(model.getRepositoryName)
              )
            )
          } catch {
            case e: io.lakefs.clients.sdk.ApiException =>
              logger.error(
                s"LakeFS ApiException for model repository '${model.getRepositoryName}': ${e.getMessage}",
                e
              )
              None
          }
      )
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}")
  def getModel(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): DashboardModel = {
    val uid = user.getUid
    withTransaction(context)(ctx => getDashboardModel(ctx, mid, Some(uid)))
  }

  @GET
  @PermitAll
  @Path("/public/{mid}")
  def getPublicModel(
      @PathParam("mid") mid: Integer
  ): DashboardModel = {
    withTransaction(context)(ctx => getDashboardModel(ctx, mid, None))
  }
}
