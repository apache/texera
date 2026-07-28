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
import org.apache.texera.amber.core.storage.model.OnVersionedFileResource
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.amber.core.storage.{DocumentFactory, FileResolver}
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.{SiteSettings, SqlServer}
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.Model.MODEL
import org.apache.texera.dao.jooq.generated.tables.ModelUploadSession.MODEL_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.ModelUploadSessionPart.MODEL_UPLOAD_SESSION_PART
import org.apache.texera.dao.jooq.generated.tables.ModelUserAccess.MODEL_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.ModelVersion.MODEL_VERSION
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.daos.{
  ModelDao,
  ModelUserAccessDao,
  ModelVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Model, ModelUserAccess, ModelVersion}
import org.apache.texera.dao.jooq.generated.tables.records.ModelUploadSessionRecord
import org.apache.texera.service.`type`.{DatasetFileNode, Diff, ExistingUploadFilesRequest}
import org.apache.texera.service.resource.ModelAccessResource._
import org.apache.texera.service.resource.ModelResource.{context, _}
import org.apache.texera.service.util.PresignedDownloadUtils
import org.apache.texera.service.util.ResourceUploadUtils.{
  matchExistingUploads,
  normalizeUploadRequest,
  put,
  validateAndNormalizeFilePathOrThrow
}
import org.apache.texera.service.util.S3StorageClient
import org.apache.texera.service.util.S3StorageClient.{
  MAXIMUM_NUM_OF_MULTIPART_S3_PARTS,
  MINIMUM_NUM_OF_MULTIPART_S3_PART,
  PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS
}
import org.apache.texera.service.util.LakeFSExceptionHandler.withLakeFSErrorHandling
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.jooq.impl.DSL.{inline => inl}
import org.jooq.{DSLContext, EnumType, Record2, Result}
import software.amazon.awssdk.services.s3.model.UploadPartResponse

import java.io.{InputStream, OutputStream}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util
import java.util.Optional
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.Try

object ModelResource {

  // MVP supports a single framework; stored on the model so later frameworks can be added.
  private val DEFAULT_FRAMEWORK = "pytorch"

  // Recognised values for the `framework` and `format` labels.
  val SUPPORTED_FRAMEWORKS: Set[String] = Set("pytorch", "tensorflow", "onnx", "sklearn")

  val SUPPORTED_FORMATS: Set[String] =
    Set("torchscript", "state-dict", "safetensors", "onnx", "savedmodel", "joblib", "pickle")

  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  private def singleFileUploadMaxBytes(defaultMiB: Long = 20L): Long =
    SiteSettings.getLong("single_file_upload_max_size_mib", defaultMiB) * 1024L * 1024L

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

  /**
    * Helper function to get the model version from DB using mvid
    */
  private def getModelVersionByID(ctx: DSLContext, mvid: Integer): ModelVersion = {
    val modelVersionDao = new ModelVersionDao(ctx.configuration())
    val version = modelVersionDao.fetchOneByMvid(mvid)
    if (version == null) {
      throw new NotFoundException("Model Version not found")
    }
    version
  }

  /**
    * Helper function to get the latest model version from the DB
    */
  private def getLatestModelVersion(ctx: DSLContext, mid: Integer): Option[ModelVersion] = {
    ctx
      .selectFrom(MODEL_VERSION)
      .where(MODEL_VERSION.MID.eq(mid))
      .orderBy(MODEL_VERSION.CREATION_TIME.desc())
      .limit(1)
      .fetchOptionalInto(classOf[ModelVersion])
      .toScala
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

  case class DashboardModelVersion(
      modelVersion: ModelVersion,
      fileNodes: List[DatasetFileNode]
  )

  case class ModelVersionRootFileNodesResponse(
      fileNodes: List[DatasetFileNode],
      size: Long
  )
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/model")
class ModelResource extends LazyLogging {
  private val ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE = "User has no access to this model"
  private val ERR_MODEL_VERSION_NOT_FOUND_MESSAGE = "The version of the model not found"

  private val MODEL_NAME_MAX_LENGTH = 128
  private val MODEL_NAME_PATTERN = "^[A-Za-z0-9_-]+$".r

  /**
    * Validates the model name.
    *
    * Rules:
    * - Must be 1 to 128 characters long.
    * - Only letters, numbers, underscores, and hyphens are allowed.
    *
    * @param name The model name to validate.
    * @throws jakarta.ws.rs.BadRequestException if the name is invalid.
    */
  /**
    * Rejects a framework/format label the server does not recognise, so that what a
    * later loader dispatches on is always one of a known set.
    *
    * @throws jakarta.ws.rs.BadRequestException if the value is outside `allowed`
    */
  private def validateLabel(field: String, value: String, allowed: Set[String]): Unit = {
    if (!allowed.contains(value)) {
      throw new BadRequestException(
        s"Unsupported $field '$value'. Supported values: ${allowed.toList.sorted.mkString(", ")}."
      )
    }
  }

  private def validateModelName(name: String): Unit = {
    if (name == null || !MODEL_NAME_PATTERN.matches(name)) {
      throw new BadRequestException(
        "Invalid model name: only letters, numbers, underscores, and hyphens are allowed."
      )
    }
    if (name.length > MODEL_NAME_MAX_LENGTH) {
      throw new BadRequestException(
        s"Invalid model name: name must be at most $MODEL_NAME_MAX_LENGTH characters long."
      )
    }
  }

  /**
    * Runs a model write and translates a (owner_uid, name) unique-constraint
    * violation into the same BadRequestException the pre-checks throw, so
    * requests losing a concurrent race get a 400 instead of a 500.
    */
  private[resource] def failOnDuplicateModelName[T](op: => T): T = {
    try op
    catch {
      case e: DataAccessException =>
        if (e.sqlState() == "23505") {
          throw new BadRequestException("Model with the same name already exists")
        }
        throw e
    }
  }

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

      validateModelName(modelName)

      val framework =
        Option(request.framework).map(_.trim).filter(_.nonEmpty).getOrElse(DEFAULT_FRAMEWORK)
      validateLabel("framework", framework, SUPPORTED_FRAMEWORKS)
      val format = Option(request.format).map(_.trim).filter(_.nonEmpty)
      format.foreach(validateLabel("format", _, SUPPORTED_FORMATS))

      // Check if a model with the same name already exists
      val duplicateExists = ctx.fetchExists(
        ctx
          .selectFrom(MODEL)
          .where(MODEL.OWNER_UID.eq(uid))
          .and(MODEL.NAME.eq(modelName))
      )
      if (duplicateExists) {
        throw new BadRequestException("Model with the same name already exists")
      }

      // insert the model into the database
      val model = new Model()
      model.setName(modelName)
      model.setDescription(modelDescription)
      model.setIsPublic(isModelPublic)
      model.setIsDownloadable(isModelDownloadable)
      model.setOwnerUid(uid)
      model.setFramework(framework)
      model.setFormat(format.orNull)

      // insert record and get created model with mid
      val createdModel = failOnDuplicateModelName {
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

      validateModelName(modificator.name)

      // Check if the owner already has another model with the same name
      val duplicateExists = ctx.fetchExists(
        ctx
          .selectFrom(MODEL)
          .where(MODEL.OWNER_UID.eq(model.getOwnerUid))
          .and(MODEL.NAME.eq(modificator.name))
          .and(MODEL.MID.notEqual(model.getMid))
      )
      if (duplicateExists) {
        throw new BadRequestException("Model with the same name already exists")
      }

      model.setName(modificator.name)
      failOnDuplicateModelName {
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
      var accessibleModels: ListBuffer[DashboardModel] = ListBuffer()
      // first fetch all models user have explicit access to
      accessibleModels = ListBuffer.from(
        ctx
          .select()
          .from(
            MODEL
              .leftJoin(MODEL_USER_ACCESS)
              .on(MODEL_USER_ACCESS.MID.eq(MODEL.MID))
              .leftJoin(USER)
              .on(USER.UID.eq(MODEL.OWNER_UID))
          )
          .where(MODEL_USER_ACCESS.UID.eq(uid))
          .fetch()
          .map(record => {
            val model = record.into(MODEL).into(classOf[Model])
            val modelAccess = record.into(MODEL_USER_ACCESS).into(classOf[ModelUserAccess])
            val ownerEmail = record.into(USER).getEmail
            DashboardModel(
              isOwner = model.getOwnerUid == uid,
              model = model,
              accessPrivilege = modelAccess.getPrivilege,
              ownerEmail = ownerEmail,
              size = 0
            )
          })
          .asScala
      )

      // then we fetch the public models and merge it as a part of the result if not exist
      val publicModels = ctx
        .select()
        .from(
          MODEL
            .leftJoin(USER)
            .on(USER.UID.eq(MODEL.OWNER_UID))
        )
        .where(MODEL.IS_PUBLIC.eq(true))
        .fetch()
        .asScala
        .flatMap { record =>
          val model = record.into(MODEL).into(classOf[Model])
          val ownerEmail = record.into(USER).getEmail
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
        }
      publicModels.foreach { publicModel =>
        if (!accessibleModels.exists(_.model.getMid == publicModel.model.getMid)) {
          accessibleModels = accessibleModels :+ publicModel
        }
      }
      accessibleModels.toList
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

  // ===========================================================================
  // Versioning
  // ===========================================================================

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/version/create")
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def createModelVersion(
      versionName: String,
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): DashboardModelVersion = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val model = getModelByID(ctx, mid)
      val modelName = model.getName
      val repositoryName = model.getRepositoryName

      // Check if there are any changes in LakeFS before creating a new version
      val diffs = withLakeFSErrorHandling {
        LakeFSStorageClient.retrieveUncommittedObjects(repoName = repositoryName)
      }

      if (diffs.isEmpty) {
        throw new WebApplicationException(
          "No changes detected in model. Version creation aborted.",
          Response.Status.BAD_REQUEST
        )
      }

      // Generate a new version name
      val versionCount = ctx
        .selectCount()
        .from(MODEL_VERSION)
        .where(MODEL_VERSION.MID.eq(mid))
        .fetchOne(0, classOf[Int])

      val sanitizedVersionName = Option(versionName).filter(_.nonEmpty).getOrElse("")
      val newVersionName = if (sanitizedVersionName.isEmpty) {
        s"v${versionCount + 1}"
      } else {
        s"v${versionCount + 1} - $sanitizedVersionName"
      }

      // Create a commit in LakeFS
      val commit = withLakeFSErrorHandling {
        LakeFSStorageClient.createCommit(
          repoName = repositoryName,
          branch = "main",
          commitMessage = s"Created model version: $newVersionName"
        )
      }

      if (commit == null || commit.getId == null) {
        throw new WebApplicationException(
          "Failed to create commit in LakeFS. Version creation aborted.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      // Create a new model version entry in the database
      val modelVersion = new ModelVersion()
      modelVersion.setMid(mid)
      modelVersion.setCreatorUid(uid)
      modelVersion.setName(newVersionName)
      modelVersion.setVersionHash(commit.getId) // Store LakeFS version hash

      val insertedVersion = ctx
        .insertInto(MODEL_VERSION)
        .set(ctx.newRecord(MODEL_VERSION, modelVersion))
        .returning()
        .fetchOne()
        .into(classOf[ModelVersion])

      // Retrieve committed file structure
      val fileNodes = withLakeFSErrorHandling {
        LakeFSStorageClient.retrieveObjectsOfVersion(repositoryName, commit.getId)
      }

      DashboardModelVersion(
        insertedVersion,
        DatasetFileNode
          .fromLakeFSRepositoryCommittedObjects(
            Map((user.getEmail, modelName, newVersionName) -> fileNodes)
          )
      )
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/version/list")
  def getModelVersionList(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): List[ModelVersion] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val model = getModelByID(ctx, mid)
      if (!userHasReadAccess(ctx, model.getMid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      fetchModelVersions(ctx, model.getMid)
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/version/latest")
  def retrieveLatestModelVersion(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): DashboardModelVersion = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      if (!userHasReadAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      val model = getModelByID(ctx, mid)
      val latestVersion = getLatestModelVersion(ctx, mid).getOrElse(
        throw new NotFoundException(ERR_MODEL_VERSION_NOT_FOUND_MESSAGE)
      )
      DashboardModelVersion(latestVersion, versionRootFileNodes(ctx, mid, latestVersion, Some(uid)))
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/version/{mvid}/rootFileNodes")
  def retrieveModelVersionRootFileNodes(
      @PathParam("mid") mid: Integer,
      @PathParam("mvid") mvid: Integer,
      @Auth user: SessionUser
  ): ModelVersionRootFileNodesResponse = {
    val uid = user.getUid
    withTransaction(context)(ctx => fetchModelVersionRootFileNodes(ctx, mid, mvid, Some(uid)))
  }

  // ===========================================================================
  // Download
  // ===========================================================================

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download")
  def getPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response = {
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, user.getUid)
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download-s3")
  def getPresignedUrlWithS3(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response = {
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, user.getUid)
  }

  @GET
  @PermitAll
  @Path("/public-presign-download")
  def getPublicPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String
  ): Response = {
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, null)
  }

  @GET
  @PermitAll
  @Path("/public-presign-download-s3")
  def getPublicPresignedUrlWithS3(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String
  ): Response = {
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, null)
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/versionZip")
  def getModelVersionZip(
      @PathParam("mid") mid: Integer,
      @QueryParam("mvid") mvid: Integer, // Model version ID, nullable
      @QueryParam("latest") latest: java.lang.Boolean, // Flag to get latest version, nullable
      @Auth user: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      if ((mvid != null && latest != null) || (mvid == null && latest == null)) {
        throw new BadRequestException("Specify exactly one: mvid=<ID> OR latest=true")
      }

      val uid = user.getUid
      if (!userHasReadAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val model = getModelByID(ctx, mid)
      // Non-owners can download only if the owner marked the model downloadable.
      if (!userOwnModel(ctx, mid, uid) && !model.getIsDownloadable) {
        throw new ForbiddenException("Model download is not allowed")
      }

      val modelVersion = if (mvid != null) {
        getModelVersionByID(ctx, mvid)
      } else {
        getLatestModelVersion(ctx, mid).getOrElse(
          throw new NotFoundException(ERR_MODEL_VERSION_NOT_FOUND_MESSAGE)
        )
      }

      val modelName = model.getName
      val repositoryName = model.getRepositoryName
      val versionHash = modelVersion.getVersionHash
      val objects = withLakeFSErrorHandling(
        s"listing files of version '$versionHash' of model '$modelName'"
      ) {
        LakeFSStorageClient.retrieveObjectsOfVersion(repositoryName, versionHash)
      }

      if (objects.isEmpty) {
        Response
          .status(Response.Status.NOT_FOUND)
          .entity(s"No objects found in version $versionHash of repository $repositoryName")
          .build()
      } else {
        val streamingOutput = new StreamingOutput {
          override def write(outputStream: OutputStream): Unit = {
            val zipOut = new ZipOutputStream(outputStream)
            try {
              objects.foreach { obj =>
                val filePath = obj.getPath
                val file = withLakeFSErrorHandling(s"downloading file '$filePath' for the zip") {
                  LakeFSStorageClient.getFileFromRepo(repositoryName, versionHash, filePath)
                }

                zipOut.putNextEntry(new ZipEntry(filePath))
                Files.copy(Paths.get(file.toURI), zipOut)
                zipOut.closeEntry()
              }
            } finally {
              zipOut.close()
            }
          }
        }

        Response
          .ok(streamingOutput, "application/zip")
          .header(
            "Content-Disposition",
            s"""attachment; filename="$modelName-${modelVersion.getName}.zip""""
          )
          .build()
      }
    }
  }

  // ===========================================================================
  // File upload (one-shot + session-based multipart)
  // ===========================================================================

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/upload")
  @Consumes(Array(MediaType.APPLICATION_OCTET_STREAM))
  def uploadOneFileToModel(
      @PathParam("mid") mid: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @QueryParam("message") message: String,
      fileStream: InputStream,
      @Context headers: HttpHeaders,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    var repoName: String = null
    var filePath: String = null
    var uploadId: String = null
    var physicalAddress: String = null

    try {
      withTransaction(context) { ctx =>
        if (!userHasWriteAccess(ctx, mid, uid))
          throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)

        val model = getModelByID(ctx, mid)
        repoName = model.getRepositoryName
        filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name)

        // ---------- decide part-size & number-of-parts ----------
        val declaredLen = Option(headers.getHeaderString(HttpHeaders.CONTENT_LENGTH)).map(_.toLong)
        var partSize = StorageConfig.s3MultipartUploadPartSize

        declaredLen.foreach { ln =>
          val needed = ((ln + partSize - 1) / partSize).toInt
          if (needed > MAXIMUM_NUM_OF_MULTIPART_S3_PARTS)
            partSize = math.max(
              MINIMUM_NUM_OF_MULTIPART_S3_PART,
              ln / (MAXIMUM_NUM_OF_MULTIPART_S3_PARTS - 1)
            )
        }

        val expectedParts = declaredLen
          .map(ln => ((ln + partSize - 1) / partSize).toInt + 1)
          .getOrElse(MAXIMUM_NUM_OF_MULTIPART_S3_PARTS)

        // ---------- ask LakeFS for presigned URLs ----------
        val presign = LakeFSStorageClient
          .initiatePresignedMultipartUploads(repoName, filePath, expectedParts)
        uploadId = presign.getUploadId
        val presignedUrls = presign.getPresignedUrls.asScala.iterator
        physicalAddress = presign.getPhysicalAddress

        // ---------- stream & upload parts ----------
        val buf = new Array[Byte](partSize.toInt)
        var buffered = 0
        var partNumber = 1
        val completedParts = ListBuffer[(Int, String)]()

        @inline def flush(): Unit = {
          if (buffered == 0) return
          if (!presignedUrls.hasNext)
            throw new WebApplicationException("Ran out of presigned part URLs – ask for more parts")

          val etag = put(buf, buffered, presignedUrls.next(), partNumber)
          completedParts += ((partNumber, etag))
          partNumber += 1
          buffered = 0
        }

        var read = fileStream.read(buf, buffered, buf.length - buffered)
        while (read != -1) {
          buffered += read
          if (buffered == buf.length) flush()
          read = fileStream.read(buf, buffered, buf.length - buffered)
        }
        fileStream.close()
        flush()

        // ---------- complete upload ----------
        withLakeFSErrorHandling(s"completing the multipart upload of file '$filePath'") {
          LakeFSStorageClient.completePresignedMultipartUploads(
            repoName,
            filePath,
            uploadId,
            completedParts.toList,
            physicalAddress
          )
        }

        Response.ok(Map("message" -> s"Uploaded $filePath in ${completedParts.size} parts")).build()
      }
    } catch {
      case e: Exception =>
        if (repoName != null && filePath != null && uploadId != null && physicalAddress != null) {
          try {
            LakeFSStorageClient.abortPresignedMultipartUploads(
              repoName,
              filePath,
              uploadId,
              physicalAddress
            )
          } catch { case _: Throwable => () }
        }
        e match {
          case web: WebApplicationException => throw web
          case other =>
            throw new WebApplicationException(
              s"Failed to upload file to model: ${other.getMessage}",
              other
            )
        }
    }
  }

  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/file")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def deleteModelFile(
      @PathParam("mid") mid: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      val repositoryName = getModelByID(ctx, mid).getRepositoryName

      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      withLakeFSErrorHandling(s"deleting file '$filePath' from the model repository") {
        LakeFSStorageClient.deleteObject(repositoryName, filePath)
      }

      Response.ok().build()
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/diff")
  def getModelDiff(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): List[Diff] = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasReadAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      // Retrieve staged (uncommitted) changes from LakeFS
      val model = getModelByID(ctx, mid)
      val lakefsDiffs = withLakeFSErrorHandling {
        LakeFSStorageClient.retrieveUncommittedObjects(model.getRepositoryName)
      }

      lakefsDiffs.map(d =>
        Diff(
          d.getPath,
          d.getPathType.getValue,
          d.getType.getValue,
          Option(d.getSizeBytes).map(_.longValue())
        )
      )
    }
  }

  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/diff")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def resetModelFileDiff(
      @PathParam("mid") mid: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      val repositoryName = getModelByID(ctx, mid).getRepositoryName

      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      withLakeFSErrorHandling(s"resetting uncommitted changes of file '$filePath'") {
        LakeFSStorageClient.resetObjectUploadOrDeletion(repositoryName, filePath)
      }
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/existing-upload-files")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def findExistingUploadFiles(
      @PathParam("mid") mid: Integer,
      request: ExistingUploadFilesRequest,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      // Validate before any storage work: a malformed request is rejected without
      // spending LakeFS round trips on it.
      val requested = normalizeUploadRequest(
        Option(request).flatMap(request => Option(request.files)).getOrElse(List.empty)
      )

      val model = getModelByID(ctx, mid)
      val committed = getLatestModelVersion(ctx, mid)
        .map { v =>
          withLakeFSErrorHandling(
            s"retrieving committed files of model '${model.getName}'"
          ) {
            LakeFSStorageClient
              .retrieveObjectsOfVersion(model.getRepositoryName, v.getVersionHash)
              .map(obj => obj.getPath -> obj.getSizeBytes.longValue())
          }
        }
        .getOrElse(List.empty)

      val staged = withLakeFSErrorHandling(
        s"retrieving staged files of model '${model.getName}'"
      ) {
        LakeFSStorageClient.retrieveUncommittedObjects(model.getRepositoryName)
      }
        .filterNot(diff => Option(diff.getType).exists(_.getValue.equalsIgnoreCase("removed")))
        .flatMap(diff => Option(diff.getSizeBytes).map(size => diff.getPath -> size.longValue()))

      val matches = matchExistingUploads(requested, committed, staged)

      Response.ok(Map("filePaths" -> matches.asJava)).build()
    }
  }

  /**
    * This method returns all owner emails of the models that the user has access to
    *
    * @return OwnerEmail[]
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-model-owners")
  def retrieveOwners(@Auth user: SessionUser): util.List[String] = {
    context
      .selectDistinct(USER.EMAIL)
      .from(USER)
      .join(MODEL)
      .on(MODEL.OWNER_UID.eq(USER.UID))
      .join(MODEL_USER_ACCESS)
      .on(MODEL_USER_ACCESS.MID.eq(MODEL.MID))
      .where(MODEL_USER_ACCESS.UID.eq(user.getUid))
      .fetchInto(classOf[String])
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/multipart-upload")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def multipartUpload(
      @QueryParam("type") operationType: String,
      @QueryParam("ownerEmail") ownerEmail: String,
      @QueryParam("modelName") modelName: String,
      @QueryParam("filePath") filePath: String,
      @QueryParam("fileSizeBytes") fileSizeBytes: Optional[java.lang.Long],
      @QueryParam("partSizeBytes") partSizeBytes: Optional[java.lang.Long],
      @QueryParam("restart") restart: Optional[java.lang.Boolean],
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    val model: Model = getModelBy(ownerEmail, modelName)

    operationType.toLowerCase match {
      case "list" => listMultipartUploads(model.getMid, uid)
      case "init" =>
        initMultipartUpload(model.getMid, filePath, fileSizeBytes, partSizeBytes, restart, uid)
      case "finish" => finishMultipartUpload(model.getMid, filePath, uid)
      case "abort"  => abortMultipartUpload(model.getMid, filePath, uid)
      case _ =>
        throw new BadRequestException("Invalid type parameter. Use 'init', 'finish', or 'abort'.")
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_OCTET_STREAM))
  @Path("/multipart-upload/part")
  def uploadPart(
      @QueryParam("ownerEmail") modelOwnerEmail: String,
      @QueryParam("modelName") modelName: String,
      @QueryParam("filePath") encodedFilePath: String,
      @QueryParam("partNumber") partNumber: Int,
      partStream: InputStream,
      @Context headers: HttpHeaders,
      @Auth user: SessionUser
  ): Response = {

    val uid = user.getUid
    val model: Model = getModelBy(modelOwnerEmail, modelName)
    val mid = model.getMid

    if (encodedFilePath == null || encodedFilePath.isEmpty)
      throw new BadRequestException("filePath is required")
    if (partNumber < 1)
      throw new BadRequestException("partNumber must be >= 1")

    val filePath = validateAndNormalizeFilePathOrThrow(
      URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
    )

    val contentLength =
      Option(headers.getHeaderString(HttpHeaders.CONTENT_LENGTH))
        .map(_.trim)
        .flatMap(s => Try(s.toLong).toOption)
        .filter(_ > 0)
        .getOrElse {
          throw new BadRequestException("Invalid/Missing Content-Length")
        }

    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid))
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)

      val session = ctx
        .selectFrom(MODEL_UPLOAD_SESSION)
        .where(
          MODEL_UPLOAD_SESSION.UID
            .eq(uid)
            .and(MODEL_UPLOAD_SESSION.MID.eq(mid))
            .and(MODEL_UPLOAD_SESSION.FILE_PATH.eq(filePath))
        )
        .fetchOne()

      if (session == null)
        throw new NotFoundException("Upload session not found. Call type=init first.")

      val expectedParts: Int = session.getNumPartsRequested
      val fileSizeBytesValue: Long = session.getFileSizeBytes
      val partSizeBytesValue: Long = session.getPartSizeBytes

      if (fileSizeBytesValue <= 0L) {
        throw new WebApplicationException(
          s"Upload session has an invalid file size of $fileSizeBytesValue. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }
      if (partSizeBytesValue <= 0L) {
        throw new WebApplicationException(
          s"Upload session has an invalid part size of $partSizeBytesValue. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      // lastPartSize = fileSize - partSize*(expectedParts-1)
      val nMinus1: Long = expectedParts.toLong - 1L
      if (nMinus1 < 0L) {
        throw new WebApplicationException(
          s"Upload session has an invalid number of requested parts of $expectedParts. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }
      if (nMinus1 > 0L && partSizeBytesValue > Long.MaxValue / nMinus1) {
        throw new WebApplicationException(
          "Overflow while computing last part size",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }
      val prefixBytes: Long = partSizeBytesValue * nMinus1
      if (prefixBytes > fileSizeBytesValue) {
        throw new WebApplicationException(
          s"Upload session is invalid: computed bytes before last part ($prefixBytes) exceed declared file size ($fileSizeBytesValue). Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }
      val lastPartSize: Long = fileSizeBytesValue - prefixBytes
      if (lastPartSize <= 0L || lastPartSize > partSizeBytesValue) {
        throw new WebApplicationException(
          s"Upload session is invalid: computed last part size ($lastPartSize bytes) must be within 1..$partSizeBytesValue bytes. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      val allowedSize: Long =
        if (partNumber < expectedParts) partSizeBytesValue else lastPartSize

      if (partNumber > expectedParts) {
        throw new BadRequestException(
          s"$partNumber exceeds the requested parts on init: $expectedParts"
        )
      }

      if (partNumber < expectedParts && contentLength < MINIMUM_NUM_OF_MULTIPART_S3_PART) {
        throw new BadRequestException(
          s"Part $partNumber is too small ($contentLength bytes). " +
            s"All non-final parts must be >= $MINIMUM_NUM_OF_MULTIPART_S3_PART bytes."
        )
      }

      if (contentLength != allowedSize) {
        throw new BadRequestException(
          s"Invalid part size for partNumber=$partNumber. " +
            s"Expected Content-Length=$allowedSize, got $contentLength."
        )
      }

      val physicalAddr = Option(session.getPhysicalAddress).map(_.trim).getOrElse("")
      if (physicalAddr.isEmpty) {
        throw new WebApplicationException(
          "Upload session is missing physicalAddress. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      val uploadId = session.getUploadId
      val (bucket, key) =
        try LakeFSStorageClient.parsePhysicalAddress(physicalAddr)
        catch {
          case e: IllegalArgumentException =>
            throw new WebApplicationException(
              s"Upload session has invalid physicalAddress. Restart the upload. (${e.getMessage})",
              Response.Status.INTERNAL_SERVER_ERROR
            )
        }

      // Per-part lock: if another request is streaming the same part, fail fast.
      val partRow =
        try {
          ctx
            .selectFrom(MODEL_UPLOAD_SESSION_PART)
            .where(
              MODEL_UPLOAD_SESSION_PART.UPLOAD_ID
                .eq(uploadId)
                .and(MODEL_UPLOAD_SESSION_PART.PART_NUMBER.eq(partNumber))
            )
            .forUpdate()
            .noWait()
            .fetchOne()
        } catch {
          case e: DataAccessException
              if Option(e.getCause)
                .collect { case s: SQLException => s.getSQLState }
                .contains("55P03") =>
            throw new WebApplicationException(
              s"Part $partNumber is already being uploaded",
              Response.Status.CONFLICT
            )
        }

      if (partRow == null) {
        throw new WebApplicationException(
          s"Part row not initialized for part $partNumber. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      // Idempotency: if ETag already set, accept the retry quickly.
      val existing = Option(partRow.getEtag).map(_.trim).getOrElse("")
      if (existing.isEmpty) {
        val response: UploadPartResponse =
          S3StorageClient.uploadPartWithRequest(
            bucket = bucket,
            key = key,
            uploadId = uploadId,
            partNumber = partNumber,
            inputStream = partStream,
            contentLength = Some(contentLength)
          )

        val etagClean = Option(response.eTag()).map(_.replace("\"", "")).map(_.trim).getOrElse("")
        if (etagClean.isEmpty) {
          throw new WebApplicationException(
            s"Missing ETag returned from S3 for part $partNumber",
            Response.Status.INTERNAL_SERVER_ERROR
          )
        }

        ctx
          .update(MODEL_UPLOAD_SESSION_PART)
          .set(MODEL_UPLOAD_SESSION_PART.ETAG, etagClean)
          .where(
            MODEL_UPLOAD_SESSION_PART.UPLOAD_ID
              .eq(uploadId)
              .and(MODEL_UPLOAD_SESSION_PART.PART_NUMBER.eq(partNumber))
          )
          .execute()
      }
      Response.ok().build()
    }
  }

  // ===========================================================================
  // Private helpers
  // ===========================================================================

  private def generatePresignedResponse(
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Response = {
    resolveModelAndPath(encodedUrl, repositoryName, commitHash, uid) match {
      case Left(errorResponse) =>
        errorResponse

      case Right((resolvedRepositoryName, resolvedCommitHash, resolvedFilePath)) =>
        PresignedDownloadUtils.presignedResponse(
          resolvedRepositoryName,
          resolvedCommitHash,
          resolvedFilePath
        )
    }
  }

  /**
    * Resolve the (repository, commit, path) triple a presign request refers to, and
    * verify the caller may read it. The file is addressed either directly
    * (repositoryName + commitHash) or through a logical `/models/...` path.
    */
  private def resolveModelAndPath(
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Either[Response, (String, String, String)] = {
    val decodedPathStr = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name())

    PresignedDownloadUtils.requireBothOrNeither(repositoryName, commitHash) match {
      case Some(errorResponse) => Left(errorResponse)

      case None =>
        val resolved = withTransaction(context) { ctx =>
          val (resolvedRepositoryName, resolvedCommitHash, resolvedFilePath) =
            Option(repositoryName) match {
              case Some(repo) =>
                (repo, commitHash, decodedPathStr)
              case None =>
                val fileUri = FileResolver.resolve(decodedPathStr)
                val document =
                  DocumentFactory
                    .openReadonlyDocument(fileUri)
                    .asInstanceOf[OnVersionedFileResource]
                (
                  document.getRepositoryName(),
                  document.getVersionHash(),
                  document.getFileRelativePath()
                )
            }

          val modelDao = new ModelDao(ctx.configuration())
          val models = modelDao.fetchByRepositoryName(resolvedRepositoryName).asScala.toList

          if (models.isEmpty || !userHasReadAccess(ctx, models.head.getMid, uid))
            throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)

          (resolvedRepositoryName, resolvedCommitHash, resolvedFilePath)
        }
        Right(resolved)
    }
  }

  private def fetchModelVersions(ctx: DSLContext, mid: Integer): List[ModelVersion] = {
    ctx
      .selectFrom(MODEL_VERSION)
      .where(MODEL_VERSION.MID.eq(mid))
      .orderBy(MODEL_VERSION.CREATION_TIME.desc())
      .fetchInto(classOf[ModelVersion])
      .asScala
      .toList
  }

  /**
    * Builds the file-tree children of a single model version, drilling into the
    * owner/model/version nesting produced by DatasetFileNode.
    */
  private def versionRootFileNodes(
      ctx: DSLContext,
      mid: Integer,
      modelVersion: ModelVersion,
      uid: Option[Integer]
  ): List[DatasetFileNode] = {
    val model = getModelByID(ctx, mid)
    val ownerEmail = getOwner(ctx, mid).getEmail

    val modelsNode = DatasetFileNode
      .fromLakeFSRepositoryCommittedObjects(
        Map(
          (ownerEmail, model.getName, modelVersion.getName) -> LakeFSStorageClient
            .retrieveObjectsOfVersion(model.getRepositoryName, modelVersion.getVersionHash)
        )
      )
      .head

    val ownerNode = modelsNode.getChildren.headOption.getOrElse(
      throw new IllegalStateException(
        s"Model file tree for ${model.getName} is missing its owner node"
      )
    )

    ownerNode.children.get
      .find(_.getName == model.getName)
      .head
      .children
      .get
      .find(_.getName == modelVersion.getName)
      .head
      .children
      .get
  }

  private def fetchModelVersionRootFileNodes(
      ctx: DSLContext,
      mid: Integer,
      mvid: Integer,
      uid: Option[Integer]
  ): ModelVersionRootFileNodesResponse = {
    val model = getDashboardModel(ctx, mid, uid)
    val modelVersion = getModelVersionByID(ctx, mvid)
    val modelName = model.model.getName
    val repositoryName = model.model.getRepositoryName

    val modelsNode = DatasetFileNode
      .fromLakeFSRepositoryCommittedObjects(
        Map(
          (model.ownerEmail, modelName, modelVersion.getName) -> LakeFSStorageClient
            .retrieveObjectsOfVersion(repositoryName, modelVersion.getVersionHash)
        )
      )
      .head

    val ownerFileNode = modelsNode.getChildren.headOption.getOrElse(
      throw new IllegalStateException(
        s"Model file tree for $modelName is missing its owner node"
      )
    )

    ModelVersionRootFileNodesResponse(
      ownerFileNode.children.get
        .find(_.getName == modelName)
        .head
        .children
        .get
        .find(_.getName == modelVersion.getName)
        .head
        .children
        .get,
      DatasetFileNode.calculateTotalSize(List(modelsNode))
    )
  }

  private def getModelBy(ownerEmail: String, modelName: String): Model = {
    val model = context
      .select(MODEL.fields: _*)
      .from(MODEL)
      .leftJoin(USER)
      .on(USER.UID.eq(MODEL.OWNER_UID))
      .where(USER.EMAIL.eq(ownerEmail))
      .and(MODEL.NAME.eq(modelName))
      .fetchOneInto(classOf[Model])
    if (model == null) {
      throw new BadRequestException("Model not found")
    }
    model
  }

  private def listMultipartUploads(mid: Integer, requesterUid: Int): Response = {
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, requesterUid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val filePaths =
        ctx
          .selectDistinct(MODEL_UPLOAD_SESSION.FILE_PATH)
          .from(MODEL_UPLOAD_SESSION)
          .where(MODEL_UPLOAD_SESSION.MID.eq(mid))
          .and(
            DSL.condition(
              "created_at > current_timestamp - (? * interval '1 hour')",
              PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS
            )
          )
          .orderBy(MODEL_UPLOAD_SESSION.FILE_PATH.asc())
          .fetch(MODEL_UPLOAD_SESSION.FILE_PATH)
          .asScala
          .toList

      Response.ok(Map("filePaths" -> filePaths.asJava)).build()
    }
  }

  private def initMultipartUpload(
      mid: Integer,
      encodedFilePath: String,
      fileSizeBytes: Optional[java.lang.Long],
      partSizeBytes: Optional[java.lang.Long],
      restart: Optional[java.lang.Boolean],
      uid: Integer
  ): Response = {

    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val model = getModelByID(ctx, mid)
      val repositoryName = model.getRepositoryName

      val filePath =
        validateAndNormalizeFilePathOrThrow(
          URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
        )

      if (fileSizeBytes == null || !fileSizeBytes.isPresent)
        throw new BadRequestException("fileSizeBytes is required for initialization")
      if (partSizeBytes == null || !partSizeBytes.isPresent)
        throw new BadRequestException("partSizeBytes is required for initialization")

      val fileSizeBytesValue: Long = fileSizeBytes.get.longValue()
      val partSizeBytesValue: Long = partSizeBytes.get.longValue()

      if (fileSizeBytesValue <= 0L) throw new BadRequestException("fileSizeBytes must be > 0")
      if (partSizeBytesValue <= 0L) throw new BadRequestException("partSizeBytes must be > 0")

      val totalMaxBytes: Long = singleFileUploadMaxBytes()
      if (totalMaxBytes <= 0L) {
        throw new WebApplicationException(
          "singleFileUploadMaxBytes must be > 0",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }
      if (fileSizeBytesValue > totalMaxBytes) {
        throw new BadRequestException(
          s"fileSizeBytes=$fileSizeBytesValue exceeds singleFileUploadMaxBytes=$totalMaxBytes"
        )
      }

      val addend: Long = partSizeBytesValue - 1L
      if (addend < 0L || fileSizeBytesValue > Long.MaxValue - addend) {
        throw new WebApplicationException(
          "Overflow while computing numParts",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      val numPartsLong: Long = (fileSizeBytesValue + addend) / partSizeBytesValue
      if (numPartsLong < 1L || numPartsLong > MAXIMUM_NUM_OF_MULTIPART_S3_PARTS.toLong) {
        throw new BadRequestException(
          s"Computed numParts=$numPartsLong is out of range 1..$MAXIMUM_NUM_OF_MULTIPART_S3_PARTS"
        )
      }
      val computedNumParts: Int = numPartsLong.toInt

      if (computedNumParts > 1 && partSizeBytesValue < MINIMUM_NUM_OF_MULTIPART_S3_PART) {
        throw new BadRequestException(
          s"partSizeBytes=$partSizeBytesValue is too small. " +
            s"All non-final parts must be >= $MINIMUM_NUM_OF_MULTIPART_S3_PART bytes."
        )
      }
      var session: ModelUploadSessionRecord = null
      var rows: Result[Record2[Integer, String]] = null
      try {
        session = ctx
          .selectFrom(MODEL_UPLOAD_SESSION)
          .where(
            MODEL_UPLOAD_SESSION.UID
              .eq(uid)
              .and(MODEL_UPLOAD_SESSION.MID.eq(mid))
              .and(MODEL_UPLOAD_SESSION.FILE_PATH.eq(filePath))
          )
          .forUpdate()
          .noWait()
          .fetchOne()
        if (session != null) {
          rows = ctx
            .select(MODEL_UPLOAD_SESSION_PART.PART_NUMBER, MODEL_UPLOAD_SESSION_PART.ETAG)
            .from(MODEL_UPLOAD_SESSION_PART)
            .where(MODEL_UPLOAD_SESSION_PART.UPLOAD_ID.eq(session.getUploadId))
            .forUpdate()
            .noWait()
            .fetch()
          val dbFileSize = session.getFileSizeBytes
          val dbPartSize = session.getPartSizeBytes
          val dbNumParts = session.getNumPartsRequested
          val createdAt: OffsetDateTime = session.getCreatedAt

          val isExpired =
            createdAt
              .plusHours(PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS.toLong)
              .isBefore(OffsetDateTime.now(createdAt.getOffset))

          val conflictConfig =
            dbFileSize != fileSizeBytesValue ||
              dbPartSize != partSizeBytesValue ||
              dbNumParts != computedNumParts ||
              isExpired ||
              Option(restart).exists(_.orElse(false))

          if (conflictConfig) {
            // Parts will be deleted automatically (ON DELETE CASCADE)
            ctx
              .deleteFrom(MODEL_UPLOAD_SESSION)
              .where(MODEL_UPLOAD_SESSION.UPLOAD_ID.eq(session.getUploadId))
              .execute()

            try {
              LakeFSStorageClient.abortPresignedMultipartUploads(
                repositoryName,
                filePath,
                session.getUploadId,
                session.getPhysicalAddress
              )
            } catch { case _: Throwable => () }
            session = null
            rows = null
          }
        }
      } catch {
        case e: DataAccessException
            if Option(e.getCause)
              .collect { case s: SQLException => s.getSQLState }
              .contains("55P03") =>
          throw new WebApplicationException(
            "Another client is uploading this file",
            Response.Status.CONFLICT
          )
      }

      if (session == null) {
        val presign = withLakeFSErrorHandling {
          LakeFSStorageClient.initiatePresignedMultipartUploads(
            repositoryName,
            filePath,
            computedNumParts
          )
        }

        val uploadIdStr = presign.getUploadId
        val physicalAddr = presign.getPhysicalAddress

        try {
          val rowsInserted = ctx
            .insertInto(MODEL_UPLOAD_SESSION)
            .set(MODEL_UPLOAD_SESSION.FILE_PATH, filePath)
            .set(MODEL_UPLOAD_SESSION.MID, mid)
            .set(MODEL_UPLOAD_SESSION.UID, uid)
            .set(MODEL_UPLOAD_SESSION.UPLOAD_ID, uploadIdStr)
            .set(MODEL_UPLOAD_SESSION.PHYSICAL_ADDRESS, physicalAddr)
            .set(MODEL_UPLOAD_SESSION.NUM_PARTS_REQUESTED, Integer.valueOf(computedNumParts))
            .set(MODEL_UPLOAD_SESSION.FILE_SIZE_BYTES, java.lang.Long.valueOf(fileSizeBytesValue))
            .set(MODEL_UPLOAD_SESSION.PART_SIZE_BYTES, java.lang.Long.valueOf(partSizeBytesValue))
            .onDuplicateKeyIgnore()
            .execute()

          if (rowsInserted == 1) {
            val partNumberSeries =
              DSL.generateSeries(1, computedNumParts).asTable("gs", "partNumberField")
            val partNumberField = partNumberSeries.field("partNumberField", classOf[Integer])

            ctx
              .insertInto(
                MODEL_UPLOAD_SESSION_PART,
                MODEL_UPLOAD_SESSION_PART.UPLOAD_ID,
                MODEL_UPLOAD_SESSION_PART.PART_NUMBER,
                MODEL_UPLOAD_SESSION_PART.ETAG
              )
              .select(
                ctx
                  .select(
                    inl(uploadIdStr),
                    partNumberField,
                    inl("")
                  )
                  .from(partNumberSeries)
              )
              .execute()

            session = ctx
              .selectFrom(MODEL_UPLOAD_SESSION)
              .where(
                MODEL_UPLOAD_SESSION.UID
                  .eq(uid)
                  .and(MODEL_UPLOAD_SESSION.MID.eq(mid))
                  .and(MODEL_UPLOAD_SESSION.FILE_PATH.eq(filePath))
              )
              .fetchOne()
          } else {
            try {
              LakeFSStorageClient.abortPresignedMultipartUploads(
                repositoryName,
                filePath,
                uploadIdStr,
                physicalAddr
              )
            } catch { case _: Throwable => () }

            session = ctx
              .selectFrom(MODEL_UPLOAD_SESSION)
              .where(
                MODEL_UPLOAD_SESSION.UID
                  .eq(uid)
                  .and(MODEL_UPLOAD_SESSION.MID.eq(mid))
                  .and(MODEL_UPLOAD_SESSION.FILE_PATH.eq(filePath))
              )
              .fetchOne()
          }
        } catch {
          case e: Exception =>
            try {
              LakeFSStorageClient.abortPresignedMultipartUploads(
                repositoryName,
                filePath,
                uploadIdStr,
                physicalAddr
              )
            } catch { case _: Throwable => () }
            throw e
        }
      }

      if (session == null) {
        throw new WebApplicationException(
          "Failed to create or locate upload session",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      val uploadId = session.getUploadId
      val nParts = session.getNumPartsRequested

      if (rows == null) {
        rows =
          try {
            ctx
              .select(MODEL_UPLOAD_SESSION_PART.PART_NUMBER, MODEL_UPLOAD_SESSION_PART.ETAG)
              .from(MODEL_UPLOAD_SESSION_PART)
              .where(MODEL_UPLOAD_SESSION_PART.UPLOAD_ID.eq(uploadId))
              .forUpdate()
              .noWait()
              .fetch()
          } catch {
            case e: DataAccessException
                if Option(e.getCause)
                  .collect { case s: SQLException => s.getSQLState }
                  .contains("55P03") =>
              throw new WebApplicationException(
                "Another client is uploading parts for this file",
                Response.Status.CONFLICT
              )
          }
      }

      val missingParts = rows.asScala
        .filter(r =>
          Option(r.get(MODEL_UPLOAD_SESSION_PART.ETAG)).map(_.trim).getOrElse("").isEmpty
        )
        .map(r => r.get(MODEL_UPLOAD_SESSION_PART.PART_NUMBER).intValue())
        .toList

      val completedPartsCount = nParts - missingParts.size

      Response
        .ok(
          Map(
            "missingParts" -> missingParts.asJava,
            "completedPartsCount" -> Integer.valueOf(completedPartsCount)
          )
        )
        .build()
    }
  }

  private def finishMultipartUpload(
      mid: Integer,
      encodedFilePath: String,
      uid: Int
  ): Response = {

    val filePath = validateAndNormalizeFilePathOrThrow(
      URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
    )

    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val model = getModelByID(ctx, mid)

      val session =
        try {
          ctx
            .selectFrom(MODEL_UPLOAD_SESSION)
            .where(
              MODEL_UPLOAD_SESSION.UID
                .eq(uid)
                .and(MODEL_UPLOAD_SESSION.MID.eq(mid))
                .and(MODEL_UPLOAD_SESSION.FILE_PATH.eq(filePath))
            )
            .forUpdate()
            .noWait()
            .fetchOne()
        } catch {
          case e: DataAccessException
              if Option(e.getCause)
                .collect { case s: SQLException => s.getSQLState }
                .contains("55P03") =>
            throw new WebApplicationException(
              "Upload is already being finalized/aborted",
              Response.Status.CONFLICT
            )
        }

      if (session == null) {
        throw new NotFoundException("Upload session not found or already finalized")
      }

      val uploadId = session.getUploadId
      val expectedParts = session.getNumPartsRequested

      val physicalAddr = Option(session.getPhysicalAddress).map(_.trim).getOrElse("")
      if (physicalAddr.isEmpty) {
        throw new WebApplicationException(
          "Upload session is missing physicalAddress. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      val total = DSL.count()
      val done =
        DSL
          .count()
          .filterWhere(MODEL_UPLOAD_SESSION_PART.ETAG.ne(""))
          .as("done")

      val agg = ctx
        .select(total.as("total"), done)
        .from(MODEL_UPLOAD_SESSION_PART)
        .where(MODEL_UPLOAD_SESSION_PART.UPLOAD_ID.eq(uploadId))
        .fetchOne()

      val totalCnt = agg.get("total", classOf[java.lang.Integer]).intValue()
      val doneCnt = agg.get("done", classOf[java.lang.Integer]).intValue()

      if (totalCnt != expectedParts) {
        throw new WebApplicationException(
          s"Part table mismatch: expected $expectedParts rows but found $totalCnt. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      if (doneCnt != expectedParts) {
        val missing = ctx
          .select(MODEL_UPLOAD_SESSION_PART.PART_NUMBER)
          .from(MODEL_UPLOAD_SESSION_PART)
          .where(
            MODEL_UPLOAD_SESSION_PART.UPLOAD_ID
              .eq(uploadId)
              .and(MODEL_UPLOAD_SESSION_PART.ETAG.eq(""))
          )
          .orderBy(MODEL_UPLOAD_SESSION_PART.PART_NUMBER.asc())
          .limit(50)
          .fetch(MODEL_UPLOAD_SESSION_PART.PART_NUMBER)
          .asScala
          .toList

        throw new WebApplicationException(
          s"Upload incomplete. Some missing ETags for parts are: ${missing.mkString(",")}",
          Response.Status.CONFLICT
        )
      }

      // Build partsList in order
      val partsList: List[(Int, String)] =
        ctx
          .select(MODEL_UPLOAD_SESSION_PART.PART_NUMBER, MODEL_UPLOAD_SESSION_PART.ETAG)
          .from(MODEL_UPLOAD_SESSION_PART)
          .where(MODEL_UPLOAD_SESSION_PART.UPLOAD_ID.eq(uploadId))
          .orderBy(MODEL_UPLOAD_SESSION_PART.PART_NUMBER.asc())
          .fetch()
          .asScala
          .map(r =>
            (
              r.get(MODEL_UPLOAD_SESSION_PART.PART_NUMBER).intValue(),
              r.get(MODEL_UPLOAD_SESSION_PART.ETAG)
            )
          )
          .toList

      val objectStats = withLakeFSErrorHandling {
        LakeFSStorageClient.completePresignedMultipartUploads(
          model.getRepositoryName,
          filePath,
          uploadId,
          partsList,
          physicalAddr
        )
      }

      // FINAL SERVER-SIDE SIZE CHECK (do not rely on init)
      val actualSizeBytes =
        Option(objectStats.getSizeBytes).map(_.longValue()).getOrElse(-1L)

      if (actualSizeBytes <= 0L) {
        throw new WebApplicationException(
          "lakeFS did not return sizeBytes for completed multipart upload",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      val maxBytes = singleFileUploadMaxBytes()
      val tooLarge = actualSizeBytes > maxBytes

      if (tooLarge) {
        try {
          LakeFSStorageClient.resetObjectUploadOrDeletion(model.getRepositoryName, filePath)
        } catch {
          case _: Throwable => ()
        }
      }

      // always cleanup session
      ctx
        .deleteFrom(MODEL_UPLOAD_SESSION)
        .where(
          MODEL_UPLOAD_SESSION.UID
            .eq(uid)
            .and(MODEL_UPLOAD_SESSION.MID.eq(mid))
            .and(MODEL_UPLOAD_SESSION.FILE_PATH.eq(filePath))
        )
        .execute()

      if (tooLarge) {
        throw new WebApplicationException(
          s"Upload exceeded max size: actualSizeBytes=$actualSizeBytes maxBytes=$maxBytes",
          Response.Status.REQUEST_ENTITY_TOO_LARGE
        )
      }

      Response
        .ok(
          Map(
            "message" -> "Multipart upload completed successfully",
            "filePath" -> objectStats.getPath
          )
        )
        .build()
    }
  }

  private def abortMultipartUpload(
      mid: Integer,
      encodedFilePath: String,
      uid: Int
  ): Response = {

    val filePath = validateAndNormalizeFilePathOrThrow(
      URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
    )

    val (repoName, uploadId, physicalAddr) = withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val model = getModelByID(ctx, mid)

      val session =
        try {
          ctx
            .selectFrom(MODEL_UPLOAD_SESSION)
            .where(
              MODEL_UPLOAD_SESSION.UID
                .eq(uid)
                .and(MODEL_UPLOAD_SESSION.MID.eq(mid))
                .and(MODEL_UPLOAD_SESSION.FILE_PATH.eq(filePath))
            )
            .forUpdate()
            .noWait()
            .fetchOne()
        } catch {
          case e: DataAccessException
              if Option(e.getCause)
                .collect { case s: SQLException => s.getSQLState }
                .contains("55P03") =>
            throw new WebApplicationException(
              "Upload is already being finalized/aborted",
              Response.Status.CONFLICT
            )
        }

      if (session == null) {
        throw new NotFoundException("Upload session not found or already finalized")
      }

      val physicalAddr = Option(session.getPhysicalAddress).map(_.trim).getOrElse("")

      // Delete session; parts removed via ON DELETE CASCADE
      ctx
        .deleteFrom(MODEL_UPLOAD_SESSION)
        .where(
          MODEL_UPLOAD_SESSION.UID
            .eq(uid)
            .and(MODEL_UPLOAD_SESSION.MID.eq(mid))
            .and(MODEL_UPLOAD_SESSION.FILE_PATH.eq(filePath))
        )
        .execute()

      (model.getRepositoryName, session.getUploadId, physicalAddr)
    }

    withLakeFSErrorHandling {
      LakeFSStorageClient.abortPresignedMultipartUploads(repoName, filePath, uploadId, physicalAddr)
    }

    Response.ok(Map("message" -> "Multipart upload aborted successfully")).build()
  }
}
