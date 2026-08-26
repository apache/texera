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
import org.apache.texera.amber.core.storage.ResourceType
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.{SiteSettings, SqlServer}
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.Model.MODEL
import org.apache.texera.dao.jooq.generated.tables.ModelVersion.MODEL_VERSION
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.daos.{ModelDao, ModelUserAccessDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Model, ModelUserAccess, ModelVersion}
import org.apache.texera.service.`type`.{Diff, ExistingUploadFilesRequest, LakeFSFileNode}
import org.apache.texera.service.resource.ResourceTables.{Model => MODEL_RESOURCE}
import org.apache.texera.service.resource.ModelAccessResource._
import org.apache.texera.service.resource.ModelResource.{context, _}
import org.apache.texera.service.util.S3StorageClient
import org.apache.texera.service.util.LakeFSExceptionHandler.withLakeFSErrorHandling
import org.jooq.{DSLContext, EnumType}

import java.io.InputStream
import java.util.Optional
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

object ModelResource {

  // MVP supports a single framework; stored on the model so later frameworks can be added.
  // Callers may omit the framework; it is a display label, not a validation gate for files.
  val DEFAULT_FRAMEWORK = "pytorch"

  // Recognised values for the `framework` and `format` labels. They are metadata, not file
  // checks -- a loader dispatches on them, so an unknown value is rejected at creation
  // rather than surfacing later as an unloadable model.
  val SUPPORTED_FRAMEWORKS: Set[String] = Set("pytorch", "tensorflow", "onnx", "sklearn")

  val SUPPORTED_FORMATS: Set[String] =
    Set("torchscript", "state-dict", "safetensors", "onnx", "savedmodel", "joblib", "pickle")

  private def validateLabel(field: String, value: String, allowed: Set[String]): Unit = {
    if (!allowed.contains(value)) {
      throw new BadRequestException(
        s"Unsupported $field '$value'. Supported values: ${allowed.toList.sorted.mkString(", ")}."
      )
    }
  }

  // Matches model_version.name VARCHAR(128).
  private val MAX_VERSION_NAME_LENGTH = 128

  private val MULTIPART_OPERATIONS = Seq("list", "init", "finish", "abort")

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
  /** Scoped to `mid`: the access check runs against `mid`, so an unscoped lookup would
    * resolve another model's version through this repository.
    */
  private def getModelVersionByID(ctx: DSLContext, mid: Integer, mvid: Integer): ModelVersion = {
    val version = ctx
      .selectFrom(MODEL_VERSION)
      .where(MODEL_VERSION.MVID.eq(mvid).and(MODEL_VERSION.MID.eq(mid)))
      .fetchOneInto(classOf[ModelVersion])
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
      fileNodes: List[LakeFSFileNode]
  )

  case class ModelVersionRootFileNodesResponse(
      fileNodes: List[LakeFSFileNode],
      size: Long
  )
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/model")
class ModelResource extends LazyLogging {
  private val ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE = "User has no access to this model"
  private val ERR_MODEL_VERSION_NOT_FOUND_MESSAGE = "The version of the model not found"

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
      val framework =
        Option(request.framework).map(_.trim).filter(_.nonEmpty).getOrElse(DEFAULT_FRAMEWORK)
      validateLabel("framework", framework, SUPPORTED_FRAMEWORKS)
      val format = Option(request.format).map(_.trim).filter(_.nonEmpty)
      format.foreach(validateLabel("format", _, SUPPORTED_FORMATS))

      model.setFramework(framework)
      model.setFormat(format.orNull)

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
              size = repositorySizeOrZero(model)
            )
          ),
        fromPublic = (model, ownerEmail) =>
          Some(
            DashboardModel(
              isOwner = false,
              model = model,
              accessPrivilege = PrivilegeEnum.READ,
              ownerEmail = ownerEmail,
              size = repositorySizeOrZero(model)
            )
          )
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

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/versionZip")
  def getModelVersionZip(
      @PathParam("mid") mid: Integer,
      @QueryParam("mvid") mvid: Integer,
      @QueryParam("latest") latest: java.lang.Boolean,
      @Auth user: SessionUser
  ): Response =
    withTransaction(context) { ctx =>
      if ((mvid != null && latest != null) || (mvid == null && latest == null)) {
        throw new BadRequestException("Specify exactly one: mvid=<ID> OR latest=true")
      }

      val uid = user.getUid
      if (!userHasReadAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val model = getModelByID(ctx, mid)
      // Non-owners may download only while the owner leaves the model downloadable.
      if (!userOwnModel(ctx, mid, uid) && !model.getIsDownloadable) {
        throw new ForbiddenException("Model download is not allowed")
      }

      val modelVersion =
        if (mvid != null) getModelVersionByID(ctx, mid, mvid)
        else
          getLatestModelVersion(ctx, mid).getOrElse(
            throw new NotFoundException(ERR_MODEL_VERSION_NOT_FOUND_MESSAGE)
          )

      ResourceUploadService.versionZipResponse(
        model.getRepositoryName,
        modelVersion.getVersionHash,
        model.getName,
        modelVersion.getName
      )
    }

  /** Owner facet for the model list page. */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-model-owners")
  def retrieveOwners(@Auth user: SessionUser): java.util.List[String] =
    withTransaction(context)(ctx =>
      ResourceAccess.ownerEmailsVisibleTo(ctx, MODEL_RESOURCE, user.getUid)
    )

  // ===========================================================================
  // Staged changes
  // ===========================================================================

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/diff")
  def getModelDiff(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): List[Diff] =
    ResourceUploadService.stagedChanges(ResourceStorage.Model, mid, user.getUid)

  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/diff")
  def resetModelFileDiff(
      @PathParam("mid") mid: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response =
    ResourceUploadService.resetStagedChange(
      ResourceStorage.Model,
      mid,
      encodedFilePath,
      user.getUid
    )

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/existing-upload-files")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def findExistingUploadFiles(
      @PathParam("mid") mid: Integer,
      request: ExistingUploadFilesRequest,
      @Auth user: SessionUser
  ): Response =
    ResourceUploadService.matchExistingUploads(
      ResourceStorage.Model,
      mid,
      user.getUid,
      request,
      ctx => getLatestModelVersion(ctx, mid).map(_.getVersionHash)
    )

  // ===========================================================================
  // Presigned downloads
  // ===========================================================================

  /**
    * Resolves a presign request against the model tables and wraps the signed URL.
    * The resolution itself is shared with datasets; only the descriptor differs.
    */
  /** Size of a model's LakeFS repository, or 0 if LakeFS cannot answer. */
  private def repositorySizeOrZero(model: Model): Long = {
    try {
      LakeFSStorageClient.retrieveRepositorySize(model.getRepositoryName)
    } catch {
      case e: io.lakefs.clients.sdk.ApiException =>
        logger.error(
          s"LakeFS ApiException for model repository '${model.getRepositoryName}': ${e.getMessage}",
          e
        )
        0L
    }
  }

  private def generatePresignedResponse(
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Response =
    ResourceUploadService.presignedUrlResponse(
      ResourceStorage.Model,
      encodedUrl,
      repositoryName,
      commitHash,
      uid
    )

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download")
  def getPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response =
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, user.getUid)

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download-s3")
  def getPresignedUrlWithS3(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response =
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, user.getUid)

  @GET
  @PermitAll
  @Path("/public-presign-download")
  def getPublicPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String
  ): Response =
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, null)

  @GET
  @PermitAll
  @Path("/public-presign-download-s3")
  def getPublicPresignedUrlWithS3(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String
  ): Response =
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, null)

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

      // Before the commit: the commit is outside this transaction, so a name the insert
      // rejects would leave a commit no version points at and strand the staged file.
      if (newVersionName.length > MAX_VERSION_NAME_LENGTH) {
        throw new BadRequestException(
          s"Version name is too long: ${newVersionName.length} characters, " +
            s"maximum is $MAX_VERSION_NAME_LENGTH."
        )
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
        LakeFSFileNode
          .fromLakeFSRepositoryCommittedObjects(
            ResourceType.Model,
            Map((getOwner(ctx, mid).getEmail, modelName, newVersionName) -> fileNodes)
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
      val latestVersion = getLatestModelVersion(ctx, mid).getOrElse(
        throw new NotFoundException(ERR_MODEL_VERSION_NOT_FOUND_MESSAGE)
      )
      DashboardModelVersion(latestVersion, versionRootFileNodes(ctx, mid, latestVersion))
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
    ResourceUploadService.uploadOneFile(
      ResourceStorage.Model,
      mid,
      encodedFilePath,
      fileStream,
      headers,
      user.getUid
    )
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
    ResourceUploadService.deleteStagedFile(
      ResourceStorage.Model,
      mid,
      encodedFilePath,
      user.getUid
    )
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

    // Optional query param: null when omitted, so validate before dereferencing and before
    // the getModelBy round-trip.
    val operation = Option(operationType).map(_.trim.toLowerCase).getOrElse("")
    if (!MULTIPART_OPERATIONS.contains(operation)) {
      throw new BadRequestException(
        s"Invalid type parameter. Use ${MULTIPART_OPERATIONS.map(o => s"'$o'").mkString(", ")}."
      )
    }

    val model: Model = getModelBy(ownerEmail, modelName)

    operation match {
      case "list" => listMultipartUploads(model.getMid, uid)
      case "init" =>
        initMultipartUpload(model.getMid, filePath, fileSizeBytes, partSizeBytes, restart, uid)
      case "finish" => finishMultipartUpload(model.getMid, filePath, uid)
      case _        => abortMultipartUpload(model.getMid, filePath, uid)
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
    val model = getModelBy(modelOwnerEmail, modelName)
    ResourceUploadService.uploadPart(
      ResourceStorage.Model,
      model.getMid,
      user.getUid,
      encodedFilePath,
      partNumber,
      partStream,
      headers
    )
  }

  // ===========================================================================
  // Private helpers
  // ===========================================================================

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
    * owner/model/version nesting produced by LakeFSFileNode.
    */
  private def versionRootFileNodes(
      ctx: DSLContext,
      mid: Integer,
      modelVersion: ModelVersion
  ): List[LakeFSFileNode] = {
    val model = getModelByID(ctx, mid)
    ResourceUploadService
      .versionRootFileNodes(
        ResourceType.Model,
        getOwner(ctx, mid).getEmail,
        model.getName,
        modelVersion.getName,
        model.getRepositoryName,
        modelVersion.getVersionHash
      )
      ._1
  }

  private def fetchModelVersionRootFileNodes(
      ctx: DSLContext,
      mid: Integer,
      mvid: Integer,
      uid: Option[Integer]
  ): ModelVersionRootFileNodesResponse = {
    val model = getDashboardModel(ctx, mid, uid)
    val modelVersion = getModelVersionByID(ctx, mid, mvid)
    val (nodes, size) = ResourceUploadService.versionRootFileNodes(
      ResourceType.Model,
      model.ownerEmail,
      model.model.getName,
      modelVersion.getName,
      model.model.getRepositoryName,
      modelVersion.getVersionHash
    )
    ModelVersionRootFileNodesResponse(nodes, size)
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

  private def listMultipartUploads(mid: Integer, requesterUid: Int): Response =
    ResourceUploadService.listUploads(ResourceStorage.Model, mid, requesterUid)

  private def initMultipartUpload(
      mid: Integer,
      encodedFilePath: String,
      fileSizeBytes: Optional[java.lang.Long],
      partSizeBytes: Optional[java.lang.Long],
      restart: Optional[java.lang.Boolean],
      uid: Integer
  ): Response =
    ResourceUploadService.initUpload(
      ResourceStorage.Model,
      mid,
      encodedFilePath,
      fileSizeBytes,
      partSizeBytes,
      restart,
      uid
    )

  private def finishMultipartUpload(mid: Integer, encodedFilePath: String, uid: Int): Response =
    ResourceUploadService.finishUpload(ResourceStorage.Model, mid, encodedFilePath, uid)

  private def abortMultipartUpload(mid: Integer, encodedFilePath: String, uid: Int): Response =
    ResourceUploadService.abortUpload(ResourceStorage.Model, mid, encodedFilePath, uid)
}
