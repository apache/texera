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
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.common.util.EmailUtil
import org.apache.texera.amber.core.storage.model.OnVersionedFileResource
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.amber.core.storage.{DocumentFactory, FileResolver, ResourceType}
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SiteSettings
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.DatasetContributor.DATASET_CONTRIBUTOR
import org.apache.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.daos.{
  DatasetDao,
  DatasetUserAccessDao,
  DatasetVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  DatasetUserAccess,
  DatasetVersion
}
import org.apache.texera.service.`type`.DatasetFileNode
import org.apache.texera.service.resource.DatasetAccessResource._
import org.apache.texera.service.resource.ResourceTables.{Dataset => DATASET_RESOURCE}
import org.apache.texera.service.resource.DatasetResource.{context, _}
import org.apache.texera.service.util.S3StorageClient
import org.jooq.impl.DSL
import org.jooq.{DSLContext, EnumType}

import java.io.{InputStream, OutputStream}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.Optional
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import org.apache.commons.io.FilenameUtils
import org.apache.texera.service.util.LakeFSExceptionHandler.withLakeFSErrorHandling

object DatasetResource {

  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  // Builds a resource logical path (/<resourceType>/ownerEmail/resourceName/relativePath).
  private def logicalPath(
      resourceType: ResourceType.Value,
      ownerEmail: String,
      resourceName: String,
      relativePath: String
  ): String =
    s"$resourceType/$ownerEmail/$resourceName/$relativePath"

  private def singleFileUploadMaxBytes(defaultMiB: Long = 20L): Long =
    SiteSettings.getLong("single_file_upload_max_size_mib", defaultMiB) * 1024L * 1024L

  /**
    * Helper function to get the dataset from DB using did
    */
  private def getDatasetByID(ctx: DSLContext, did: Integer): Dataset = {
    val datasetDao = new DatasetDao(ctx.configuration())
    val dataset = datasetDao.fetchOneByDid(did)
    if (dataset == null) {
      throw new NotFoundException(f"Dataset $did not found")
    }
    dataset
  }

  /**
    * Helper function to get the dataset version from DB using dvid
    */
  private def getDatasetVersionByID(
      ctx: DSLContext,
      dvid: Integer
  ): DatasetVersion = {
    val datasetVersionDao = new DatasetVersionDao(ctx.configuration())
    val version = datasetVersionDao.fetchOneByDvid(dvid)
    if (version == null) {
      throw new NotFoundException("Dataset Version not found")
    }
    version
  }

  /**
    * Helper function to get the latest dataset version from the DB
    */
  private def getLatestDatasetVersion(
      ctx: DSLContext,
      did: Integer
  ): Option[DatasetVersion] = {
    ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .orderBy(DATASET_VERSION.CREATION_TIME.desc())
      .limit(1)
      .fetchOptionalInto(classOf[DatasetVersion])
      .toScala
  }

  /**
    * Helper function to get the contributors using the did
    */
  def getContributorsByDid(ctx: DSLContext, did: Integer): List[Contributor] = {
    ctx
      .selectFrom(DATASET_CONTRIBUTOR)
      .where(DATASET_CONTRIBUTOR.DID.eq(did))
      .fetch()
      .asScala
      .toList
      .map { record =>
        Contributor(
          name = record.getName,
          creator = record.getCreator,
          affiliation = record.getAffiliation,
          email = record.getEmail,
          comments = record.getComments,
          uid = Option(record.getUid)
        )
      }
  }

  /**
    * Resolves a normalized contributor email to a user account, creating a
    * placeholder account when no user with that email exists.
    */
  private def resolveContributorUid(
      ctx: DSLContext,
      did: Integer,
      name: String,
      normalizedEmail: String
  ): Integer = {
    val existing = ctx
      .select(USER.UID)
      .from(USER)
      .where(DSL.lower(USER.EMAIL).eq(normalizedEmail))
      .fetchOne(USER.UID)
    if (existing != null) {
      existing
    } else {
      val placeholder = ctx.newRecord(USER)
      placeholder.setName(name)
      placeholder.setEmail(normalizedEmail)
      placeholder.setRole(UserRoleEnum.INACTIVE)
      placeholder.setIsPlaceholder(true)
      placeholder.setComment(s"Auto-created as contributor of dataset $did")
      placeholder.store()
      placeholder.getUid
    }
  }

  /**
    * Helper function to insert the contributors of a dataset in one batch
    */
  private def contributorEmail(contributor: Contributor): Option[String] =
    Option(contributor.email).map(EmailUtil.normalize).filter(_.nonEmpty)

  def insertContributors(ctx: DSLContext, did: Integer, contributors: List[Contributor]): Unit = {
    contributors.foreach { contributor =>
      if (contributor == null || contributor.name == null || contributor.name.trim.isEmpty) {
        throw new BadRequestException("Each contributor must have a name")
      }
      if (
        contributor.name.length > 256 ||
        Option(contributor.email).exists(_.length > 256) ||
        Option(contributor.affiliation).exists(_.length > 256)
      ) {
        throw new BadRequestException("Contributor fields must not exceed 256 characters")
      }
      contributorEmail(contributor).foreach { email =>
        if (!EmailUtil.isValid(email)) {
          throw new BadRequestException(s"Invalid contributor email: ${contributor.email}")
        }
      }
    }

    val emails = contributors.flatMap(contributorEmail)
    if (emails.distinct.size != emails.size) {
      throw new BadRequestException("Each contributor of a dataset must have a distinct email")
    }

    val records = contributors.map { contributor =>
      val record = ctx.newRecord(DATASET_CONTRIBUTOR)
      record.setDid(did)
      record.setName(contributor.name)
      record.setCreator(contributor.creator)
      record.setAffiliation(contributor.affiliation)
      record.setEmail(contributor.email)
      record.setComments(contributor.comments)
      contributorEmail(contributor).foreach(email =>
        record.setUid(resolveContributorUid(ctx, did, contributor.name, email))
      )
      record
    }
    ctx.batchInsert(records.asJava).execute()
  }

  case class Contributor(
      name: String,
      creator: Boolean = false,
      affiliation: String = null,
      email: String = null,
      comments: String = null,
      uid: Option[Integer] = None
  )

  case class DatasetContributorsModification(
      did: Integer,
      contributors: Option[List[Contributor]] = None
  )

  case class DashboardDataset(
      dataset: Dataset,
      ownerEmail: String,
      accessPrivilege: EnumType,
      isOwner: Boolean,
      size: Long,
      contributors: List[Contributor] = Nil
  )

  case class DashboardDatasetVersion(
      datasetVersion: DatasetVersion,
      fileNodes: List[DatasetFileNode]
  )

  case class CreateDatasetRequest(
      datasetName: String,
      datasetDescription: String,
      isDatasetPublic: Boolean,
      isDatasetDownloadable: Boolean,
      contributors: Option[List[Contributor]] = None
  )

  // Shared with the other versioned resources; aliased so callers keep using
  // DatasetResource.Diff / .ExistingUploadFile.
  type Diff = org.apache.texera.service.`type`.Diff
  val Diff: org.apache.texera.service.`type`.Diff.type = org.apache.texera.service.`type`.Diff
  type ExistingUploadFile = org.apache.texera.service.`type`.ExistingUploadFile
  val ExistingUploadFile: org.apache.texera.service.`type`.ExistingUploadFile.type = org.apache.texera.service.`type`.ExistingUploadFile
  type ExistingUploadFilesRequest = org.apache.texera.service.`type`.ExistingUploadFilesRequest
  val ExistingUploadFilesRequest: org.apache.texera.service.`type`.ExistingUploadFilesRequest.type =
    org.apache.texera.service.`type`.ExistingUploadFilesRequest

  case class DatasetDescriptionModification(did: Integer, description: String)

  case class DatasetNameModification(did: Integer, name: String)

  case class DatasetVersionRootFileNodesResponse(
      fileNodes: List[DatasetFileNode],
      size: Long
  )

  case class CoverImageRequest(coverImage: String)
}

@Produces(Array(MediaType.APPLICATION_JSON, "image/jpeg", "application/pdf"))
@Path("/dataset")
class DatasetResource extends LazyLogging {
  private val ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE = "User has no access to this dataset"
  private val ERR_DATASET_VERSION_NOT_FOUND_MESSAGE = "The version of the dataset not found"
  private val EXPIRATION_MINUTES = 5

  private val COVER_IMAGE_SIZE_LIMIT_BYTES: Long = 10 * 1024 * 1024 // 10 MB
  private val ALLOWED_IMAGE_EXTENSIONS: Set[String] = Set(".jpg", ".jpeg", ".png", ".gif", ".webp")

  private val resourceType = ResourceType.Dataset

  /**
    * Helper function to get the dataset from DB with additional information including user access privilege and owner email
    */
  private def getDashboardDataset(
      ctx: DSLContext,
      did: Integer,
      requesterUid: Option[Integer]
  ): DashboardDataset = {
    val targetDataset = getDatasetByID(ctx, did)

    if (requesterUid.isEmpty && !targetDataset.getIsPublic) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    } else if (requesterUid.exists(uid => !userHasReadAccess(ctx, did, uid))) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }

    val userAccessPrivilege = requesterUid
      .map(uid => getDatasetUserAccessPrivilege(ctx, did, uid))
      .getOrElse(PrivilegeEnum.READ)

    val isOwner = requesterUid.contains(targetDataset.getOwnerUid)

    DashboardDataset(
      targetDataset,
      getOwner(ctx, did).getEmail,
      userAccessPrivilege,
      isOwner,
      withLakeFSErrorHandling(s"retrieving the size of dataset '${targetDataset.getName}'") {
        LakeFSStorageClient.retrieveRepositorySize(targetDataset.getRepositoryName)
      },
      contributors = DatasetResource.getContributorsByDid(ctx, did)
    )
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/create")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def createDataset(
      request: CreateDatasetRequest,
      @Auth user: SessionUser
  ): DashboardDataset = {

    withTransaction(context) { ctx =>
      val uid = user.getUid
      val datasetUserAccessDao: DatasetUserAccessDao = new DatasetUserAccessDao(ctx.configuration())

      val datasetName = request.datasetName
      val datasetDescription = request.datasetDescription
      val isDatasetPublic = request.isDatasetPublic
      val isDatasetDownloadable = request.isDatasetDownloadable

      validateDatasetName(datasetName)
      ResourceNaming.requireNameAvailable(ctx, DATASET_RESOURCE, uid, datasetName)

      // insert the dataset into the database
      val dataset = new Dataset()
      dataset.setName(datasetName)
      dataset.setDescription(datasetDescription)
      dataset.setIsPublic(isDatasetPublic)
      dataset.setIsDownloadable(isDatasetDownloadable)
      dataset.setOwnerUid(uid)

      // insert record and get created dataset with did
      val createdDataset = failOnDuplicateDatasetName {
        ctx
          .insertInto(DATASET)
          .set(ctx.newRecord(DATASET, dataset))
          .returning()
          .fetchOne()
      }

      // Initialize the repository in LakeFS
      val repositoryName = s"dataset-${createdDataset.getDid}"
      try {
        withLakeFSErrorHandling(s"creating the repository of dataset '${dataset.getName}'") {
          LakeFSStorageClient.initRepo(repositoryName)
        }
      } catch {
        case e: Exception =>
          // roll back the dataset record so a failed LakeFS init leaves no orphan row
          ctx
            .deleteFrom(DATASET)
            .where(DATASET.DID.eq(createdDataset.getDid))
            .execute()
          e match {
            case web: WebApplicationException => throw web
            case other =>
              throw new WebApplicationException(
                s"Failed to create the dataset: ${other.getMessage}"
              )
          }
      }

      // After the LakeFS call so placeholder inserts don't hold user-table locks across it.
      val savedContributors = request.contributors.getOrElse(Nil)
      DatasetResource.insertContributors(ctx, createdDataset.getDid, savedContributors)

      // update repository name of the created dataset
      createdDataset.setRepositoryName(repositoryName)
      createdDataset.update()

      // Insert the requester as the WRITE access user for this dataset
      val datasetUserAccess = new DatasetUserAccess()
      datasetUserAccess.setDid(createdDataset.getDid)
      datasetUserAccess.setUid(uid)
      datasetUserAccess.setPrivilege(PrivilegeEnum.WRITE)
      datasetUserAccessDao.insert(datasetUserAccess)

      DashboardDataset(
        createdDataset.into(classOf[Dataset]),
        user.getEmail,
        PrivilegeEnum.WRITE,
        isOwner = true,
        0,
        savedContributors
      )
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/create")
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def createDatasetVersion(
      versionName: String,
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): DashboardDatasetVersion = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val dataset = getDatasetByID(ctx, did)
      val datasetName = dataset.getName
      val repositoryName = dataset.getRepositoryName

      // Check if there are any changes in LakeFS before creating a new version
      val diffs = withLakeFSErrorHandling {
        LakeFSStorageClient.retrieveUncommittedObjects(repoName = repositoryName)
      }

      if (diffs.isEmpty) {
        throw new WebApplicationException(
          "No changes detected in dataset. Version creation aborted.",
          Response.Status.BAD_REQUEST
        )
      }

      // Generate a new version name
      val versionCount = ctx
        .selectCount()
        .from(DATASET_VERSION)
        .where(DATASET_VERSION.DID.eq(did))
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
          commitMessage = s"Created dataset version: $newVersionName"
        )
      }

      if (commit == null || commit.getId == null) {
        throw new WebApplicationException(
          "Failed to create commit in LakeFS. Version creation aborted.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      // Create a new dataset version entry in the database
      val datasetVersion = new DatasetVersion()
      datasetVersion.setDid(did)
      datasetVersion.setCreatorUid(uid)
      datasetVersion.setName(newVersionName)
      datasetVersion.setVersionHash(commit.getId) // Store LakeFS version hash

      val insertedVersion = ctx
        .insertInto(DATASET_VERSION)
        .set(ctx.newRecord(DATASET_VERSION, datasetVersion))
        .returning()
        .fetchOne()
        .into(classOf[DatasetVersion])

      // Retrieve committed file structure
      val fileNodes = withLakeFSErrorHandling {
        LakeFSStorageClient.retrieveObjectsOfVersion(repositoryName, commit.getId)
      }

      DashboardDatasetVersion(
        insertedVersion,
        DatasetFileNode
          .fromLakeFSRepositoryCommittedObjects(
            resourceType,
            Map((user.getEmail, datasetName, newVersionName) -> fileNodes)
          )
      )
    }
  }

  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}")
  def deleteDataset(@PathParam("did") did: Integer, @Auth user: SessionUser): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val dataset = getDatasetByID(ctx, did)
      if (!userOwnDataset(ctx, dataset.getDid, uid)) {
        // throw the exception that user has no access to certain dataset
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      withLakeFSErrorHandling(s"deleting the repository of dataset '${dataset.getName}'") {
        LakeFSStorageClient.deleteRepo(dataset.getRepositoryName)
      }
      // delete the directory on S3
      if (
        S3StorageClient.directoryExists(StorageConfig.lakefsBucketName, dataset.getRepositoryName)
      ) {
        S3StorageClient.deleteDirectory(StorageConfig.lakefsBucketName, dataset.getRepositoryName)
      }

      // delete the dataset from the DB
      datasetDao.deleteById(dataset.getDid)

      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/description")
  def updateDatasetDescription(
      modificator: DatasetDescriptionModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val uid = sessionUser.getUid
      val datasetDao = new DatasetDao(ctx.configuration())
      val dataset = getDatasetByID(ctx, modificator.did)
      if (!userHasWriteAccess(ctx, modificator.did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      dataset.setDescription(modificator.description)
      datasetDao.update(dataset)
      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/contributors")
  def updateDatasetContributors(
      modificator: DatasetContributorsModification,
      @Auth user: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, modificator.did, user.getUid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      ctx
        .delete(DATASET_CONTRIBUTOR)
        .where(DATASET_CONTRIBUTOR.DID.eq(modificator.did))
        .execute()
      DatasetResource.insertContributors(
        ctx,
        modificator.did,
        modificator.contributors.getOrElse(Nil)
      )

      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/name")
  def updateDatasetName(
      modificator: DatasetNameModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val uid = sessionUser.getUid
      val datasetDao = new DatasetDao(ctx.configuration())
      val dataset = getDatasetByID(ctx, modificator.did)
      if (!userHasWriteAccess(ctx, modificator.did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      validateDatasetName(modificator.name)
      ResourceNaming.requireNameAvailable(
        ctx,
        DATASET_RESOURCE,
        dataset.getOwnerUid,
        modificator.name,
        excludingId = Some(dataset.getDid)
      )

      dataset.setName(modificator.name)
      failOnDuplicateDatasetName {
        datasetDao.update(dataset)
      }
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/upload")
  @Consumes(Array(MediaType.APPLICATION_OCTET_STREAM))
  def uploadOneFileToDataset(
      @PathParam("did") did: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @QueryParam("message") message: String,
      fileStream: InputStream,
      @Context headers: HttpHeaders,
      @Auth user: SessionUser
  ): Response = {
    ResourceUploadService.uploadOneFile(
      ResourceStorage.Dataset,
      did,
      encodedFilePath,
      fileStream,
      headers,
      user.getUid
    )
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download")
  def getPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, uid)
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
    val uid = user.getUid
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, uid)
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

  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/file")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def deleteDatasetFile(
      @PathParam("did") did: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response = {
    ResourceUploadService.deleteStagedFile(
      ResourceStorage.Dataset,
      did,
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
      @QueryParam("datasetName") datasetName: String,
      @QueryParam("filePath") filePath: String,
      @QueryParam("fileSizeBytes") fileSizeBytes: Optional[java.lang.Long],
      @QueryParam("partSizeBytes") partSizeBytes: Optional[java.lang.Long],
      @QueryParam("restart") restart: Optional[java.lang.Boolean],
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    val dataset: Dataset = getDatasetBy(ownerEmail, datasetName)

    operationType.toLowerCase match {
      case "list" => listMultipartUploads(dataset.getDid, uid)
      case "init" =>
        initMultipartUpload(dataset.getDid, filePath, fileSizeBytes, partSizeBytes, restart, uid)
      case "finish" => finishMultipartUpload(dataset.getDid, filePath, uid)
      case "abort"  => abortMultipartUpload(dataset.getDid, filePath, uid)
      case _ =>
        throw new BadRequestException("Invalid type parameter. Use 'init', 'finish', or 'abort'.")
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_OCTET_STREAM))
  @Path("/multipart-upload/part")
  def uploadPart(
      @QueryParam("ownerEmail") datasetOwnerEmail: String,
      @QueryParam("datasetName") datasetName: String,
      @QueryParam("filePath") encodedFilePath: String,
      @QueryParam("partNumber") partNumber: Int,
      partStream: InputStream,
      @Context headers: HttpHeaders,
      @Auth user: SessionUser
  ): Response = {
    val dataset = getDatasetBy(datasetOwnerEmail, datasetName)
    ResourceUploadService.uploadPart(
      ResourceStorage.Dataset,
      dataset.getDid,
      user.getUid,
      encodedFilePath,
      partNumber,
      partStream,
      headers
    )
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/update/publicity")
  def toggleDatasetPublicity(
      @PathParam("did") did: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val existedDataset = getDatasetByID(ctx, did)
      val newPublicStatus = !existedDataset.getIsPublic
      existedDataset.setIsPublic(newPublicStatus)

      datasetDao.update(existedDataset)
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/update/downloadable")
  def toggleDatasetDownloadable(
      @PathParam("did") did: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userOwnDataset(ctx, did, uid)) {
        throw new ForbiddenException("Only dataset owners can modify download permissions")
      }

      val existedDataset = getDatasetByID(ctx, did)
      val newDownloadableStatus = !existedDataset.getIsDownloadable

      existedDataset.setIsDownloadable(newDownloadableStatus)

      datasetDao.update(existedDataset)
      Response.ok().build()
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/diff")
  def getDatasetDiff(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): List[Diff] = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      // Retrieve staged (uncommitted) changes from LakeFS
      val dataset = getDatasetByID(ctx, did)
      val lakefsDiffs = withLakeFSErrorHandling {
        LakeFSStorageClient.retrieveUncommittedObjects(dataset.getRepositoryName)
      }

      // Convert LakeFS Diff objects to our custom Diff case class
      lakefsDiffs.map(d =>
        new Diff(
          d.getPath,
          d.getPathType.getValue,
          d.getType.getValue,
          Option(d.getSizeBytes).map(_.longValue())
        )
      )
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/existing-upload-files")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def findExistingUploadFiles(
      @PathParam("did") did: Integer,
      request: ExistingUploadFilesRequest,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val requested = Option(request)
        .flatMap(request => Option(request.files))
        .getOrElse(List.empty)
        .map { file =>
          val originalPath = file.path
          val path = ResourceNaming.validateAndNormalizeFilePathOrThrow(originalPath)
          if (file.sizeBytes < 0L) throw new BadRequestException("sizeBytes must be >= 0")
          (path, originalPath, file.sizeBytes)
        }

      val dataset = getDatasetByID(ctx, did)
      val committed = getLatestDatasetVersion(ctx, did)
        .map { v =>
          withLakeFSErrorHandling(
            s"retrieving committed files of dataset '${dataset.getName}'"
          ) {
            LakeFSStorageClient
              .retrieveObjectsOfVersion(dataset.getRepositoryName, v.getVersionHash)
              .map(obj => obj.getPath -> obj.getSizeBytes.longValue())
          }
        }
        .getOrElse(List.empty)

      val staged = withLakeFSErrorHandling(
        s"retrieving staged files of dataset '${dataset.getName}'"
      ) {
        LakeFSStorageClient.retrieveUncommittedObjects(dataset.getRepositoryName)
      }
        .filterNot(diff => Option(diff.getType).exists(_.getValue.equalsIgnoreCase("removed")))
        .flatMap(diff => Option(diff.getSizeBytes).map(size => diff.getPath -> size.longValue()))

      val existing = (committed ++ staged).toMap
      val matches = requested
        .collect {
          case (path, originalPath, size) if existing.get(path).contains(size) => originalPath
        }
        .toList
        .distinct
        .sorted

      Response.ok(Map("filePaths" -> matches.asJava)).build()
    }
  }

  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/diff")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def resetDatasetFileDiff(
      @PathParam("did") did: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      val repositoryName = getDatasetByID(ctx, did).getRepositoryName

      // Decode the file path
      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      withLakeFSErrorHandling(s"resetting uncommitted changes of file '$filePath'") {
        LakeFSStorageClient.resetObjectUploadOrDeletion(repositoryName, filePath)
      }
      Response.ok().build()
    }
  }

  /**
    * This method returns a list of DashboardDatasets objects that are accessible by current user.
    *
    * @param user the session user
    * @return list of user accessible DashboardDataset objects
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/list")
  def listDatasets(
      @Auth user: SessionUser
  ): List[DashboardDataset] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      ResourceAccess.listVisible(
        ctx,
        DATASET_RESOURCE,
        uid,
        classOf[Dataset],
        (dataset: Dataset) => dataset.getDid
      )(
        fromGrant = (dataset, ownerEmail, privilege, isOwner) =>
          Some(
            DashboardDataset(
              isOwner = isOwner,
              dataset = dataset,
              accessPrivilege = privilege,
              ownerEmail = ownerEmail,
              size = 0
            )
          ),
        fromPublic = (dataset, ownerEmail) =>
          try {
            Some(
              DashboardDataset(
                isOwner = false,
                dataset = dataset,
                accessPrivilege = PrivilegeEnum.READ,
                ownerEmail = ownerEmail,
                size = LakeFSStorageClient.retrieveRepositorySize(dataset.getRepositoryName)
              )
            )
          } catch {
            case e: io.lakefs.clients.sdk.ApiException =>
              logger.error(
                s"LakeFS ApiException for dataset repository '${dataset.getRepositoryName}': ${e.getMessage}",
                e
              )
              None
          }
      )
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/list")
  def getDatasetVersionList(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): List[DatasetVersion] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val dataset = getDatasetByID(ctx, did)
      if (!userHasReadAccess(ctx, dataset.getDid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      fetchDatasetVersions(ctx, dataset.getDid)
    })
  }

  @GET
  @PermitAll
  @Path("/{name}/publicVersion/list")
  def getPublicDatasetVersionList(
      @PathParam("name") did: Integer
  ): List[DatasetVersion] = {
    withTransaction(context)(ctx => {
      if (!isDatasetPublic(ctx, did)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      fetchDatasetVersions(ctx, did)
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/latest")
  def retrieveLatestDatasetVersion(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): DashboardDatasetVersion = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      val dataset = getDatasetByID(ctx, did)
      val latestVersion = getLatestDatasetVersion(ctx, did).getOrElse(
        throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
      )

      val datasetsNode = DatasetFileNode
        .fromLakeFSRepositoryCommittedObjects(
          resourceType,
          Map(
            (
              getOwner(ctx, did).getEmail,
              dataset.getName,
              latestVersion.getName
            ) -> LakeFSStorageClient
              .retrieveObjectsOfVersion(dataset.getRepositoryName, latestVersion.getVersionHash)
          )
        )
        .head

      val ownerNode = datasetsNode.getChildren.headOption.getOrElse(
        throw new IllegalStateException(
          s"Dataset file tree for ${dataset.getName} is missing its owner node"
        )
      )

      DashboardDatasetVersion(
        latestVersion,
        ownerNode.children.get
          .find(_.getName == dataset.getName)
          .head
          .children
          .get
          .find(_.getName == latestVersion.getName)
          .head
          .children
          .get
      )
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/versionZip")
  def getDatasetVersionZip(
      @PathParam("did") did: Integer,
      @QueryParam("dvid") dvid: Integer, // Dataset version ID, nullable
      @QueryParam("latest") latest: java.lang.Boolean, // Flag to get latest version, nullable
      @Auth user: SessionUser
  ): Response = {

    withTransaction(context) { ctx =>
      if ((dvid != null && latest != null) || (dvid == null && latest == null)) {
        throw new BadRequestException("Specify exactly one: dvid=<ID> OR latest=true")
      }

      // Check read access and download permission
      val uid = user.getUid
      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      // Retrieve dataset and check download permission
      val dataset = getDatasetByID(ctx, did)
      // Non-owners can download if dataset is downloadable and they have read access
      if (!userOwnDataset(ctx, did, uid) && !dataset.getIsDownloadable) {
        throw new ForbiddenException("Dataset download is not allowed")
      }

      // Determine which version to retrieve
      val datasetVersion = if (dvid != null) {
        getDatasetVersionByID(ctx, dvid)
      } else if (java.lang.Boolean.TRUE.equals(latest)) {
        getLatestDatasetVersion(ctx, did).getOrElse(
          throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
        )
      } else {
        throw new BadRequestException("Invalid parameters")
      }

      // Retrieve dataset and version details
      val datasetName = dataset.getName
      val repositoryName = dataset.getRepositoryName
      val versionHash = datasetVersion.getVersionHash
      val objects = withLakeFSErrorHandling(
        s"listing files of version '$versionHash' of dataset '$datasetName'"
      ) {
        LakeFSStorageClient.retrieveObjectsOfVersion(repositoryName, versionHash)
      }

      if (objects.isEmpty) {
        return Response
          .status(Response.Status.NOT_FOUND)
          .entity(s"No objects found in version $versionHash of repository $repositoryName")
          .build()
      }

      // StreamingOutput for ZIP download
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

      val zipFilename = s"""attachment; filename="$datasetName-${datasetVersion.getName}.zip""""

      Response
        .ok(streamingOutput, "application/zip")
        .header("Content-Disposition", zipFilename)
        .build()
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/{dvid}/rootFileNodes")
  def retrieveDatasetVersionRootFileNodes(
      @PathParam("did") did: Integer,
      @PathParam("dvid") dvid: Integer,
      @Auth user: SessionUser
  ): DatasetVersionRootFileNodesResponse = {
    val uid = user.getUid
    withTransaction(context)(ctx => fetchDatasetVersionRootFileNodes(ctx, did, dvid, Some(uid)))
  }

  @GET
  @PermitAll
  @Path("/{did}/publicVersion/{dvid}/rootFileNodes")
  def retrievePublicDatasetVersionRootFileNodes(
      @PathParam("did") did: Integer,
      @PathParam("dvid") dvid: Integer
  ): DatasetVersionRootFileNodesResponse = {
    withTransaction(context)(ctx => fetchDatasetVersionRootFileNodes(ctx, did, dvid, None))
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}")
  def getDataset(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): DashboardDataset = {
    val uid = user.getUid
    withTransaction(context)(ctx => getDashboardDataset(ctx, did, Some(uid)))
  }

  @GET
  @PermitAll
  @Path("/public/{did}")
  def getPublicDataset(
      @PathParam("did") did: Integer
  ): DashboardDataset = {
    withTransaction(context)(ctx => getDashboardDataset(ctx, did, None))
  }

  /**
    * This method returns all owner user names of the dataset that the user has access to
    *
    * @return OwnerName[]
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-dataset-owners")
  def retrieveOwners(@Auth user: SessionUser): util.List[String] = {
    context
      .selectDistinct(USER.EMAIL)
      .from(USER)
      .join(DATASET)
      .on(DATASET.OWNER_UID.eq(USER.UID))
      .join(DATASET_USER_ACCESS)
      .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
      .where(DATASET_USER_ACCESS.UID.eq(user.getUid))
      .fetchInto(classOf[String])
  }

  /** @see [[ResourceNaming.validateName]] */
  private def validateDatasetName(name: String): Unit =
    ResourceNaming.validateName(DATASET_RESOURCE.label, name)

  /** @see [[ResourceNaming.failOnDuplicateName]] */
  private[resource] def failOnDuplicateDatasetName[T](op: => T): T =
    ResourceNaming.failOnDuplicateName(DATASET_RESOURCE.label)(op)

  private def fetchDatasetVersions(ctx: DSLContext, did: Integer): List[DatasetVersion] = {
    ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .orderBy(DATASET_VERSION.CREATION_TIME.desc()) // Change to .asc() for ascending order
      .fetchInto(classOf[DatasetVersion])
      .asScala
      .toList
  }

  private def fetchDatasetVersionRootFileNodes(
      ctx: DSLContext,
      did: Integer,
      dvid: Integer,
      uid: Option[Integer]
  ): DatasetVersionRootFileNodesResponse = {
    val dataset = getDashboardDataset(ctx, did, uid)
    val datasetVersion = getDatasetVersionByID(ctx, dvid)
    val datasetName = dataset.dataset.getName
    val repositoryName = dataset.dataset.getRepositoryName

    val datasetsNode = DatasetFileNode
      .fromLakeFSRepositoryCommittedObjects(
        resourceType,
        Map(
          (dataset.ownerEmail, datasetName, datasetVersion.getName) -> LakeFSStorageClient
            .retrieveObjectsOfVersion(repositoryName, datasetVersion.getVersionHash)
        )
      )
      .head

    val ownerFileNode = datasetsNode.getChildren.headOption.getOrElse(
      throw new IllegalStateException(
        s"Dataset file tree for $datasetName is missing its owner node"
      )
    )

    DatasetVersionRootFileNodesResponse(
      ownerFileNode.children.get
        .find(_.getName == datasetName)
        .head
        .children
        .get
        .find(_.getName == datasetVersion.getName)
        .head
        .children
        .get,
      DatasetFileNode.calculateTotalSize(List(datasetsNode))
    )
  }

  private def generatePresignedResponse(
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Response = {
    resolveDatasetAndPath(encodedUrl, repositoryName, commitHash, uid) match {
      case Left(errorResponse) =>
        errorResponse

      case Right((resolvedRepositoryName, resolvedCommitHash, resolvedFilePath)) =>
        val url = withLakeFSErrorHandling(
          s"generating a presigned URL for file '$resolvedFilePath'"
        ) {
          LakeFSStorageClient.getFilePresignedUrl(
            resolvedRepositoryName,
            resolvedCommitHash,
            resolvedFilePath
          )
        }

        Response.ok(Map("presignedUrl" -> url)).build()
    }
  }

  private def resolveDatasetAndPath(
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Either[Response, (String, String, String)] = {
    val decodedPathStr = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name())

    (Option(repositoryName), Option(commitHash)) match {
      case (Some(_), None) | (None, Some(_)) =>
        // Case 1: Only one parameter is provided (error case)
        Left(
          Response
            .status(Response.Status.BAD_REQUEST)
            .entity(
              "Both repositoryName and commitHash must be provided together, or neither should be provided."
            )
            .build()
        )

      case (Some(repositoryName), Some(commit)) =>
        // Case 2: repositoryName and commitHash are provided, validate access
        val response = withTransaction(context) { ctx =>
          val datasetDao = new DatasetDao(ctx.configuration())
          val datasets = datasetDao.fetchByRepositoryName(repositoryName).asScala.toList

          if (datasets.isEmpty || !userHasReadAccess(ctx, datasets.head.getDid, uid))
            throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)

          val dataset = datasets.head
          // Standard read access check only - download restrictions handled per endpoint
          // Non-download operations (viewing) should work for all public datasets

          (repositoryName, commit, decodedPathStr)
        }
        Right(response)

      case (None, None) =>
        // Case 3: Neither repositoryName nor commitHash are provided, resolve normally
        val response = withTransaction(context) { ctx =>
          val fileUri = FileResolver.resolve(decodedPathStr)
          val document =
            DocumentFactory.openReadonlyDocument(fileUri).asInstanceOf[OnVersionedFileResource]
          val datasetDao = new DatasetDao(ctx.configuration())
          val datasets =
            datasetDao.fetchByRepositoryName(document.getRepositoryName()).asScala.toList

          if (datasets.isEmpty || !userHasReadAccess(ctx, datasets.head.getDid, uid))
            throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)

          val dataset = datasets.head
          // Standard read access check only - download restrictions handled per endpoint
          // Non-download operations (viewing) should work for all public datasets

          (
            document.getRepositoryName(),
            document.getVersionHash(),
            document.getFileRelativePath()
          )
        }
        Right(response)
    }
  }

  // === Multipart helpers ===

  private def getDatasetBy(ownerEmail: String, datasetName: String) = {
    val dataset = context
      .select(DATASET.fields: _*)
      .from(DATASET)
      .leftJoin(USER)
      .on(USER.UID.eq(DATASET.OWNER_UID))
      .where(USER.EMAIL.eq(ownerEmail))
      .and(DATASET.NAME.eq(datasetName))
      .fetchOneInto(classOf[Dataset])
    if (dataset == null) {
      throw new BadRequestException("Dataset not found")
    }
    dataset
  }

  private def listMultipartUploads(did: Integer, requesterUid: Int): Response =
    ResourceUploadService.listUploads(ResourceStorage.Dataset, did, requesterUid)

  private def initMultipartUpload(
      did: Integer,
      encodedFilePath: String,
      fileSizeBytes: Optional[java.lang.Long],
      partSizeBytes: Optional[java.lang.Long],
      restart: Optional[java.lang.Boolean],
      uid: Integer
  ): Response =
    ResourceUploadService.initUpload(
      ResourceStorage.Dataset,
      did,
      encodedFilePath,
      fileSizeBytes,
      partSizeBytes,
      restart,
      uid
    )

  private def finishMultipartUpload(did: Integer, encodedFilePath: String, uid: Int): Response =
    ResourceUploadService.finishUpload(ResourceStorage.Dataset, did, encodedFilePath, uid)

  private def abortMultipartUpload(did: Integer, encodedFilePath: String, uid: Int): Response =
    ResourceUploadService.abortUpload(ResourceStorage.Dataset, did, encodedFilePath, uid)

  /**
    * Updates the cover image for a dataset.
    *
    * @param did Dataset ID
    * @param request Cover image request containing the relative file path
    * @param sessionUser Authenticated user session
    * @return Response with updated cover image path
    *
    * Expected coverImage format: "version/folder/image.jpg" (relative to dataset root)
    */
  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/update/cover")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def updateDatasetCoverImage(
      @PathParam("did") did: Integer,
      request: CoverImageRequest,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val uid = sessionUser.getUid
      val dataset = getDatasetByID(ctx, did)
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      if (request.coverImage == null || request.coverImage.trim.isEmpty) {
        throw new BadRequestException("Cover image path is required")
      }

      val normalized = ResourceNaming.validateAndNormalizeFilePathOrThrow(request.coverImage)

      val extension = FilenameUtils.getExtension(normalized)
      if (extension == null || !ALLOWED_IMAGE_EXTENSIONS.contains(s".$extension".toLowerCase)) {
        throw new BadRequestException("Invalid file type")
      }

      val owner = getOwner(ctx, did)
      val document = DocumentFactory
        .openReadonlyDocument(
          FileResolver.resolve(
            logicalPath(resourceType, owner.getEmail, dataset.getName, normalized)
          )
        )
        .asInstanceOf[OnVersionedFileResource]

      val fileSize = withLakeFSErrorHandling(s"reading the size of cover image '$normalized'") {
        LakeFSStorageClient.getFileSize(
          document.getRepositoryName(),
          document.getVersionHash(),
          document.getFileRelativePath()
        )
      }

      if (fileSize > COVER_IMAGE_SIZE_LIMIT_BYTES) {
        throw new BadRequestException(
          s"Cover image must be less than ${COVER_IMAGE_SIZE_LIMIT_BYTES / (1024 * 1024)} MB"
        )
      }

      dataset.setCoverImage(normalized)
      new DatasetDao(ctx.configuration()).update(dataset)
      Response.ok(Map("coverImage" -> normalized)).build()
    }
  }

  /**
    * Get the cover image for a dataset.
    * Returns a 307 redirect to the presigned S3 URL.
    *
    * @param did Dataset ID
    * @return 307 Temporary Redirect to cover image
    */
  @GET
  @PermitAll
  @Path("/{did}/cover")
  def getDatasetCover(
      @PathParam("did") did: Integer,
      @Auth sessionUser: Optional[SessionUser]
  ): Response = {
    withTransaction(context) { ctx =>
      val dataset = getDatasetByID(ctx, did)

      val requesterUid = if (sessionUser.isPresent) Some(sessionUser.get().getUid) else None

      if (requesterUid.isEmpty && !dataset.getIsPublic) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      } else if (requesterUid.exists(uid => !userHasReadAccess(ctx, did, uid))) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val coverImage = Option(dataset.getCoverImage).getOrElse(
        throw new NotFoundException("No cover image")
      )

      val owner = getOwner(ctx, did)
      val fullPath =
        logicalPath(resourceType, owner.getEmail, dataset.getName, coverImage)

      val document = DocumentFactory
        .openReadonlyDocument(FileResolver.resolve(fullPath))
        .asInstanceOf[OnVersionedFileResource]

      val presignedUrl = withLakeFSErrorHandling(
        s"generating a presigned URL for cover image '$coverImage'"
      ) {
        LakeFSStorageClient.getFilePresignedUrl(
          document.getRepositoryName(),
          document.getVersionHash(),
          document.getFileRelativePath()
        )
      }

      Response.temporaryRedirect(new URI(presignedUrl)).build()
    }
  }

  /**
    * Get a presigned S3 URL for the dataset cover image as JSON.
    * JWT-aware variant of GET /{did}/cover; required for private datasets
    * since `<img src>` cannot attach the Authorization header.
    */
  @GET
  @PermitAll
  @Path("/{did}/cover-url")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getDatasetCoverUrl(
      @PathParam("did") did: Integer,
      @Auth sessionUser: Optional[SessionUser]
  ): Response = {
    withTransaction(context) { ctx =>
      val dataset = getDatasetByID(ctx, did)

      val requesterUid = if (sessionUser.isPresent) Some(sessionUser.get().getUid) else None

      if (requesterUid.isEmpty && !dataset.getIsPublic) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      } else if (requesterUid.exists(uid => !userHasReadAccess(ctx, did, uid))) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      Option(dataset.getCoverImage) match {
        case None =>
          Response.ok(Map("url" -> null)).build()
        case Some(coverImage) =>
          val owner = getOwner(ctx, did)
          val fullPath =
            logicalPath(resourceType, owner.getEmail, dataset.getName, coverImage)

          val document = DocumentFactory
            .openReadonlyDocument(FileResolver.resolve(fullPath))
            .asInstanceOf[OnVersionedFileResource]

          val presignedUrl = withLakeFSErrorHandling(
            s"generating a presigned URL for cover image '$coverImage'"
          ) {
            LakeFSStorageClient.getFilePresignedUrl(
              document.getRepositoryName(),
              document.getVersionHash(),
              document.getFileRelativePath()
            )
          }

          Response.ok(Map("url" -> presignedUrl)).build()
      }
    }
  }
}
