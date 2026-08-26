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
import jakarta.ws.rs.core.{HttpHeaders, Response, StreamingOutput}
import org.apache.texera.amber.core.storage.ResourceType
import org.apache.texera.amber.core.storage.model.OnVersionedFileResource
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.amber.core.storage.{DocumentFactory, FileResolver}
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.{SiteSettings, SqlServer}
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.Model.MODEL
import org.apache.texera.dao.jooq.generated.tables.ModelUploadSession.MODEL_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.ModelUploadSessionPart.MODEL_UPLOAD_SESSION_PART
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSession.DATASET_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSessionPart.DATASET_UPLOAD_SESSION_PART
import org.apache.texera.dao.jooq.generated.tables.records.{
  DatasetRecord,
  DatasetUploadSessionPartRecord,
  DatasetUploadSessionRecord,
  DatasetUserAccessRecord,
  ModelRecord,
  ModelUploadSessionPartRecord,
  ModelUploadSessionRecord,
  ModelUserAccessRecord
}
import org.apache.texera.service.`type`.LakeFSFileNode
import org.apache.texera.service.util.LakeFSExceptionHandler.withLakeFSErrorHandling
import org.apache.texera.service.util.S3StorageClient
import org.apache.texera.service.util.S3StorageClient.{
  MAXIMUM_NUM_OF_MULTIPART_S3_PARTS,
  MINIMUM_NUM_OF_MULTIPART_S3_PART,
  PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS
}
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.jooq.impl.DSL.{inline => inl}
import org.jooq.{DSLContext, Record, Record2, Result, Table, TableField}
import software.amazon.awssdk.services.s3.model.UploadPartResponse

import java.io.{InputStream, OutputStream}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.Optional
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Describes where a resource's files live and how its in-progress uploads are tracked.
  *
  * [[ResourceTables]] names the columns that carry identity, ownership and grants; this adds
  * the storage side — the LakeFS repository column plus the `*_upload_session` and
  * `*_upload_session_part` tables that back resumable multipart uploads. Naming the columns
  * keeps one implementation of the upload engine serving every resource type, so adding the
  * next one costs a descriptor rather than another copy of the locking, part-size and
  * resume rules.
  *
  * @tparam R record type of the resource table
  * @tparam A record type of the companion user-access table
  * @tparam S record type of the upload-session table
  * @tparam P record type of the upload-session-part table
  */
case class ResourceStorage[R <: Record, A <: Record, S <: Record, P <: Record](
    resource: ResourceTables[R, A],
    resourceType: ResourceType.Value,
    repositoryNameField: TableField[R, String],
    sessionResourceId: TableField[S, Integer],
    sessionUid: TableField[S, Integer],
    sessionFilePath: TableField[S, String],
    sessionUploadId: TableField[S, String],
    sessionPhysicalAddress: TableField[S, String],
    sessionNumParts: TableField[S, Integer],
    sessionFileSize: TableField[S, java.lang.Long],
    sessionPartSize: TableField[S, java.lang.Long],
    sessionCreatedAt: TableField[S, OffsetDateTime],
    partUploadId: TableField[P, String],
    partNumber: TableField[P, Integer],
    partEtag: TableField[P, String]
) {
  def sessionTable: Table[S] = sessionResourceId.getTable
  def partTable: Table[P] = partUploadId.getTable
}

object ResourceStorage {

  val Dataset: ResourceStorage[
    DatasetRecord,
    DatasetUserAccessRecord,
    DatasetUploadSessionRecord,
    DatasetUploadSessionPartRecord
  ] =
    ResourceStorage(
      resource = ResourceTables.Dataset,
      resourceType = ResourceType.Dataset,
      repositoryNameField = DATASET.REPOSITORY_NAME,
      sessionResourceId = DATASET_UPLOAD_SESSION.DID,
      sessionUid = DATASET_UPLOAD_SESSION.UID,
      sessionFilePath = DATASET_UPLOAD_SESSION.FILE_PATH,
      sessionUploadId = DATASET_UPLOAD_SESSION.UPLOAD_ID,
      sessionPhysicalAddress = DATASET_UPLOAD_SESSION.PHYSICAL_ADDRESS,
      sessionNumParts = DATASET_UPLOAD_SESSION.NUM_PARTS_REQUESTED,
      sessionFileSize = DATASET_UPLOAD_SESSION.FILE_SIZE_BYTES,
      sessionPartSize = DATASET_UPLOAD_SESSION.PART_SIZE_BYTES,
      sessionCreatedAt = DATASET_UPLOAD_SESSION.CREATED_AT,
      partUploadId = DATASET_UPLOAD_SESSION_PART.UPLOAD_ID,
      partNumber = DATASET_UPLOAD_SESSION_PART.PART_NUMBER,
      partEtag = DATASET_UPLOAD_SESSION_PART.ETAG
    )

  val Model: ResourceStorage[
    ModelRecord,
    ModelUserAccessRecord,
    ModelUploadSessionRecord,
    ModelUploadSessionPartRecord
  ] =
    ResourceStorage(
      resource = ResourceTables.Model,
      resourceType = ResourceType.Model,
      repositoryNameField = MODEL.REPOSITORY_NAME,
      sessionResourceId = MODEL_UPLOAD_SESSION.MID,
      sessionUid = MODEL_UPLOAD_SESSION.UID,
      sessionFilePath = MODEL_UPLOAD_SESSION.FILE_PATH,
      sessionUploadId = MODEL_UPLOAD_SESSION.UPLOAD_ID,
      sessionPhysicalAddress = MODEL_UPLOAD_SESSION.PHYSICAL_ADDRESS,
      sessionNumParts = MODEL_UPLOAD_SESSION.NUM_PARTS_REQUESTED,
      sessionFileSize = MODEL_UPLOAD_SESSION.FILE_SIZE_BYTES,
      sessionPartSize = MODEL_UPLOAD_SESSION.PART_SIZE_BYTES,
      sessionCreatedAt = MODEL_UPLOAD_SESSION.CREATED_AT,
      partUploadId = MODEL_UPLOAD_SESSION_PART.UPLOAD_ID,
      partNumber = MODEL_UPLOAD_SESSION_PART.PART_NUMBER,
      partEtag = MODEL_UPLOAD_SESSION_PART.ETAG
    )
}

/**
  * The upload and version-file machinery shared by every versioned file resource.
  *
  * Every method here was previously duplicated per resource; the two copies differed only in
  * which jOOQ columns they named. Keeping one implementation means the locking protocol
  * (`FOR UPDATE NOWAIT`, SQLState 55P03 to 409), the part-size arithmetic and its overflow
  * guards, ETag idempotency and the resume/restart rules are defined once.
  */
object ResourceUploadService {

  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  private def singleFileUploadMaxBytes(defaultMiB: Long = 20L): Long =
    SiteSettings.getLong("single_file_upload_max_size_mib", defaultMiB) * 1024L * 1024L

  /**
    * Builds the file nodes of one committed version, plus the version's total size.
    *
    * The tree is rooted at the resource-type prefix, so the paths it yields resolve against
    * the right table when they are handed back to `FileResolver`.
    */
  def versionRootFileNodes(
      resourceType: ResourceType.Value,
      ownerEmail: String,
      resourceName: String,
      versionName: String,
      repositoryName: String,
      versionHash: String
  ): (List[LakeFSFileNode], Long) = {
    val rootNode = LakeFSFileNode
      .fromLakeFSRepositoryCommittedObjects(
        resourceType,
        Map(
          (ownerEmail, resourceName, versionName) -> LakeFSStorageClient
            .retrieveObjectsOfVersion(repositoryName, versionHash)
        )
      )
      .head

    val ownerFileNode = rootNode.getChildren.headOption.getOrElse(
      throw new IllegalStateException(
        s"File tree for $resourceName is missing its owner node"
      )
    )

    val nodes = ownerFileNode.children.get
      .find(_.getName == resourceName)
      .head
      .children
      .get
      .find(_.getName == versionName)
      .head
      .children
      .get

    (nodes, LakeFSFileNode.calculateTotalSize(List(rootNode)))
  }

  private def noAccessMessage[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P]
  ): String = s"User has no access to this ${s.resource.label}"

  /** Reads the LakeFS repository backing a resource, or 404s if the resource is gone. */
  private def repositoryNameOf[R <: Record, A <: Record, S <: Record, P <: Record](
      ctx: DSLContext,
      s: ResourceStorage[R, A, S, P],
      resourceId: Integer
  ): String =
    Option(
      ctx
        .select(s.repositoryNameField)
        .from(s.resource.table)
        .where(s.resource.idField.eq(resourceId))
        .fetchOne(s.repositoryNameField)
    ).getOrElse(
      throw new NotFoundException(s"${s.resource.label.capitalize} $resourceId not found")
    )

  /**
    * Resolves a presign-download request to (repositoryName, commitHash, relativePath),
    * after confirming the caller may read the owning resource.
    *
    * A caller either addresses the file directly (repositoryName + commitHash) or hands over a
    * logical path and lets the server resolve it; supplying exactly one of the pair is a client
    * error. Both routes then converge: find the resource that owns the repository, check read
    * access, and hand back the triple. `FileResolver` already dispatches on the path's
    * resource-type prefix, so the logical-path route needs nothing resource-specific here.
    */
  def resolveVersionedFile[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer,
      enforceDownloadable: Boolean = false
  ): Either[Response, (String, String, String)] = {
    val decodedPath = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name())

    requireBothOrNeither(repositoryName, commitHash) match {
      case Some(badRequest) => Left(badRequest)
      case None =>
        Right(withTransaction(context) { ctx =>
          if (repositoryName != null) {
            requireReadAccessToRepository(ctx, s, repositoryName, uid, enforceDownloadable)
            (repositoryName, commitHash, decodedPath)
          } else {
            val document = DocumentFactory
              .openReadonlyDocument(FileResolver.resolve(decodedPath))
              .asInstanceOf[OnVersionedFileResource]
            requireReadAccessToRepository(
              ctx,
              s,
              document.getRepositoryName(),
              uid,
              enforceDownloadable
            )
            (
              document.getRepositoryName(),
              document.getVersionHash(),
              document.getFileRelativePath()
            )
          }
        })
    }
  }

  /**
    * The whole presign-download endpoint: resolve the addressed file, check read
    * access, and hand back the signed URL. Both resources' endpoints are this call.
    */
  def presignedUrlResponse[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer,
      enforceDownloadable: Boolean = false
  ): Response =
    resolveVersionedFile(
      s,
      encodedUrl,
      repositoryName,
      commitHash,
      uid,
      enforceDownloadable
    ) match {
      case Left(errorResponse)         => errorResponse
      case Right((repo, commit, path)) => presignedResponse(repo, commit, path)
    }

  /**
    * A caller either addresses a file directly (repositoryName + commitHash) or lets
    * the server resolve it from a logical path (neither). Exactly one is a client error.
    *
    * @return Some(400 response) when only one of the two was provided, None otherwise
    */
  private def requireBothOrNeither(
      repositoryName: String,
      commitHash: String
  ): Option[Response] =
    (Option(repositoryName), Option(commitHash)) match {
      case (Some(_), None) | (None, Some(_)) =>
        Some(
          Response
            .status(Response.Status.BAD_REQUEST)
            .entity(
              "Both repositoryName and commitHash must be provided together, " +
                "or neither should be provided."
            )
            .build()
        )
      case _ => None
    }

  /** Wrap a LakeFS presigned URL in the response shape the frontend expects. */
  private def presignedResponse(
      repositoryName: String,
      commitHash: String,
      filePath: String
  ): Response = {
    val url = withLakeFSErrorHandling(
      s"generating a presigned URL for file '$filePath'"
    ) {
      LakeFSStorageClient.getFilePresignedUrl(repositoryName, commitHash, filePath)
    }

    Response.ok(Map("presignedUrl" -> url)).build()
  }

  /** Read access is checked on the resource that owns the repository, not on the file. */
  private def requireReadAccessToRepository[R <: Record, A <: Record, S <: Record, P <: Record](
      ctx: DSLContext,
      s: ResourceStorage[R, A, S, P],
      repositoryName: String,
      uid: Integer,
      enforceDownloadable: Boolean = false
  ): Unit = {
    val row = ctx
      .select(s.resource.idField, s.resource.ownerUidField, s.resource.isDownloadableField)
      .from(s.resource.table)
      .where(s.repositoryNameField.eq(repositoryName))
      .fetchOne()

    val id = if (row == null) null else row.get(s.resource.idField)
    if (id == null || !ResourceAccess.userHasReadAccess(ctx, s.resource, id, uid)) {
      throw new ForbiddenException(noAccessMessage(s))
    }

    // Readable is not the same as downloadable: without this, a caller who may only
    // read could presign the raw files and bypass the owner's download switch.
    if (enforceDownloadable) {
      val isOwner = uid != null && uid == row.get(s.resource.ownerUidField)
      if (!isOwner && !row.get(s.resource.isDownloadableField)) {
        throw new ForbiddenException(s"${s.resource.label.capitalize} download is not allowed")
      }
    }
  }

  /**
    * Streams every file of one committed version as a ZIP.
    *
    * Resolving which version, and whether the caller may download it, stays with the
    * resource: the download flag is not part of [[ResourceTables]]. This is the part that
    * is identical either way -- listing the version's objects and piping them into a zip.
    */
  def versionZipResponse(
      repositoryName: String,
      versionHash: String,
      displayName: String,
      versionName: String
  ): Response = {
    val objects = withLakeFSErrorHandling(
      s"listing files of version '$versionHash' of '$displayName'"
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
        .header("Content-Disposition", s"""attachment; filename="$displayName-$versionName.zip"""")
        .build()
    }
  }

  /** Staged (uncommitted) changes in a resource's repository. Requires read access. */
  def stagedChanges[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      resourceId: Integer,
      uid: Integer
  ): List[org.apache.texera.service.`type`.Diff] =
    withTransaction(context) { ctx =>
      if (!ResourceAccess.userHasReadAccess(ctx, s.resource, resourceId, uid)) {
        throw new ForbiddenException(noAccessMessage(s))
      }
      val repositoryName = repositoryNameOf(ctx, s, resourceId)
      withLakeFSErrorHandling {
        LakeFSStorageClient.retrieveUncommittedObjects(repositoryName)
      }.map(d =>
        org.apache.texera.service.`type`.Diff(
          d.getPath,
          d.getPathType.getValue,
          d.getType.getValue,
          Option(d.getSizeBytes).map(_.longValue())
        )
      )
    }

  /** Discards one staged change, restoring the file to its last committed state. */
  def resetStagedChange[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      resourceId: Integer,
      encodedFilePath: String,
      uid: Integer
  ): Response =
    withTransaction(context) { ctx =>
      if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, resourceId, uid)) {
        throw new ForbiddenException(noAccessMessage(s))
      }
      val repositoryName = repositoryNameOf(ctx, s, resourceId)
      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      withLakeFSErrorHandling(s"resetting uncommitted changes of file '$filePath'") {
        LakeFSStorageClient.resetObjectUploadOrDeletion(repositoryName, filePath)
      }
      Response.ok().build()
    }

  /**
    * Reports which of the requested files the repository already holds at the same size,
    * so a client can skip re-uploading them. Committed and staged files both count. A
    * staged deletion contributes no match of its own, but it does not withdraw the
    * committed file either -- behaviour carried over verbatim from the dataset endpoint
    * this was extracted from.
    *
    * The latest committed version is read through `latestVersionHash` because the version
    * table is the one part of this that is resource-specific.
    */
  def matchExistingUploads[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      resourceId: Integer,
      uid: Integer,
      request: org.apache.texera.service.`type`.ExistingUploadFilesRequest,
      latestVersionHash: DSLContext => Option[String]
  ): Response =
    withTransaction(context) { ctx =>
      if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, resourceId, uid)) {
        throw new ForbiddenException(noAccessMessage(s))
      }

      val requested = Option(request)
        .flatMap(r => Option(r.files))
        .getOrElse(List.empty)
        .map { file =>
          val originalPath = file.path
          val path = ResourceNaming.validateAndNormalizeFilePathOrThrow(originalPath)
          if (file.sizeBytes < 0L) throw new BadRequestException("sizeBytes must be >= 0")
          (path, originalPath, file.sizeBytes)
        }

      val repositoryName = repositoryNameOf(ctx, s, resourceId)
      val committed = latestVersionHash(ctx)
        .map { hash =>
          withLakeFSErrorHandling(
            s"retrieving committed files of ${s.resource.label} $resourceId"
          ) {
            LakeFSStorageClient
              .retrieveObjectsOfVersion(repositoryName, hash)
              .map(obj => obj.getPath -> obj.getSizeBytes.longValue())
          }
        }
        .getOrElse(List.empty)

      val staged = withLakeFSErrorHandling(
        s"retrieving staged files of ${s.resource.label} $resourceId"
      ) {
        LakeFSStorageClient.retrieveUncommittedObjects(repositoryName)
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

  /** Removes one staged (uncommitted) file from a resource's repository. */
  def deleteStagedFile[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      resourceId: Integer,
      encodedFilePath: String,
      uid: Integer
  ): Response = {
    withTransaction(context) { ctx =>
      if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, resourceId, uid)) {
        throw new ForbiddenException(noAccessMessage(s))
      }
      val repositoryName = repositoryNameOf(ctx, s, resourceId)

      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      withLakeFSErrorHandling(
        s"deleting file '$filePath' from the ${s.resource.label} repository"
      ) {
        LakeFSStorageClient.deleteObject(repositoryName, filePath)
      }

      Response.ok().build()
    }
  }

  def uploadOneFile[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      resourceId: Integer,
      encodedFilePath: String,
      fileStream: InputStream,
      headers: HttpHeaders,
      uid: Integer
  ): Response = {
    // These variables are defined at the top so catch block can access them
    var repoName: String = null
    var filePath: String = null
    var uploadId: String = null
    var physicalAddress: String = null

    try {
      withTransaction(context) { ctx =>
        if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, resourceId, uid))
          throw new ForbiddenException(noAccessMessage(s))

        repoName = repositoryNameOf(ctx, s, resourceId)
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
          .map(ln =>
            ((ln + partSize - 1) / partSize).toInt + 1
          ) // “+1” for the last (possibly small) part
          .getOrElse(MAXIMUM_NUM_OF_MULTIPART_S3_PARTS)

        // ---------- ask LakeFS for presigned URLs ----------
        val presign = LakeFSStorageClient
          .initiatePresignedMultipartUploads(repoName, filePath, expectedParts)
        uploadId = presign.getUploadId
        val presignedUrls = presign.getPresignedUrls.asScala.iterator
        physicalAddress = presign.getPhysicalAddress

        // ---------- stream & upload parts ----------
        /*
        1. Reads the input stream in chunks of 'partSize' bytes by stacking them in a buffer
        2. Uploads each chunk (part) using a presigned URL
        3. Tracks each part number and ETag returned from S3
        4. After all parts are uploaded, completes the multipart upload
         */
        val buf = new Array[Byte](partSize.toInt)
        var buffered = 0
        var partNumber = 1
        val completedParts = ListBuffer[(Int, String)]()

        @inline def flush(): Unit = {
          if (buffered == 0) return
          if (!presignedUrls.hasNext)
            throw new WebApplicationException("Ran out of presigned part URLs – ask for more parts")

          val etag = LakeFSStorageClient.put(buf, buffered, presignedUrls.next(), partNumber)
          completedParts += ((partNumber, etag))
          partNumber += 1
          buffered = 0
        }

        var read = fileStream.read(buf, buffered, buf.length - buffered)
        while (read != -1) {
          buffered += read
          if (buffered >= buf.length) flush() // buffer full
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
          // best-effort cleanup; never let an abort failure mask the original error
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
              s"Failed to upload file to ${s.resource.label}: ${other.getMessage}",
              other
            )
        }
    }
  }

  def listUploads[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      did: Integer,
      requesterUid: Int
  ): Response = {
    withTransaction(context) { ctx =>
      if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, did, requesterUid)) {
        throw new ForbiddenException(noAccessMessage(s))
      }

      val filePaths =
        ctx
          .selectDistinct(s.sessionFilePath)
          .from(s.sessionTable)
          .where(s.sessionResourceId.eq(did))
          .and(
            DSL.condition(
              "created_at > current_timestamp - (? * interval '1 hour')",
              PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS
            )
          )
          .orderBy(s.sessionFilePath.asc())
          .fetch(s.sessionFilePath)
          .asScala
          .toList

      Response.ok(Map("filePaths" -> filePaths.asJava)).build()
    }
  }

  def initUpload[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      did: Integer,
      encodedFilePath: String,
      fileSizeBytes: Optional[java.lang.Long],
      partSizeBytes: Optional[java.lang.Long],
      restart: Optional[java.lang.Boolean],
      uid: Integer
  ): Response = {

    withTransaction(context) { ctx =>
      if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, did, uid)) {
        throw new ForbiddenException(noAccessMessage(s))
      }

      val repositoryName = repositoryNameOf(ctx, s, did)

      val filePath =
        ResourceNaming.validateAndNormalizeFilePathOrThrow(
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
      var session: S = null.asInstanceOf[S]
      var rows: Result[Record2[Integer, String]] = null
      try {
        session = ctx
          .selectFrom(s.sessionTable)
          .where(
            s.sessionUid
              .eq(uid)
              .and(s.sessionResourceId.eq(did))
              .and(s.sessionFilePath.eq(filePath))
          )
          .forUpdate()
          .noWait()
          .fetchOne()
        if (session != null) {
          //Gain parts lock
          rows = ctx
            .select(s.partNumber, s.partEtag)
            .from(s.partTable)
            .where(s.partUploadId.eq(session.get(s.sessionUploadId)))
            .forUpdate()
            .noWait()
            .fetch()
          val dbFileSize = session.get(s.sessionFileSize)
          val dbPartSize = session.get(s.sessionPartSize)
          val dbNumParts = session.get(s.sessionNumParts)
          val createdAt: OffsetDateTime = session.get(s.sessionCreatedAt)

          val isExpired =
            createdAt
              .plusHours(PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS.toLong)
              .isBefore(OffsetDateTime.now(createdAt.getOffset)) // or OffsetDateTime.now()

          val conflictConfig =
            dbFileSize != fileSizeBytesValue ||
              dbPartSize != partSizeBytesValue ||
              dbNumParts != computedNumParts ||
              isExpired ||
              Option(restart).exists(_.orElse(false))

          if (conflictConfig) {
            // Parts will be deleted automatically (ON DELETE CASCADE)
            ctx
              .deleteFrom(s.sessionTable)
              .where(s.sessionUploadId.eq(session.get(s.sessionUploadId)))
              .execute()

            try {
              LakeFSStorageClient.abortPresignedMultipartUploads(
                repositoryName,
                filePath,
                session.get(s.sessionUploadId),
                session.get(s.sessionPhysicalAddress)
              )
            } catch { case _: Throwable => () }
            session = null.asInstanceOf[S]
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
            .insertInto(s.sessionTable)
            .set(s.sessionFilePath, filePath)
            .set(s.sessionResourceId, did)
            .set(s.sessionUid, uid)
            .set(s.sessionUploadId, uploadIdStr)
            .set(s.sessionPhysicalAddress, physicalAddr)
            .set(s.sessionNumParts, Integer.valueOf(computedNumParts))
            .set(s.sessionFileSize, java.lang.Long.valueOf(fileSizeBytesValue))
            .set(s.sessionPartSize, java.lang.Long.valueOf(partSizeBytesValue))
            .onDuplicateKeyIgnore()
            .execute()

          if (rowsInserted == 1) {
            val partNumberSeries =
              DSL.generateSeries(1, computedNumParts).asTable("gs", "partNumberField")
            val partNumberField = partNumberSeries.field("partNumberField", classOf[Integer])

            ctx
              .insertInto(
                s.partTable,
                s.partUploadId,
                s.partNumber,
                s.partEtag
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
              .selectFrom(s.sessionTable)
              .where(
                s.sessionUid
                  .eq(uid)
                  .and(s.sessionResourceId.eq(did))
                  .and(s.sessionFilePath.eq(filePath))
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
              .selectFrom(s.sessionTable)
              .where(
                s.sessionUid
                  .eq(uid)
                  .and(s.sessionResourceId.eq(did))
                  .and(s.sessionFilePath.eq(filePath))
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

      val dbNumParts = session.get(s.sessionNumParts)

      val uploadId = session.get(s.sessionUploadId)
      val nParts = dbNumParts

      // CHANGED: lock rows with NOWAIT; if any row is locked by another uploader -> 409
      if (rows == null) {
        rows =
          try {
            ctx
              .select(s.partNumber, s.partEtag)
              .from(s.partTable)
              .where(s.partUploadId.eq(uploadId))
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

      // CHANGED: compute missingParts + completedPartsCount from the SAME query result
      val missingParts = rows.asScala
        .filter(r => Option(r.get(s.partEtag)).map(_.trim).getOrElse("").isEmpty)
        .map(r => r.get(s.partNumber).intValue())
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

  def finishUpload[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      did: Integer,
      encodedFilePath: String,
      uid: Int
  ): Response = {

    val filePath = ResourceNaming.validateAndNormalizeFilePathOrThrow(
      URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
    )

    withTransaction(context) { ctx =>
      if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, did, uid)) {
        throw new ForbiddenException(noAccessMessage(s))
      }

      val repositoryName = repositoryNameOf(ctx, s, did)

      // Lock the session so abort/finish don't race each other
      val session =
        try {
          ctx
            .selectFrom(s.sessionTable)
            .where(
              s.sessionUid
                .eq(uid)
                .and(s.sessionResourceId.eq(did))
                .and(s.sessionFilePath.eq(filePath))
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

      val uploadId = session.get(s.sessionUploadId)
      val expectedParts = session.get(s.sessionNumParts)

      val physicalAddr = Option(session.get(s.sessionPhysicalAddress)).map(_.trim).getOrElse("")
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
          .filterWhere(s.partEtag.ne(""))
          .as("done")

      val agg = ctx
        .select(total.as("total"), done)
        .from(s.partTable)
        .where(s.partUploadId.eq(uploadId))
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
          .select(s.partNumber)
          .from(s.partTable)
          .where(
            s.partUploadId
              .eq(uploadId)
              .and(s.partEtag.eq(""))
          )
          .orderBy(s.partNumber.asc())
          .limit(50)
          .fetch(s.partNumber)
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
          .select(s.partNumber, s.partEtag)
          .from(s.partTable)
          .where(s.partUploadId.eq(uploadId))
          .orderBy(s.partNumber.asc())
          .fetch()
          .asScala
          .map(r =>
            (
              r.get(s.partNumber).intValue(),
              r.get(s.partEtag)
            )
          )
          .toList

      val objectStats = withLakeFSErrorHandling {
        LakeFSStorageClient.completePresignedMultipartUploads(
          repositoryName,
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
          LakeFSStorageClient.resetObjectUploadOrDeletion(repositoryName, filePath)
        } catch {
          case _: Throwable => ()
        }
      }

      // always cleanup session
      ctx
        .deleteFrom(s.sessionTable)
        .where(
          s.sessionUid
            .eq(uid)
            .and(s.sessionResourceId.eq(did))
            .and(s.sessionFilePath.eq(filePath))
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

  def abortUpload[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      did: Integer,
      encodedFilePath: String,
      uid: Int
  ): Response = {

    val filePath = ResourceNaming.validateAndNormalizeFilePathOrThrow(
      URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
    )

    val (repoName, uploadId, physicalAddr) = withTransaction(context) { ctx =>
      if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, did, uid)) {
        throw new ForbiddenException(noAccessMessage(s))
      }

      val repositoryName = repositoryNameOf(ctx, s, did)

      val session =
        try {
          ctx
            .selectFrom(s.sessionTable)
            .where(
              s.sessionUid
                .eq(uid)
                .and(s.sessionResourceId.eq(did))
                .and(s.sessionFilePath.eq(filePath))
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

      val physicalAddr = Option(session.get(s.sessionPhysicalAddress)).map(_.trim).getOrElse("")

      // Delete session; parts removed via ON DELETE CASCADE
      ctx
        .deleteFrom(s.sessionTable)
        .where(
          s.sessionUid
            .eq(uid)
            .and(s.sessionResourceId.eq(did))
            .and(s.sessionFilePath.eq(filePath))
        )
        .execute()

      (repositoryName, session.get(s.sessionUploadId), physicalAddr)
    }

    withLakeFSErrorHandling {
      LakeFSStorageClient.abortPresignedMultipartUploads(repoName, filePath, uploadId, physicalAddr)
    }

    Response.ok(Map("message" -> "Multipart upload aborted successfully")).build()
  }

  def uploadPart[R <: Record, A <: Record, S <: Record, P <: Record](
      s: ResourceStorage[R, A, S, P],
      did: Integer,
      uid: Int,
      encodedFilePath: String,
      partNumber: Int,
      partStream: InputStream,
      headers: HttpHeaders
  ): Response = {
    if (encodedFilePath == null || encodedFilePath.isEmpty)
      throw new BadRequestException("filePath is required")
    if (partNumber < 1)
      throw new BadRequestException("partNumber must be >= 1")

    val filePath = ResourceNaming.validateAndNormalizeFilePathOrThrow(
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
      if (!ResourceAccess.userHasWriteAccess(ctx, s.resource, did, uid))
        throw new ForbiddenException(noAccessMessage(s))

      val session = ctx
        .selectFrom(s.sessionTable)
        .where(
          s.sessionUid
            .eq(uid)
            .and(s.sessionResourceId.eq(did))
            .and(s.sessionFilePath.eq(filePath))
        )
        .fetchOne()

      if (session == null)
        throw new NotFoundException("Upload session not found. Call type=init first.")

      val expectedParts: Int = session.get(s.sessionNumParts)
      val fileSizeBytesValue: Long = session.get(s.sessionFileSize)
      val partSizeBytesValue: Long = session.get(s.sessionPartSize)

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

      val physicalAddr = Option(session.get(s.sessionPhysicalAddress)).map(_.trim).getOrElse("")
      if (physicalAddr.isEmpty) {
        throw new WebApplicationException(
          "Upload session is missing physicalAddress. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      val uploadId = session.get(s.sessionUploadId)
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
            .selectFrom(s.partTable)
            .where(
              s.partUploadId
                .eq(uploadId)
                .and(s.partNumber.eq(partNumber))
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
        // Should not happen if init pre-created rows
        throw new WebApplicationException(
          s"Part row not initialized for part $partNumber. Restart the upload.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      // Idempotency: if ETag already set, accept the retry quickly.
      val existing = Option(partRow.get(s.partEtag)).map(_.trim).getOrElse("")
      if (existing.isEmpty) {
        // Stream to S3 while holding the part lock (prevents concurrent streams for same part)
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
          .update(s.partTable)
          .set(s.partEtag, etagClean)
          .where(
            s.partUploadId
              .eq(uploadId)
              .and(s.partNumber.eq(partNumber))
          )
          .execute()
      }
      Response.ok().build()
    }
  }

}
