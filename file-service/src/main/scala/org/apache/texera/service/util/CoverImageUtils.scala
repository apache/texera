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

package org.apache.texera.service.util

import jakarta.ws.rs.BadRequestException
import org.apache.commons.io.FilenameUtils
import org.apache.commons.vfs2.FileNotFoundException
import org.apache.texera.amber.core.storage.model.OnVersionedFileResource
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.amber.core.storage.{DocumentFactory, FileResolver, ResourceType}
import org.apache.texera.service.resource.ResourceNaming
import org.apache.texera.service.util.LakeFSExceptionHandler.withLakeFSErrorHandling

/**
  * Resource-agnostic halves of the cover-image endpoints, shared by datasets and models.
  * The extension allowlist is a security control: a cover is served to the browser as a
  * presigned URL, so a duplicated allowlist that drifts is a real risk.
  */
object CoverImageUtils {

  val SIZE_LIMIT_BYTES: Long = 10 * 1024 * 1024 // 10 MB

  /** varchar(255) on `model`; `dataset` passes its own narrower limit. */
  val MAX_PATH_LENGTH: Int = 255

  private val ALLOWED_EXTENSIONS: Set[String] = Set(".jpg", ".jpeg", ".png", ".gif", ".webp")

  /** Normalizes a cover path relative to the resource root and enforces the image allowlist. */
  def validatePathOrThrow(coverImage: String, maxPathLength: Int): String = {
    if (coverImage == null || coverImage.trim.isEmpty) {
      throw new BadRequestException("Cover image path is required")
    }

    val normalized = ResourceNaming.validateAndNormalizeFilePathOrThrow(coverImage)

    val extension = FilenameUtils.getExtension(normalized)
    if (extension == null || !ALLOWED_EXTENSIONS.contains(s".$extension".toLowerCase)) {
      throw new BadRequestException("Invalid file type")
    }

    // FileResolver needs <version>/<file>; a bare name builds a path it cannot parse.
    if (normalized.split("/").length < 2) {
      throw new BadRequestException(
        "Cover image path must be relative to the resource root, as '<version>/<file>'"
      )
    }

    // Guard the column width here so an over-long path is a 400, not a jOOQ-wrapped 500.
    if (normalized.length > maxPathLength) {
      throw new BadRequestException(s"Cover image path must be at most $maxPathLength characters")
    }
    normalized
  }

  /**
    * Opens the committed image at the cover path, or None if it no longer resolves.
    * An Option because FileResolver's IOException has no mapper and would otherwise be
    * an opaque 500 on every card render; callers decide what an unresolvable cover means.
    */
  def openCover(
      resourceType: ResourceType.Value,
      ownerEmail: String,
      resourceName: String,
      normalized: String
  ): Option[OnVersionedFileResource] =
    try {
      Some(
        DocumentFactory
          .openReadonlyDocument(
            FileResolver.resolve(s"$resourceType/$ownerEmail/$resourceName/$normalized")
          )
          .asInstanceOf[OnVersionedFileResource]
      )
    } catch {
      case _: FileNotFoundException => None
    }

  /** Write-path reading of an unresolvable cover: the client sent a bad path. */
  def openCoverOrBadRequest(
      resourceType: ResourceType.Value,
      ownerEmail: String,
      resourceName: String,
      normalized: String
  ): OnVersionedFileResource =
    openCover(resourceType, ownerEmail, resourceName, normalized).getOrElse(
      throw new BadRequestException(s"No committed file at cover image path '$normalized'")
    )

  def requireWithinSizeLimit(fileSize: Long): Unit =
    if (fileSize > SIZE_LIMIT_BYTES) {
      throw new BadRequestException(
        s"Cover image must be less than ${SIZE_LIMIT_BYTES / (1024 * 1024)} MB"
      )
    }

  def fileSizeOf(document: OnVersionedFileResource, normalized: String): Long =
    withLakeFSErrorHandling(s"reading the size of cover image '$normalized'") {
      LakeFSStorageClient.getFileSize(
        document.getRepositoryName(),
        document.getVersionHash(),
        document.getFileRelativePath()
      )
    }

  /** Presigned S3 URL for an already-authorized cover image. */
  def presignedUrl(document: OnVersionedFileResource, normalized: String): String =
    withLakeFSErrorHandling(s"generating a presigned URL for cover image '$normalized'") {
      LakeFSStorageClient.getFilePresignedUrl(
        document.getRepositoryName(),
        document.getVersionHash(),
        document.getFileRelativePath()
      )
    }
}
