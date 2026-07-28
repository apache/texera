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
import org.apache.texera.service.`type`.ExistingUploadFile

import java.net.{HttpURLConnection, URL}

/**
  * Resource-agnostic helpers shared by the dataset and model upload flows.
  */
object ResourceUploadUtils {

  /**
    * PUT exactly `len` bytes from `buf` to a presigned URL and return the ETag.
    */
  def put(buf: Array[Byte], len: Int, url: String, partNum: Int): String = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setDoOutput(true)
    conn.setRequestMethod("PUT")
    conn.setFixedLengthStreamingMode(len)
    val out = conn.getOutputStream
    out.write(buf, 0, len)
    out.close()

    val code = conn.getResponseCode
    if (code != HttpURLConnection.HTTP_OK && code != HttpURLConnection.HTTP_CREATED)
      throw new RuntimeException(s"Part $partNum upload failed (HTTP $code)")

    val etag = conn.getHeaderField("ETag").replace("\"", "")
    conn.disconnect()
    etag
  }

  /**
    * Validates a file path using Apache Commons IO. Rejects empty paths,
    * paths that traverse above the root, and absolute paths.
    */
  def validateAndNormalizeFilePathOrThrow(path: String): String = {
    if (path == null || path.trim.isEmpty) {
      throw new BadRequestException("Path cannot be empty")
    }

    val normalized = FilenameUtils.normalize(path, true)
    if (normalized == null) {
      throw new BadRequestException("Invalid path")
    }

    if (FilenameUtils.getPrefixLength(normalized) > 0) {
      throw new BadRequestException("Absolute paths not allowed")
    }
    normalized
  }

  /**
    * A requested upload whose path has been normalized, keeping the caller's original
    * spelling so it can be echoed back and correlated with the client's own queue.
    */
  case class NormalizedUploadFile(path: String, originalPath: String, sizeBytes: Long)

  /**
    * Validate and normalize what the client says it is about to upload.
    *
    * @throws jakarta.ws.rs.BadRequestException if a path is invalid or a size is negative
    */
  def normalizeUploadRequest(requested: List[ExistingUploadFile]): List[NormalizedUploadFile] =
    Option(requested).getOrElse(List.empty).map { file =>
      val originalPath = file.path
      val path = validateAndNormalizeFilePathOrThrow(originalPath)
      if (file.sizeBytes < 0L) throw new BadRequestException("sizeBytes must be >= 0")
      NormalizedUploadFile(path, originalPath, file.sizeBytes)
    }

  /**
    * Of the files the client is about to upload, return those the repository already
    * holds with the exact same size — the client skips re-uploading those.
    *
    * @param requested  output of [[normalizeUploadRequest]]
    * @param committed  (path, size) of the files in the latest committed version
    * @param staged     (path, size) of the uncommitted files, deletions excluded
    */
  def matchExistingUploads(
      requested: List[NormalizedUploadFile],
      committed: List[(String, Long)],
      staged: List[(String, Long)]
  ): List[String] = {
    val existing = (committed ++ staged).toMap
    requested
      .collect {
        case file if existing.get(file.path).contains(file.sizeBytes) => file.originalPath
      }
      .distinct
      .sorted
  }
}
