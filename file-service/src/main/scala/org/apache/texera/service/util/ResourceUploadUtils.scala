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
}
