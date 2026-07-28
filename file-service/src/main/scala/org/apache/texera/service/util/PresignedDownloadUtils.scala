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

import jakarta.ws.rs.core.Response
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.service.util.LakeFSExceptionHandler.withLakeFSErrorHandling

// Resource-agnostic halves of the presign-download endpoints]
object PresignedDownloadUtils {

  private val ERR_BOTH_OR_NEITHER =
    "Both repositoryName and commitHash must be provided together, or neither should be provided."

  /**
    * A caller either addresses a file directly (repositoryName + commitHash) or lets
    * the server resolve it from a logical path (neither). Exactly one is a client error.
    *
    * @return Some(400 response) when only one of the two was provided, None otherwise
    */
  def requireBothOrNeither(repositoryName: String, commitHash: String): Option[Response] =
    (Option(repositoryName), Option(commitHash)) match {
      case (Some(_), None) | (None, Some(_)) =>
        Some(
          Response
            .status(Response.Status.BAD_REQUEST)
            .entity(ERR_BOTH_OR_NEITHER)
            .build()
        )
      case _ => None
    }

  /**
    * Wrap a LakeFS presigned URL in the response shape the frontend expects.
    * Call only after the caller's read access has been verified.
    */
  def presignedResponse(repositoryName: String, commitHash: String, filePath: String): Response = {
    val url = withLakeFSErrorHandling(
      s"generating a presigned URL for file '$filePath'"
    ) {
      LakeFSStorageClient.getFilePresignedUrl(repositoryName, commitHash, filePath)
    }

    Response.ok(Map("presignedUrl" -> url)).build()
  }
}
