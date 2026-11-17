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

import jakarta.ws.rs.core.Response
import org.jooq.Record

import java.util.Optional

/**
  * The presigned-download and multipart-upload delegations shared by every versioned
  * file resource. DatasetResource and ModelResource each expose the same endpoints and
  * forwarded them to [[ResourceUploadService]] with their own storage descriptor; the two
  * copies differed only in that descriptor. A mixing-in resource supplies its
  * [[ResourceStorage]] once and keeps the single-line endpoint methods that call these.
  *
  * The storage descriptor is held as an existential because the service methods abstract
  * over its four record type parameters and use them only to type the descriptor argument.
  */
trait ResourceUploadSupport {

  protected def resourceStorage: ResourceStorage[_ <: Record, _ <: Record, _ <: Record, _ <: Record]

  protected def generatePresignedResponse(
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Response =
    ResourceUploadService.presignedUrlResponse(
      resourceStorage,
      encodedUrl,
      repositoryName,
      commitHash,
      uid
    )

  protected def listMultipartUploads(resourceId: Integer, requesterUid: Int): Response =
    ResourceUploadService.listUploads(resourceStorage, resourceId, requesterUid)

  protected def initMultipartUpload(
      resourceId: Integer,
      encodedFilePath: String,
      fileSizeBytes: Optional[java.lang.Long],
      partSizeBytes: Optional[java.lang.Long],
      restart: Optional[java.lang.Boolean],
      uid: Integer
  ): Response =
    ResourceUploadService.initUpload(
      resourceStorage,
      resourceId,
      encodedFilePath,
      fileSizeBytes,
      partSizeBytes,
      restart,
      uid
    )

  protected def finishMultipartUpload(
      resourceId: Integer,
      encodedFilePath: String,
      uid: Int
  ): Response =
    ResourceUploadService.finishUpload(resourceStorage, resourceId, encodedFilePath, uid)

  protected def abortMultipartUpload(
      resourceId: Integer,
      encodedFilePath: String,
      uid: Int
  ): Response =
    ResourceUploadService.abortUpload(resourceStorage, resourceId, encodedFilePath, uid)
}
