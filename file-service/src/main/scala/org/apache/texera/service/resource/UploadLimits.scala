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

import org.apache.texera.dao.SiteSettings

/** An admin-editable limit: its `site_settings` key, and the fallback when the row is missing
  * (must match the leaf's default in `default.conf`).
  */
case class UploadLimit(key: String, defaultValue: Long)

/**
  * The upload ceilings of one resource type.
  *
  * `site_settings` rows are keyed by a `default.conf` leaf's last path segment, so each type
  * needs its own key names. Only the size ceiling is enforced server-side; the chunk-size and
  * concurrency limits are client-side tuning served from `/config/settings/public`.
  */
case class UploadLimits(
    singleFileMaxSizeMiB: UploadLimit,
    multipartChunkSizeMiB: UploadLimit,
    maxConcurrentFiles: UploadLimit,
    maxConcurrentFileChunks: UploadLimit
) {

  /** The largest single file this resource type accepts, in bytes. */
  def singleFileUploadMaxBytes: Long =
    SiteSettings.getLong(
      singleFileMaxSizeMiB.key,
      singleFileMaxSizeMiB.defaultValue
    ) * 1024L * 1024L

  def all: Seq[UploadLimit] =
    Seq(singleFileMaxSizeMiB, multipartChunkSizeMiB, maxConcurrentFiles, maxConcurrentFileChunks)
}

object UploadLimits {

  val Dataset: UploadLimits =
    UploadLimits(
      singleFileMaxSizeMiB = UploadLimit("single_file_upload_max_size_mib", 20L),
      multipartChunkSizeMiB = UploadLimit("multipart_upload_chunk_size_mib", 50L),
      maxConcurrentFiles = UploadLimit("max_number_of_concurrent_uploading_file", 3L),
      maxConcurrentFileChunks = UploadLimit("max_number_of_concurrent_uploading_file_chunks", 10L)
    )

  // Model weights are far larger than the files datasets are sized for: 2 GiB, not 20 MiB.
  val Model: UploadLimits =
    UploadLimits(
      singleFileMaxSizeMiB = UploadLimit("model_single_file_upload_max_size_mib", 2048L),
      multipartChunkSizeMiB = UploadLimit("model_multipart_upload_chunk_size_mib", 50L),
      maxConcurrentFiles = UploadLimit("model_max_number_of_concurrent_uploading_file", 3L),
      maxConcurrentFileChunks =
        UploadLimit("model_max_number_of_concurrent_uploading_file_chunks", 10L)
    )
}
