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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.config.StorageConfig

import java.util.UUID

/** Manages the lifecycle of LargeBinaries (objects too large for normal tuples) in S3. */
object LargeBinaryManager extends LazyLogging {
  // From config so the JVM/Python workers and cleanup all share one bucket.
  val DEFAULT_BUCKET: String = StorageConfig.s3LargeBinariesBucket

  /** Per-execution key prefix; the single source of truth for the write and delete paths. */
  private def executionPrefix(executionId: Long): String = s"objects/$executionId"

  /**
    * Base URI (trailing slash) under which `executionId`'s large binaries live; create()
    * appends a unique suffix. Empty when the bucket is unconfigured, so create() fails loudly.
    *
    * `executionId` must be a persisted EID. The sentinel id (DEFAULT_EXECUTION_ID = 1) shares
    * this space, so binaries must only be created under a real execution — else execution 1
    * and a default-context run would collide under objects/1/.
    */
  def baseUriForExecution(executionId: Long): String =
    if (DEFAULT_BUCKET.isEmpty) ""
    else s"s3://$DEFAULT_BUCKET/${executionPrefix(executionId)}/"

  /**
    * Base URI for binaries created on the current thread — thread-local because create()
    * runs on the DP thread. Seeded once at DP-thread start, assuming a thread serves one
    * execution; re-seed if workers are ever reused across executions.
    */
  private val currentBaseUri: ThreadLocal[Option[String]] =
    ThreadLocal.withInitial(() => Option.empty[String])

  /** Sets the current thread's base URI; an empty value clears it, so create() fails loudly. */
  def setCurrentBaseUri(baseUri: String): Unit =
    currentBaseUri.set(Option(baseUri).filter(_.nonEmpty))

  /**
    * Creates a LargeBinary reference by appending a unique suffix to the current thread's
    * base URI. Data is uploaded separately via LargeBinaryOutputStream.
    *
    * @return e.g. s3://bucket/objects/{eid}/{uuid}
    */
  def create(): String = {
    val baseUri = currentBaseUri
      .get()
      .getOrElse(
        throw new IllegalStateException(
          "LargeBinaryManager.create() requires a base URI, " +
            "but none was set on the current thread."
        )
      )
    s"$baseUri${UUID.randomUUID()}"
  }

  /**
    * Deletes all large binaries for one execution via deleteDirectory, which removes every object
    * under the prefix.
    *
    * @param executionId the execution whose large binaries should be removed
    */
  def deleteByExecution(executionId: Long): Unit =
    deleteByExecution(executionId, S3StorageClient.deleteDirectory)

  /** Overload taking the delete op as a parameter. Visible for testing. */
  private[util] def deleteByExecution(
      executionId: Long,
      deleteDir: (String, String) => Unit
  ): Unit = {
    try {
      deleteDir(DEFAULT_BUCKET, executionPrefix(executionId))
      logger.info(
        s"Deleted large binaries for execution $executionId from bucket: $DEFAULT_BUCKET"
      )
    } catch {
      // Swallowed: cleanup is a side effect of deletion and must not fail it. Logged at
      // error because a failure here silently leaks storage.
      case e: Exception =>
        logger.error(
          s"Failed to delete large binaries for execution $executionId " +
            s"from bucket: $DEFAULT_BUCKET",
          e
        )
    }
  }

}
