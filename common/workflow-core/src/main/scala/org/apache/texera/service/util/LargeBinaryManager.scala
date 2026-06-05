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

/**
  * Manages the lifecycle of LargeBinaries stored in S3.
  *
  * Handles creation and deletion of large objects that exceed
  * normal tuple size limits.
  */
object LargeBinaryManager extends LazyLogging {
  // Sourced from config so the JVM and Python workers (and cleanup) share one bucket.
  val DEFAULT_BUCKET: String = StorageConfig.s3LargeBinariesBucket

  /**
    * The object-key prefix that namespaces one execution's large binaries. Single source
    * of truth for the per-execution layout: baseUriForExecution() builds write URIs under
    * it and deleteByExecution() deletes it, so the write and delete paths cannot drift.
    */
  private def executionPrefix(executionId: Long): String = s"objects/$executionId"

  /**
    * Builds the execution-scoped base URI (trailing slash included) under which a given
    * execution's large binaries live. The controller names this location and hands it to
    * the worker (via WorkerConfig); create() only appends a unique suffix, so the worker
    * never constructs execution-scoped names itself. Returns an empty string when the
    * bucket is unconfigured, so create() fails loudly rather than minting a malformed URI.
    */
  def baseUriForExecution(executionId: Long): String =
    if (DEFAULT_BUCKET.isEmpty) ""
    else s"s3://$DEFAULT_BUCKET/${executionPrefix(executionId)}/"

  /**
    * Worker-scoped base URI for large binaries created on the current thread. It MUST be
    * set on, and read from, the worker's data-processing thread — the same thread that
    * runs the operator and calls create() — which is why a thread-local is used. It is
    * seeded once when the DP thread starts, which assumes one worker (hence one DP thread)
    * per execution; if workers are ever pooled or reused across executions, this must be
    * re-seeded per execution.
    */
  private val currentBaseUri: ThreadLocal[Option[String]] =
    ThreadLocal.withInitial(() => Option.empty[String])

  /**
    * Sets the base URI for large binaries created on the current thread. An empty string
    * clears it, so a missing base URI makes create() fail loudly rather than reusing a
    * stale value — keeping behavior consistent with the Python worker.
    */
  def setCurrentBaseUri(baseUri: String): Unit =
    currentBaseUri.set(Option(baseUri).filter(_.nonEmpty))

  /**
    * Creates a new LargeBinary reference under the current thread's base URI by appending
    * a unique suffix. The base URI is named by the controller and handed down, so the
    * worker never builds execution-scoped names itself. The actual data upload happens
    * separately via LargeBinaryOutputStream.
    *
    * @return S3 URI for the new LargeBinary, e.g. s3://bucket/objects/{eid}/{uuid}; the
    *         objects/{eid}/ structure comes from the base URI (baseUriForExecution), not here.
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
    * Deletes all large binaries belonging to a single execution.
    *
    * @param executionId the execution whose large binaries should be removed
    */
  def deleteByExecution(executionId: Long): Unit =
    deleteByExecution(executionId, S3StorageClient.deleteDirectory)

  /**
    * Overload that takes the directory-delete operation as a parameter. Visible for
    * testing
    */
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
      // Swallowing is intentional: cleanup runs as a side effect of execution/workflow
      // deletion and must not fail that operation. Logged at error because a failure
      // here silently leaks storage (bad credentials, unreachable endpoint, partial
      // delete), which would otherwise be invisible.
      case e: Exception =>
        logger.error(
          s"Failed to delete large binaries for execution $executionId " +
            s"from bucket: $DEFAULT_BUCKET",
          e
        )
    }
  }

}
