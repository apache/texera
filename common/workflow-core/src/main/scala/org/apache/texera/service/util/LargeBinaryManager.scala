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

import java.util.UUID

/**
  * Manages the lifecycle of LargeBinaries stored in S3.
  *
  * Handles creation and deletion of large objects that exceed
  * normal tuple size limits.
  */
object LargeBinaryManager extends LazyLogging {
  val DEFAULT_BUCKET: String = "texera-large-binaries"

  /**
    * Worker-scoped execution context. It is set on the data-processing thread when an
    * executor is initialized, so that create() can stamp each object key with its owning
    * execution id without threading the id through every operator. A thread-local keeps
    * concurrent executions in the same JVM isolated, because each worker runs on its own
    * data-processing thread.
    */
  private val currentExecutionId: ThreadLocal[Option[Long]] =
    ThreadLocal.withInitial(() => Option.empty[Long])

  /** Sets the execution id for large binaries created on the current thread. */
  def setCurrentExecutionId(executionId: Long): Unit =
    currentExecutionId.set(Some(executionId))

  /**
    * Creates a new LargeBinary reference scoped to the current execution.
    * The actual data upload happens separately via LargeBinaryOutputStream.
    *
    * @return S3 URI string for the new LargeBinary (format: s3://bucket/objects/{eid}/{uuid})
    */
  def create(): String = {
    val eid = currentExecutionId
      .get()
      .getOrElse(
        throw new IllegalStateException(
          "LargeBinaryManager.create() requires an execution context, " +
            "but none was set on the current thread."
        )
      )
    val objectKey = s"objects/$eid/${UUID.randomUUID()}"
    s"s3://$DEFAULT_BUCKET/$objectKey"
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
    * testing so specs can exercise the swallow-and-log error path without a live
    * S3/MinIO endpoint.
    */
  private[util] def deleteByExecution(
      executionId: Long,
      deleteDir: (String, String) => Unit
  ): Unit = {
    try {
      deleteDir(DEFAULT_BUCKET, s"objects/$executionId")
      logger.info(
        s"Deleted large binaries for execution $executionId from bucket: $DEFAULT_BUCKET"
      )
    } catch {
      case e: Exception =>
        logger.warn(
          s"Failed to delete large binaries for execution $executionId " +
            s"from bucket: $DEFAULT_BUCKET",
          e
        )
    }
  }

}
