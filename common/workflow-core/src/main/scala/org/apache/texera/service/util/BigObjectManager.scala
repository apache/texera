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
  * Manages the lifecycle of BigObjects stored in S3.
  *
  * Handles creation and deletion of large objects that exceed
  * normal tuple size limits.
  */
object BigObjectManager extends LazyLogging {
  private val DEFAULT_BUCKET = "texera-big-objects"

  /**
    * Creates a new BigObject reference.
    * The actual data upload happens separately via BigObjectOutputStream.
    *
    * @return S3 URI string for the new BigObject (format: s3://bucket/key)
    */
  def create(): String = {
    S3StorageClient.createBucketIfNotExist(DEFAULT_BUCKET)

    val objectKey = s"objects/${System.currentTimeMillis()}/${UUID.randomUUID()}"
    val uri = s"s3://$DEFAULT_BUCKET/$objectKey"

    uri
  }

  /**
    * Deletes all big objects from the bucket.
    *
    * @throws Exception if the deletion fails
    * @return Unit
    */
  def deleteAllObjects(): Unit = {
    try {
      S3StorageClient.deleteDirectory(DEFAULT_BUCKET, "objects")
      logger.info(s"Successfully deleted all big objects from bucket: $DEFAULT_BUCKET")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to delete big objects from bucket: $DEFAULT_BUCKET", e)
    }
  }

}
