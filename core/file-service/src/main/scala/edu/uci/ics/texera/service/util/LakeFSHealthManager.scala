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

package edu.uci.ics.texera.service.util

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.util.LakeFSStorageClient

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import org.slf4j.LoggerFactory

class LakeFSHealthManager(intervalSeconds: Int) extends io.dropwizard.lifecycle.Managed {
  private val logger = LoggerFactory.getLogger(getClass)
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  @volatile private var bucketCreated = false

  override def start(): Unit = {
    scheduler.scheduleAtFixedRate(
      () => {
        if (!bucketCreated) {
          try {
            S3StorageClient.createBucketIfNotExist(StorageConfig.lakefsBucketName)
            logger.info("S3 bucket created successfully.")
            bucketCreated = true
          } catch {
            case e: Exception =>
              logger.warn("Periodic bucket creation failed", e)
          }
        }

        try {
          LakeFSStorageClient.healthCheck()
        } catch {
          case e: Exception =>
            logger.warn("LakeFS health check failed", e)
        }
      },
      0,
      intervalSeconds,
      TimeUnit.SECONDS
    )
  }

  override def stop(): Unit = {
    scheduler.shutdownNow()
  }
}
