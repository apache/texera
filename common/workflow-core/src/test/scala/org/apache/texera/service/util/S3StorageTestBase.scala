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

import com.dimafeng.testcontainers.MinIOContainer
import org.apache.texera.amber.config.StorageConfig
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.testcontainers.utility.DockerImageName

/**
  * Base trait for tests requiring S3 storage (MinIO).
  * Provides access to a single shared MinIO container across all test suites.
  *
  * Usage: Mix this trait into any test suite that needs S3 storage.
  */
trait S3StorageTestBase extends BeforeAndAfterAll { this: Suite =>

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Trigger lazy initialization of shared container
    S3StorageTestBase.ensureContainerStarted()
  }
}

object S3StorageTestBase {
  private lazy val container: MinIOContainer = {
    val c = MinIOContainer(
      dockerImageName = DockerImageName.parse("minio/minio:RELEASE.2025-02-28T09-55-16Z"),
      userName = "texera_minio",
      password = "password"
    )
    c.start()

    val endpoint = s"http://${c.host}:${c.mappedPort(9000)}"
    StorageConfig.s3Endpoint = endpoint

    println(s"[S3Storage] Started shared MinIO at $endpoint")

    sys.addShutdownHook {
      println("[S3Storage] Stopping shared MinIO...")
      c.stop()
    }

    c
  }

  /** Ensures the container is started (triggers lazy initialization). */
  def ensureContainerStarted(): Unit = {
    container // Access lazy val to trigger initialization
    ()
  }
}
