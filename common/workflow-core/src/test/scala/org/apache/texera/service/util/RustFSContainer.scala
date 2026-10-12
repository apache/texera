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

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

/**
  * Factory for the RustFS container used by the S3-backed test suites.
  *
  * testcontainers-scala ships a `MinIOContainer` module but no RustFS one, so this builds the
  * equivalent on top of `GenericContainer`. The differences from `MinIOContainer` that matter:
  *
  *   - credentials come from `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` rather than
  *     `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`;
  *   - readiness is `GET /health` on the S3 port, not `GET /minio/health/live`;
  *   - `RUSTFS_REGION` must match the region the AWS SDK signs with, because the region is part
  *     of the SigV4 credential scope.
  *
  * The console (9001) is left disabled: no test drives it, and starting it only widens the
  * surface the readiness check has to wait for.
  */
object RustFSContainer {

  /** Image pinned in lockstep with `bin/single-node/docker-compose.yml` and `bin/k8s/values.yaml`. */
  val ImageName: String = "rustfs/rustfs:1.0.0-rc.6"

  /** S3 API port inside the container. */
  val Port: Int = 9000

  val DefaultUser: String = "texera_rustfs"
  val DefaultPassword: String = "password"
  val DefaultRegion: String = "us-west-2"

  def apply(
      userName: String = DefaultUser,
      password: String = DefaultPassword,
      region: String = DefaultRegion
  ): GenericContainer = {
    val container = GenericContainer(
      dockerImage = ImageName,
      exposedPorts = Seq(Port),
      env = Map(
        "RUSTFS_ACCESS_KEY" -> userName,
        "RUSTFS_SECRET_KEY" -> password,
        "RUSTFS_REGION" -> region,
        "RUSTFS_VOLUMES" -> "/data",
        // Log to stdout so a failed start shows up in the testcontainers log consumer
        // instead of a file inside the container. `warn` keeps that readable.
        "RUSTFS_OBS_LOG_DIRECTORY" -> "",
        "RUSTFS_OBS_LOGGER_LEVEL" -> "warn"
      ),
      waitStrategy = Wait
        .forHttp("/health")
        .forPort(Port)
        .forStatusCode(200)
        .withStartupTimeout(Duration.ofMinutes(2))
    )
    container
  }
}
