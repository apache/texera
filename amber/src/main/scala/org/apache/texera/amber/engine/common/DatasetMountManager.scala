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

package org.apache.texera.amber.engine.common

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.common.config.EnvironmentalVariable

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import scala.io.Source
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Using

/**
  * Lazily FUSE-mounts dataset versions into the computing unit's local file system
  * using GeeseFS. A dataset version is addressed by a locator
  * "<repositoryName>:<commitHash>"; since a commit is immutable, a mount is created at
  * most once per (repository, commit) for the lifetime of the pod and reused by every
  * subsequent worker/execution.
  *
  * The mount is authorized with the pod's per-user JWT, not with global LakeFS
  * credentials (a computing-unit pod is user-accessible, so it must never hold the
  * global LakeFS identity). GeeseFS talks to file-service's JWT-authenticated S3 proxy:
  * the JWT already present in the pod is passed to GeeseFS as its S3 access key, so it
  * rides in every request's signature and the proxy re-signs to the LakeFS S3 gateway
  * with the global credentials held only server-side. No separate mount credential is
  * issued or stored.
  *
  * Requirements inside the container: the `geesefs` binary (with the CAP_SYS_ADMIN
  * file capability when running as a non-root user), `fuse3`, and access to /dev/fuse
  * (the computing-unit pod is created privileged for dataset mounting).
  */
object DatasetMountManager extends LazyLogging {

  private val mountRoot: Path =
    Paths.get(sys.env.getOrElse("TEXERA_DATASET_MOUNT_ROOT", "/tmp/texera-dataset-mounts"))

  private val geesefsBinary: String = sys.env.getOrElse("TEXERA_GEESEFS_PATH", "geesefs")

  private val mountTimeoutMs = 30000L

  // GeeseFS requires a non-empty S3 secret key to produce a signature, but file-service's
  // proxy treats the JWT (the access key) as the bearer capability and never validates the
  // pod-side signature, so any fixed placeholder secret works.
  private val mountSecretPlaceholder = "texera-jwt-mount"

  private def userJwtToken: String =
    sys.env.getOrElse(EnvironmentalVariable.ENV_USER_JWT_TOKEN, "").trim

  /**
    * Base URL of file-service as reachable from the pod (scheme://authority), derived
    * from the presigned-URL endpoint the pod already receives. GeeseFS mounts against the
    * S3 proxy hosted at its root.
    */
  private def fileServiceBaseUrl: String = {
    val presignEndpoint = sys.env.getOrElse(
      EnvironmentalVariable.ENV_FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT,
      "http://localhost:9092/api/dataset/presign-download"
    )
    val uri = new URI(presignEndpoint)
    s"${uri.getScheme}://${uri.getAuthority}"
  }

  /**
    * Ensure the dataset version identified by the locator "<repositoryName>:<commitHash>"
    * is mounted, and return the local mount point. Thread-safe and idempotent.
    */
  def ensureMounted(locator: String): Path =
    synchronized {
      val (repositoryName, commitHash) = locator.split(":", 2) match {
        case Array(repo, commit) if repo.nonEmpty && commit.nonEmpty => (repo, commit)
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid dataset mount locator '$locator'; expected <repositoryName>:<commitHash>."
          )
      }

      val mountPoint = mountRoot.resolve(repositoryName).resolve(commitHash)
      if (isMounted(mountPoint)) {
        logger.info(s"Dataset $locator already mounted at $mountPoint")
        return mountPoint
      }

      val token = userJwtToken
      if (token.isEmpty) {
        throw new RuntimeException(
          s"No ${EnvironmentalVariable.ENV_USER_JWT_TOKEN} present in the computing unit; " +
            "cannot authorize a dataset mount without a user JWT."
        )
      }

      // Authorize the mount with the pod's JWT and mount through file-service's S3 proxy;
      // no global LakeFS credentials are ever placed in this (user-accessible) pod. The
      // JWT is passed as the S3 access key so the proxy can read it back out and verify it.
      val proxyEndpoint = fileServiceBaseUrl

      Files.createDirectories(mountPoint)
      val command = Seq(
        geesefsBinary,
        "--endpoint",
        proxyEndpoint,
        "--memory-limit",
        "512",
        "-o",
        "ro",
        s"$repositoryName:$commitHash",
        mountPoint.toString
      )
      logger.info(s"Mounting dataset $locator at $mountPoint via: ${command.mkString(" ")}")

      val output = new StringBuilder
      val exitCode = Process(
        command,
        None,
        "AWS_ACCESS_KEY_ID" -> token,
        "AWS_SECRET_ACCESS_KEY" -> mountSecretPlaceholder
      ).!(ProcessLogger(line => {
        output.append(line).append('\n')
        logger.info(s"geesefs: $line")
      }))

      if (exitCode != 0) {
        throw new RuntimeException(
          s"geesefs failed to mount dataset $locator (exit code $exitCode): ${output.toString.trim}"
        )
      }

      // geesefs daemonizes after a successful mount; wait until the kernel reports it.
      val deadline = System.currentTimeMillis() + mountTimeoutMs
      while (!isMounted(mountPoint)) {
        if (System.currentTimeMillis() > deadline) {
          throw new RuntimeException(
            s"Dataset $locator did not appear as a mount at $mountPoint within ${mountTimeoutMs}ms."
          )
        }
        Thread.sleep(200)
      }
      logger.info(s"Dataset $locator mounted at $mountPoint")
      mountPoint
    }

  private def isMounted(mountPoint: Path): Boolean = {
    if (!Files.exists(mountPoint)) {
      return false
    }
    val target = mountPoint.toAbsolutePath.toString
    Using(Source.fromFile("/proc/mounts")) { source =>
      source
        .getLines()
        .exists(line => {
          val fields = line.split(" ")
          fields.length > 2 && fields(1) == target && fields(2).startsWith("fuse")
        })
    }.getOrElse(false)
  }
}
