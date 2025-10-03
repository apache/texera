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

package edu.uci.ics.texera.service

import com.dimafeng.testcontainers.{
  ForAllTestContainer,
  GenericContainer,
  PostgreSQLContainer,
  MinIOContainer,
  MultipleContainers
}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import edu.uci.ics.amber.config.StorageConfig
import edu.uci.ics.texera.service.util.S3StorageClient

/**
  * Trait to spin up a LakeFS + MinIO + Postgres stack using Testcontainers,
  * similar to how MockTexeraDB uses EmbeddedPostgres.
  */
trait MockLakeFS extends ForAllTestContainer with BeforeAndAfterAll { self: Suite =>
  // network for containers to communicate
  val network: Network = Network.newNetwork()

  // Postgres for LakeFS metadata
  val postgres: PostgreSQLContainer = PostgreSQLContainer
    .Def(
      dockerImageName = DockerImageName.parse("postgres:15"),
      databaseName = "texera_lakefs",
      username = "texera_lakefs_admin",
      password = "password"
    )
    .createContainer()
  postgres.container.withNetwork(network)

  // MinIO for object storage
  val minio = MinIOContainer(
    dockerImageName = DockerImageName.parse("minio/minio:RELEASE.2025-02-28T09-55-16Z"),
    userName = "texera_minio",
    password = "password"
  )
  minio.container.withNetwork(network)

  // LakeFS
  val lakefsDatabaseURL: String =
    s"postgresql://${postgres.username}:${postgres.password}" +
      s"@${postgres.container.getNetworkAliases.get(0)}:5432/${postgres.databaseName}" +
      s"?sslmode=disable"
  val lakefsUsername = "texera-admin"
  val lakefsAccessKeyID = "AKIAIOSFOLKFSSAMPLES"
  val lakefsSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  val lakefs = GenericContainer(
    dockerImage = "treeverse/lakefs:1.51",
    exposedPorts = Seq(8000),
    env = Map(
      "LAKEFS_BLOCKSTORE_TYPE" -> "s3",
      "LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE" -> "true",
      "LAKEFS_BLOCKSTORE_S3_ENDPOINT" -> s"http://${minio.container.getNetworkAliases.get(0)}:9000",
      "LAKEFS_BLOCKSTORE_S3_PRE_SIGNED_ENDPOINT" -> "http://localhost:9000",
      "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID" -> "texera_minio",
      "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY" -> "password",
      "LAKEFS_AUTH_ENCRYPT_SECRET_KEY" -> "random_string_for_lakefs",
      "LAKEFS_LOGGING_LEVEL" -> "INFO",
      "LAKEFS_STATS_ENABLED" -> "1",
      "LAKEFS_DATABASE_TYPE" -> "postgres",
      "LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING" -> lakefsDatabaseURL,
      "LAKEFS_INSTALLATION_USER_NAME" -> lakefsUsername,
      "LAKEFS_INSTALLATION_ACCESS_KEY_ID" -> lakefsAccessKeyID,
      "LAKEFS_INSTALLATION_SECRET_ACCESS_KEY" -> lakefsSecretAccessKey
    )
  )
  lakefs.container.withNetwork(network)

  override val container = MultipleContainers(postgres, minio, lakefs)

  def lakefsBaseUrl: String = s"http://${lakefs.host}:${lakefs.mappedPort(8000)}"
  def minioEndpoint: String = s"http://${minio.host}:${minio.mappedPort(9000)}"

  override def afterStart(): Unit = {
    super.afterStart()

    // setup LakeFS
    val lakefsSetupResult = lakefs.container.execInContainer(
      "lakefs",
      "setup",
      "--user-name",
      lakefsUsername,
      "--access-key-id",
      lakefsAccessKeyID,
      "--secret-access-key",
      lakefsSecretAccessKey
    )
    if (lakefsSetupResult.getExitCode != 0) {
      throw new RuntimeException(
        s"Failed to setup LakeFS: ${lakefsSetupResult.getStderr}"
      )
    }

    // replace storage endpoints in StorageConfig
    StorageConfig.s3Endpoint = minioEndpoint
    StorageConfig.lakefsEndpoint = s"$lakefsBaseUrl/api/v1"

    // create S3 bucket
    S3StorageClient.createBucketIfNotExist(StorageConfig.lakefsBucketName)
  }
}
