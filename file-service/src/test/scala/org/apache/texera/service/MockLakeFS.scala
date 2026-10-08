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

package org.apache.texera.service

import com.dimafeng.testcontainers._
import io.lakefs.clients.sdk.{ApiClient, RepositoriesApi}
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.service.util.{RustFSContainer, S3StorageClient}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration

import java.net.URI

/**
  * Trait to spin up a LakeFS + RustFS + Postgres stack using Testcontainers,
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

  // LakeFS bakes the pre-signed endpoint into its env before containers start,
  // so the object store cannot use a dynamically mapped host port: presigned URLs
  // must be reachable from the host at an address known ahead of time. Reserve a
  // free host port and pin the S3 port to it.
  val objectStoreHostPort: Int = {
    val socket = new java.net.ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }

  // RustFS for object storage
  val objectStore: GenericContainer = RustFSContainer()
  objectStore.container.withNetwork(network)
  objectStore.container.withCreateContainerCmdModifier { cmd =>
    import com.github.dockerjava.api.model.{ExposedPort, PortBinding, Ports}
    // RustFSContainer exposes only the S3 port, so pinning it is the whole
    // binding set; the console is not started.
    cmd.getHostConfig.withPortBindings(
      new PortBinding(
        Ports.Binding.bindPort(objectStoreHostPort),
        ExposedPort.tcp(RustFSContainer.Port)
      )
    )
  }

  // LakeFS
  val lakefsDatabaseURL: String =
    s"postgresql://${postgres.username}:${postgres.password}" +
      s"@${postgres.container.getNetworkAliases.get(0)}:5432/${postgres.databaseName}" +
      s"?sslmode=disable"

  val lakefsUsername = "texera-admin"

  // These are the API credentials created/used during setup.
  // In lakeFS, the access key + secret key are used as basic-auth username/password for the API.
  val lakefsAccessKeyID = "AKIAIOSFOLKFSSAMPLES"
  val lakefsSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

  val lakefs = GenericContainer(
    dockerImage = "treeverse/lakefs:1.51",
    exposedPorts = Seq(8000),
    env = Map(
      "LAKEFS_BLOCKSTORE_TYPE" -> "s3",
      "LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE" -> "true",
      "LAKEFS_BLOCKSTORE_S3_ENDPOINT" -> s"http://${objectStore.container.getNetworkAliases
        .get(0)}:${RustFSContainer.Port}",
      "LAKEFS_BLOCKSTORE_S3_PRE_SIGNED_ENDPOINT" -> s"http://localhost:$objectStoreHostPort",
      "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID" -> RustFSContainer.DefaultUser,
      "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY" -> RustFSContainer.DefaultPassword,
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

  override val container = MultipleContainers(postgres, objectStore, lakefs)

  def lakefsBaseUrl: String = s"http://${lakefs.host}:${lakefs.mappedPort(8000)}"
  def objectStoreEndpoint: String =
    s"http://${objectStore.host}:${objectStore.mappedPort(RustFSContainer.Port)}"
  def lakefsApiBasePath: String = s"$lakefsBaseUrl/api/v1"

  // ---- Clients (lazy so they initialize after containers are started) ----

  lazy val lakefsApiClient: ApiClient = {
    val apiClient = new ApiClient()
    apiClient.setBasePath(lakefsApiBasePath)
    // basic-auth for lakeFS API uses accessKey as username, secretKey as password
    apiClient.setUsername(lakefsAccessKeyID)
    apiClient.setPassword(lakefsSecretAccessKey)
    apiClient
  }

  lazy val repositoriesApi: RepositoriesApi = new RepositoriesApi(lakefsApiClient)

  /**
    * S3 client instance for testing pointed at RustFS.
    *
    * Notes:
    * - The region MUST match RUSTFS_REGION: it is part of the SigV4 credential scope.
    * - Path-style is important: http://host:port/bucket/key
    */
  lazy val s3Client: S3Client = {
    //Temporal credentials for testing purposes only
    val creds =
      AwsBasicCredentials.create(RustFSContainer.DefaultUser, RustFSContainer.DefaultPassword)
    S3Client
      .builder()
      .endpointOverride(URI.create(StorageConfig.s3Endpoint)) // set in afterStart()
      .region(Region.US_WEST_2) // must match RustFSContainer's RUSTFS_REGION
      .credentialsProvider(StaticCredentialsProvider.create(creds))
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .build()
  }

  override def afterStart(): Unit = {
    super.afterStart()

    // setup LakeFS (idempotent-ish, but will fail if it truly cannot run)
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
      throw new RuntimeException(s"Failed to setup LakeFS: ${lakefsSetupResult.getStderr}")
    }

    // replace storage endpoints in StorageConfig
    StorageConfig.s3Endpoint = objectStoreEndpoint
    StorageConfig.lakefsEndpoint = lakefsApiBasePath

    // create S3 bucket used by lakeFS in tests
    S3StorageClient.createBucketIfNotExist(StorageConfig.lakefsBucketName)
  }
}
