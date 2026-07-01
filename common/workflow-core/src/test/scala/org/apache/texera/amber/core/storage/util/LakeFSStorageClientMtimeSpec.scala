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

package org.apache.texera.amber.core.storage.util

import com.dimafeng.testcontainers.{
  ForAllTestContainer,
  GenericContainer,
  MinIOContainer,
  MultipleContainers,
  PostgreSQLContainer
}
import io.lakefs.clients.sdk.ApiException
import org.apache.texera.common.config.StorageConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, S3Exception}
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.charset.StandardCharsets

/**
  * Spec for [[LakeFSStorageClient.getStagedObjectMtime]].
  *
  * The method is a thin LakeFS-SDK passthrough (statObject -> mtime), so it can only be
  * exercised against a real LakeFS. This spins up the same Postgres + MinIO + LakeFS stack
  * the file-service tests use, with Postgres backing the LakeFS metadata store.
  */
class LakeFSStorageClientMtimeSpec
    extends AnyFlatSpec
    with Matchers
    with ForAllTestContainer
    with org.scalatest.BeforeAndAfterAll {

  // Shared network so LakeFS can reach Postgres and MinIO by their in-network aliases while
  // the test reaches LakeFS/MinIO via mapped host ports.
  private val network: Network = Network.newNetwork()

  private val minioUser = "texera_minio"
  private val minioPassword = "password"

  // Postgres metadata store for LakeFS. Using a real DB (rather than LakeFS's local/quickstart
  // KV) keeps setup explicit and deterministic: local mode auto-initializes on boot, which then
  // makes an explicit `lakefs setup` fail as "already initialized".
  private val postgres: PostgreSQLContainer = PostgreSQLContainer
    .Def(
      dockerImageName = DockerImageName.parse("postgres:15"),
      databaseName = "texera_lakefs",
      username = "texera_lakefs_admin",
      password = "password"
    )
    .createContainer()
  postgres.container.withNetwork(network)

  private val lakefsDatabaseURL: String =
    s"postgresql://${postgres.username}:${postgres.password}" +
      s"@${postgres.container.getNetworkAliases.get(0)}:5432/${postgres.databaseName}?sslmode=disable"

  private val minio: MinIOContainer = MinIOContainer(
    dockerImageName = DockerImageName.parse("minio/minio:RELEASE.2025-02-28T09-55-16Z"),
    userName = minioUser,
    password = minioPassword
  )
  minio.container.withNetwork(network)

  private val lakefs: GenericContainer = GenericContainer(
    dockerImage = "treeverse/lakefs:1.51",
    exposedPorts = Seq(8000),
    env = Map(
      "LAKEFS_DATABASE_TYPE" -> "postgres",
      "LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING" -> lakefsDatabaseURL,
      "LAKEFS_BLOCKSTORE_TYPE" -> "s3",
      "LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE" -> "true",
      "LAKEFS_BLOCKSTORE_S3_ENDPOINT" -> s"http://${minio.container.getNetworkAliases.get(0)}:9000",
      "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID" -> minioUser,
      "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY" -> minioPassword,
      "LAKEFS_AUTH_ENCRYPT_SECRET_KEY" -> "random_string_for_lakefs",
      "LAKEFS_INSTALLATION_USER_NAME" -> "texera-admin",
      "LAKEFS_INSTALLATION_ACCESS_KEY_ID" -> StorageConfig.lakefsUsername,
      "LAKEFS_INSTALLATION_SECRET_ACCESS_KEY" -> StorageConfig.lakefsPassword
    )
  )
  lakefs.container.withNetwork(network)

  override val container: MultipleContainers = MultipleContainers(postgres, minio, lakefs)

  private def minioEndpoint: String = s"http://${minio.host}:${minio.mappedPort(9000)}"
  private def lakefsApiBasePath: String = s"http://${lakefs.host}:${lakefs.mappedPort(8000)}/api/v1"

  override def afterStart(): Unit = {
    super.afterStart()

    // Initialize the LakeFS installation (creates the admin user matching the configured creds).
    val setup = lakefs.container.execInContainer(
      "lakefs",
      "setup",
      "--user-name",
      "texera-admin",
      "--access-key-id",
      StorageConfig.lakefsUsername,
      "--secret-access-key",
      StorageConfig.lakefsPassword
    )
    if (setup.getExitCode != 0) {
      throw new RuntimeException(s"Failed to set up LakeFS: ${setup.getStderr}")
    }

    // Point the JVM-wide singletons at the test containers BEFORE any LakeFSStorageClient call,
    // since its api client is a lazy val that captures the endpoint on first use.
    StorageConfig.s3Endpoint = minioEndpoint
    StorageConfig.lakefsEndpoint = lakefsApiBasePath

    // LakeFS needs its blockstore bucket to exist before any repo can be created.
    createLakefsBucket()
  }

  private def createLakefsBucket(): Unit = {
    val s3 = S3Client
      .builder()
      .endpointOverride(URI.create(minioEndpoint))
      .region(Region.US_WEST_2) // required by the builder; irrelevant for MinIO
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(minioUser, minioPassword))
      )
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .build()
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket(StorageConfig.lakefsBucketName).build())
    } catch {
      case _: S3Exception => // already exists / owned: fine
    } finally {
      s3.close()
    }
  }

  private def uniqueRepo(): String = s"lakefs-mtime-test-${System.nanoTime()}"

  private def stage(repoName: String, path: String, content: String = "staged-bytes"): Unit =
    LakeFSStorageClient.writeFileToRepo(
      repoName,
      path,
      new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
    )

  "getStagedObjectMtime" should "return the mtime of a freshly staged (uncommitted) object" in {
    val repoName = uniqueRepo()
    LakeFSStorageClient.initRepo(repoName)
    val path = "data/staged.bin"
    stage(repoName, path)

    val before = System.currentTimeMillis() / 1000 - 60
    val mtime = LakeFSStorageClient.getStagedObjectMtime(repoName, path)

    mtime should be > 0L
    // A just-uploaded object's mtime must be recent, not some default/epoch-zero value.
    mtime should be >= before
  }

  it should "throw a 404 ApiException for an object that does not exist" in {
    val repoName = uniqueRepo()
    LakeFSStorageClient.initRepo(repoName)

    val ex = intercept[ApiException] {
      LakeFSStorageClient.getStagedObjectMtime(repoName, "data/missing.bin")
    }
    ex.getCode shouldEqual 404
  }
}
