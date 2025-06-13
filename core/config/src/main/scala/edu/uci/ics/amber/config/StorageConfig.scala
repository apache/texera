/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.amber.config

import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.util.ConfigParserUtil.parseSizeStringToBytes
import edu.uci.ics.amber.util.PathUtils.corePath

import java.nio.file.Path

object StorageConfig {

  // Load configuration
  private val conf: Config = ConfigFactory.parseResources("storage.conf").resolve()

  // General storage settings
  val resultStorageMode: String = conf.getString("storage.result-storage-mode")

  // JDBC specifics
  val jdbcUrl: String = conf.getString("storage.jdbc.url")
  val jdbcUsername: String = conf.getString("storage.jdbc.username")
  val jdbcPassword: String = conf.getString("storage.jdbc.password")

  // Iceberg specifics
  val icebergCatalogType: String = conf.getString("storage.iceberg.catalog.type")
  val icebergRESTCatalogUri: String = conf.getString("storage.iceberg.catalog.rest-uri")

  // Iceberg Postgres specifics
  val icebergPostgresCatalogUriWithoutScheme: String =
    conf.getString("storage.iceberg.catalog.postgres.uri-without-scheme")
  val icebergPostgresCatalogUsername: String =
    conf.getString("storage.iceberg.catalog.postgres.username")
  val icebergPostgresCatalogPassword: String =
    conf.getString("storage.iceberg.catalog.postgres.password")

  // Iceberg Table specifics
  val icebergTableResultNamespace: String = conf.getString("storage.iceberg.table.result-namespace")
  val icebergTableConsoleMessagesNamespace: String =
    conf.getString("storage.iceberg.table.console-messages-namespace")
  val icebergTableRuntimeStatisticsNamespace: String =
    conf.getString("storage.iceberg.table.runtime-statistics-namespace")
  val icebergTableCommitBatchSize: Int =
    conf.getInt("storage.iceberg.table.commit.batch-size")
  val icebergTableCommitNumRetries: Int =
    conf.getInt("storage.iceberg.table.commit.retry.num-retries")
  val icebergTableCommitMinRetryWaitMs: Int =
    conf.getInt("storage.iceberg.table.commit.retry.min-wait-ms")
  val icebergTableCommitMaxRetryWaitMs: Int =
    conf.getInt("storage.iceberg.table.commit.retry.max-wait-ms")

  // LakeFS specifics
  val lakefsEndpoint: String = conf.getString("storage.lakefs.endpoint")
  val lakefsApiSecret: String = conf.getString("storage.lakefs.auth.api-secret")
  val lakefsUsername: String = conf.getString("storage.lakefs.auth.username")
  val lakefsPassword: String = conf.getString("storage.lakefs.auth.password")
  val lakefsBlockStorageType: String = conf.getString("storage.lakefs.block-storage.type")
  val lakefsBucketName: String = conf.getString("storage.lakefs.block-storage.bucket-name")

  // S3 specifics
  val s3Endpoint: String = conf.getString("storage.s3.endpoint")
  val s3Region: String = conf.getString("storage.s3.region")
  val s3Username: String = conf.getString("storage.s3.auth.username")
  val s3Password: String = conf.getString("storage.s3.auth.password")
  val s3MultipartUploadPartSize: Long = parseSizeStringToBytes(
    conf.getString("storage.s3.multipart.part-size")
  )

  // File storage configurations
  val fileStorageDirectoryPath: Path =
    corePath.resolve("amber").resolve("user-resources").resolve("workflow-results")

  // JDBC
  val ENV_JDBC_URL = "STORAGE_JDBC_URL"
  val ENV_JDBC_USERNAME = "STORAGE_JDBC_USERNAME"
  val ENV_JDBC_PASSWORD = "STORAGE_JDBC_PASSWORD"

  // Iceberg Catalog
  val ENV_ICEBERG_CATALOG_TYPE = "STORAGE_ICEBERG_CATALOG_TYPE"
  val ENV_ICEBERG_CATALOG_REST_URI = "STORAGE_ICEBERG_CATALOG_REST_URI"

  // Iceberg Postgres Catalog
  val ENV_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME =
    "STORAGE_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME"
  val ENV_ICEBERG_CATALOG_POSTGRES_USERNAME = "STORAGE_ICEBERG_CATALOG_POSTGRES_USERNAME"
  val ENV_ICEBERG_CATALOG_POSTGRES_PASSWORD = "STORAGE_ICEBERG_CATALOG_POSTGRES_PASSWORD"

  // Iceberg Table
  val ENV_ICEBERG_TABLE_RESULT_NAMESPACE = "STORAGE_ICEBERG_TABLE_RESULT_NAMESPACE"
  val ENV_ICEBERG_TABLE_CONSOLE_MESSAGES_NAMESPACE =
    "STORAGE_ICEBERG_TABLE_CONSOLE_MESSAGES_NAMESPACE"
  val ENV_ICEBERG_TABLE_RUNTIME_STATISTICS_NAMESPACE =
    "STORAGE_ICEBERG_TABLE_RUNTIME_STATISTICS_NAMESPACE"
  val ENV_ICEBERG_TABLE_COMMIT_BATCH_SIZE = "STORAGE_ICEBERG_TABLE_COMMIT_BATCH_SIZE"
  val ENV_ICEBERG_TABLE_COMMIT_NUM_RETRIES = "STORAGE_ICEBERG_TABLE_COMMIT_NUM_RETRIES"
  val ENV_ICEBERG_TABLE_COMMIT_MIN_WAIT_MS = "STORAGE_ICEBERG_TABLE_COMMIT_MIN_WAIT_MS"
  val ENV_ICEBERG_TABLE_COMMIT_MAX_WAIT_MS = "STORAGE_ICEBERG_TABLE_COMMIT_MAX_WAIT_MS"

  // LakeFS
  val ENV_LAKEFS_ENDPOINT = "STORAGE_LAKEFS_ENDPOINT"
  val ENV_LAKEFS_AUTH_API_SECRET = "STORAGE_LAKEFS_AUTH_API_SECRET"
  val ENV_LAKEFS_AUTH_USERNAME = "STORAGE_LAKEFS_AUTH_USERNAME"
  val ENV_LAKEFS_AUTH_PASSWORD = "STORAGE_LAKEFS_AUTH_PASSWORD"
  val ENV_LAKEFS_BLOCK_STORAGE_TYPE = "STORAGE_LAKEFS_BLOCK_STORAGE_TYPE"
  val ENV_LAKEFS_BLOCK_STORAGE_BUCKET_NAME = "STORAGE_LAKEFS_BLOCK_STORAGE_BUCKET_NAME"

  // S3
  val ENV_S3_ENDPOINT = "STORAGE_S3_ENDPOINT"
  val ENV_S3_REGION = "STORAGE_S3_REGION"
  val ENV_S3_AUTH_USERNAME = "STORAGE_S3_AUTH_USERNAME"
  val ENV_S3_AUTH_PASSWORD = "STORAGE_S3_AUTH_PASSWORD"
}
