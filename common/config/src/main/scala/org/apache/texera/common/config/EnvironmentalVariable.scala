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

package org.apache.texera.common.config

// Names of the environment variables that Scala code refers to directly, for example to read one
// outside the *.conf files or to pass one to a child process or pod. The *.conf files read their
// variables by name through ${?NAME}, so a new config option needs a constant here only when code
// refers to the variable itself.
object EnvironmentalVariable {

  // utility function to load the env var
  def get(name: String): Option[String] = {
    Option(System.getenv(name))
  }

  /**
    * JVM related opts
    */
  val ENV_JAVA_OPTS = "JAVA_OPTS"

  /**
    * FileService related endpoint
    */
  val ENV_FILE_SERVICE_GET_DATASET_PRESIGNED_URL_ENDPOINT =
    "FILE_SERVICE_GET_DATASET_PRESIGNED_URL_ENDPOINT"
  val ENV_FILE_SERVICE_GET_MODEL_PRESIGNED_URL_ENDPOINT =
    "FILE_SERVICE_GET_MODEL_PRESIGNED_URL_ENDPOINT"
  val ENV_FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT =
    "FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT"

  /**
    * Auth related vars
    */
  val ENV_USER_JWT_TOKEN = "USER_JWT_TOKEN"
  val ENV_AUTH_JWT_SECRET = "AUTH_JWT_SECRET"

  // OpenTelemetry observability (see observability.conf)
  val ENV_OTEL_SDK_DISABLED = "OTEL_SDK_DISABLED"
  val ENV_OTEL_EXPORTER_OTLP_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT"
  val ENV_OTEL_RESOURCE_ATTRIBUTES = "OTEL_RESOURCE_ATTRIBUTES"
  val ENV_OTEL_METRIC_EXPORT_INTERVAL = "OTEL_METRIC_EXPORT_INTERVAL"
  val ENV_TEXERA_OTEL_ALLOWED_HOSTS = "TEXERA_OTEL_ALLOWED_HOSTS"

  /**
    * Dataset-mount vars injected into the CU pod. The mount is performed by the per-node
    * mounter and reaches the pod through mount propagation, so the pod only needs to know
    * which computing unit it is and where the propagated mount appears.
    *
    * The mounter's own address is deliberately NOT among these. Only an authenticated
    * platform caller may request a mount (see authenticate_caller in bin/mounter/mounter.py)
    * and a pod running untrusted user code holds no token for that audience, so the address
    * would serve no purpose here other than to probe the node's privileged mounter. A pod
    * that wants a mount asks the platform, which authorizes the (user, cuid) pair before
    * forwarding -- the cuid below is a claim to be checked, not a credential.
    */
  val ENV_CU_ID = "TEXERA_CU_ID"
  val ENV_MOUNT_IN_POD_ROOT = "TEXERA_MOUNT_IN_POD_ROOT"
  val ENV_ACCESS_CONTROL_SERVICE_URL = "ACCESS_CONTROL_SERVICE_URL"

  // JDBC
  val ENV_JDBC_URL = "STORAGE_JDBC_URL"
  val ENV_JDBC_USERNAME = "STORAGE_JDBC_USERNAME"
  val ENV_JDBC_PASSWORD = "STORAGE_JDBC_PASSWORD"

  // Iceberg Catalog
  val ENV_ICEBERG_CATALOG_TYPE = "STORAGE_ICEBERG_CATALOG_TYPE"
  val ENV_ICEBERG_CATALOG_REST_URI = "STORAGE_ICEBERG_CATALOG_REST_URI"
  val ENV_ICEBERG_CATALOG_REST_WAREHOUSE_NAME = "STORAGE_ICEBERG_CATALOG_REST_WAREHOUSE_NAME"

  // Iceberg Postgres Catalog
  val ENV_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME =
    "STORAGE_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME"
  val ENV_ICEBERG_CATALOG_POSTGRES_USERNAME = "STORAGE_ICEBERG_CATALOG_POSTGRES_USERNAME"
  val ENV_ICEBERG_CATALOG_POSTGRES_PASSWORD = "STORAGE_ICEBERG_CATALOG_POSTGRES_PASSWORD"

  // LakeFS
  val ENV_LAKEFS_ENDPOINT = "STORAGE_LAKEFS_ENDPOINT"

  // S3
  val ENV_S3_ENDPOINT = "STORAGE_S3_ENDPOINT"
  val ENV_S3_REGION = "STORAGE_S3_REGION"
  val ENV_S3_AUTH_USERNAME = "STORAGE_S3_AUTH_USERNAME"
  val ENV_S3_AUTH_PASSWORD = "STORAGE_S3_AUTH_PASSWORD"

  /**
    * Variables in application.conf that code also refers to by name
    */
  // Cache
  val ENV_CACHE_ENABLED = "CACHE_ENABLED"

  // User System
  val ENV_USER_SYS_ENABLED = "USER_SYS_ENABLED"

  // Web Server
  val ENV_MAX_WORKFLOW_WEBSOCKET_REQUEST_PAYLOAD_SIZE_KB =
    "MAX_WORKFLOW_WEBSOCKET_REQUEST_PAYLOAD_SIZE_KB"

  // Schedule Generator
  val ENV_SCHEDULE_GENERATOR_ENABLE_COST_BASED_SCHEDULE_GENERATOR =
    "SCHEDULE_GENERATOR_ENABLE_COST_BASED_SCHEDULE_GENERATOR"
}
