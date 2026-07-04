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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[EnvironmentalVariable]], the registry of environment-variable *names* used across
  * the *.conf files. Referencing every constant forces the whole object to initialize, so a
  * deleted or renamed constant surfaces here; asserting distinctness guards against two settings
  * accidentally sharing one env-var name.
  */
class EnvironmentalVariableSpec extends AnyFlatSpec with Matchers {

  import EnvironmentalVariable._

  private val allNames: Seq[String] = Seq(
    ENV_JAVA_OPTS,
    ENV_FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT,
    ENV_FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT,
    ENV_USER_JWT_TOKEN,
    ENV_AUTH_JWT_SECRET,
    ENV_JDBC_URL,
    ENV_JDBC_USERNAME,
    ENV_JDBC_PASSWORD,
    ENV_ICEBERG_CATALOG_TYPE,
    ENV_ICEBERG_CATALOG_REST_URI,
    ENV_ICEBERG_CATALOG_REST_WAREHOUSE_NAME,
    ENV_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME,
    ENV_ICEBERG_CATALOG_POSTGRES_USERNAME,
    ENV_ICEBERG_CATALOG_POSTGRES_PASSWORD,
    ENV_ICEBERG_TABLE_RESULT_NAMESPACE,
    ENV_ICEBERG_TABLE_CONSOLE_MESSAGES_NAMESPACE,
    ENV_ICEBERG_TABLE_RUNTIME_STATISTICS_NAMESPACE,
    ENV_ICEBERG_TABLE_STATE_NAMESPACE,
    ENV_ICEBERG_TABLE_COMMIT_BATCH_SIZE,
    ENV_ICEBERG_TABLE_COMMIT_NUM_RETRIES,
    ENV_ICEBERG_TABLE_COMMIT_MIN_WAIT_MS,
    ENV_ICEBERG_TABLE_COMMIT_MAX_WAIT_MS,
    ENV_LAKEFS_ENDPOINT,
    ENV_LAKEFS_AUTH_API_SECRET,
    ENV_LAKEFS_AUTH_USERNAME,
    ENV_LAKEFS_AUTH_PASSWORD,
    ENV_LAKEFS_BLOCK_STORAGE_TYPE,
    ENV_LAKEFS_BLOCK_STORAGE_BUCKET_NAME,
    ENV_S3_ENDPOINT,
    ENV_S3_REGION,
    ENV_S3_AUTH_USERNAME,
    ENV_S3_AUTH_PASSWORD,
    ENV_CONSTANTS_LOGGING_QUEUE_SIZE_INTERVAL,
    ENV_CONSTANTS_NUM_WORKER_PER_OPERATOR,
    ENV_CONSTANTS_MAX_RESOLUTION_ROWS,
    ENV_CONSTANTS_MAX_RESOLUTION_COLUMNS,
    ENV_CONSTANTS_STATUS_UPDATE_INTERVAL,
    ENV_FLOW_CONTROL_MAX_CREDIT_ALLOWED_IN_BYTES_PER_CHANNEL,
    ENV_FLOW_CONTROL_CREDIT_POLL_INTERVAL_IN_MS,
    ENV_NETWORK_BUFFERING_DEFAULT_DATA_TRANSFER_BATCH_SIZE,
    ENV_NETWORK_BUFFERING_ENABLE_ADAPTIVE_BUFFERING,
    ENV_NETWORK_BUFFERING_ADAPTIVE_BUFFERING_TIMEOUT_MS,
    ENV_RECONFIGURATION_ENABLE_TRANSACTIONAL_RECONFIGURATION,
    ENV_CACHE_ENABLED,
    ENV_USER_SYS_ENABLED,
    ENV_USER_SYS_GOOGLE_CLIENT_ID,
    ENV_USER_SYS_GOOGLE_SMTP_GMAIL,
    ENV_USER_SYS_GOOGLE_SMTP_PASSWORD,
    ENV_USER_SYS_VERSION_TIME_LIMIT_IN_MINUTES,
    ENV_RESULT_CLEANUP_TTL_IN_SECONDS,
    ENV_RESULT_CLEANUP_COLLECTION_CHECK_INTERVAL_IN_SECONDS,
    ENV_WEB_SERVER_WORKFLOW_STATE_CLEANUP_IN_SECONDS,
    ENV_WEB_SERVER_PYTHON_CONSOLE_BUFFER_SIZE,
    ENV_WEB_SERVER_WORKFLOW_RESULT_PULLING_IN_SECONDS,
    ENV_WEB_SERVER_CLEAN_ALL_EXECUTION_RESULTS_ON_SERVER_START,
    ENV_MAX_WORKFLOW_WEBSOCKET_REQUEST_PAYLOAD_SIZE_KB,
    ENV_FAULT_TOLERANCE_LOG_STORAGE_URI,
    ENV_FAULT_TOLERANCE_LOG_FLUSH_INTERVAL_MS,
    ENV_FAULT_TOLERANCE_LOG_RECORD_MAX_SIZE_IN_BYTE,
    ENV_FAULT_TOLERANCE_MAX_SUPPORTED_RESEND_QUEUE_LENGTH,
    ENV_FAULT_TOLERANCE_DELAY_BEFORE_RECOVERY,
    ENV_FAULT_TOLERANCE_HDFS_STORAGE_ADDRESS,
    ENV_SCHEDULE_GENERATOR_ENABLE_COST_BASED_SCHEDULE_GENERATOR,
    ENV_SCHEDULE_GENERATOR_MAX_CONCURRENT_REGIONS,
    ENV_SCHEDULE_GENERATOR_USE_GLOBAL_SEARCH,
    ENV_SCHEDULE_GENERATOR_USE_TOP_DOWN_SEARCH,
    ENV_SCHEDULE_GENERATOR_SEARCH_TIMEOUT_MILLISECONDS,
    ENV_AI_ASSISTANT_SERVER_ASSISTANT,
    ENV_AI_ASSISTANT_SERVER_AI_SERVICE_KEY,
    ENV_AI_ASSISTANT_SERVER_AI_SERVICE_URL
  )

  "EnvironmentalVariable" should "expose non-empty, unique env-var names" in {
    allNames.foreach(name => name.trim should not be empty)
    allNames.distinct.size shouldBe allNames.size
  }

  it should "use the documented names for a representative sample" in {
    ENV_JAVA_OPTS shouldBe "JAVA_OPTS"
    ENV_AUTH_JWT_SECRET shouldBe "AUTH_JWT_SECRET"
    ENV_JDBC_URL shouldBe "STORAGE_JDBC_URL"
    ENV_S3_ENDPOINT shouldBe "STORAGE_S3_ENDPOINT"
    ENV_CACHE_ENABLED shouldBe "CACHE_ENABLED"
  }

  "EnvironmentalVariable.get" should "return None for an unset variable" in {
    val absent = "TEXERA_DEFINITELY_UNSET_" + System.nanoTime().toString
    EnvironmentalVariable.get(absent) shouldBe None
  }

  it should "return Some(value) for a variable that is set in the JVM environment" in {
    val env = System.getenv()
    if (!env.isEmpty) {
      val entry = env.entrySet().iterator().next()
      EnvironmentalVariable.get(entry.getKey) shouldBe Some(entry.getValue)
    }
  }
}
