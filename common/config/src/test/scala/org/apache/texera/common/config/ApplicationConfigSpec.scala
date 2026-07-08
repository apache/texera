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
  * Spec for [[ApplicationConfig]]. Reading each value forces resolution from application.conf, so
  * a renamed or mistyped key surfaces here as a ConfigException instead of at service start-up.
  * Every key carries a `${?ENV_VAR}` override, so exact-value assertions that could be overridden
  * are guarded on the env var being unset (mirroring StorageConfigSpec).
  */
class ApplicationConfigSpec extends AnyFlatSpec with Matchers {

  // Each key carries a `${?ENV}` override, and application.conf falls back to
  // ConfigFactory.load() (which also layers JVM system properties). Guard every
  // exact-value assertion on the override being absent from both sources.
  private def ifUnset(name: String)(assertion: => Any): Unit =
    if (!sys.env.contains(name) && !sys.props.contains(name)) assertion

  "ApplicationConfig constants" should "load the default constant values" in {
    ifUnset("CONSTANTS_LOGGING_QUEUE_SIZE_INTERVAL")(
      ApplicationConfig.loggingQueueSizeInterval shouldBe 30000
    )
    ifUnset("CONSTANTS_MAX_RESOLUTION_ROWS")(ApplicationConfig.MAX_RESOLUTION_ROWS shouldBe 2000)
    ifUnset("CONSTANTS_MAX_RESOLUTION_COLUMNS")(
      ApplicationConfig.MAX_RESOLUTION_COLUMNS shouldBe 2000
    )
    ifUnset("CONSTANTS_NUM_WORKER_PER_OPERATOR")(
      ApplicationConfig.numWorkerPerOperatorByDefault shouldBe 2
    )
    ifUnset("CONSTANTS_STATUS_UPDATE_INTERVAL")(
      ApplicationConfig.getStatusUpdateIntervalInMs shouldBe 500L
    )
    ifUnset("CONSTANTS_RUNTIME_STATISTICS_PERSISTENCE_INTERVAL")(
      ApplicationConfig.getRuntimeStatisticsPersistenceIntervalInMs shouldBe 2000L
    )
  }

  "ApplicationConfig flow control" should "load credit and polling defaults" in {
    ifUnset("FLOW_CONTROL_MAX_CREDIT_ALLOWED_IN_BYTES_PER_CHANNEL")(
      ApplicationConfig.maxCreditAllowedInBytesPerChannel shouldBe 1600000000L
    )
    ifUnset("FLOW_CONTROL_CREDIT_POLL_INTERVAL_IN_MS")(
      ApplicationConfig.creditPollingIntervalInMs shouldBe 200
    )
  }

  "ApplicationConfig network buffering" should "load batch size and adaptive buffering defaults" in {
    ifUnset("NETWORK_BUFFERING_DEFAULT_DATA_TRANSFER_BATCH_SIZE")(
      ApplicationConfig.defaultDataTransferBatchSize shouldBe 400
    )
    ifUnset("NETWORK_BUFFERING_ENABLE_ADAPTIVE_BUFFERING")(
      ApplicationConfig.enableAdaptiveNetworkBuffering shouldBe true
    )
    ifUnset("NETWORK_BUFFERING_ADAPTIVE_BUFFERING_TIMEOUT_MS")(
      ApplicationConfig.adaptiveBufferingTimeoutMs shouldBe 500
    )
  }

  "ApplicationConfig reconfiguration" should "default transactional reconfiguration to false" in {
    ifUnset("RECONFIGURATION_ENABLE_TRANSACTIONAL_RECONFIGURATION")(
      ApplicationConfig.enableTransactionalReconfiguration shouldBe false
    )
  }

  "ApplicationConfig fault tolerance" should "disable logging with an empty log-storage-uri" in {
    ifUnset("FAULT_TOLERANCE_LOG_FLUSH_INTERVAL_MS")(
      ApplicationConfig.faultToleranceLogFlushIntervalInMs shouldBe 0L
    )
    ifUnset("FAULT_TOLERANCE_LOG_STORAGE_URI") {
      ApplicationConfig.faultToleranceLogRootFolder shouldBe None
      ApplicationConfig.isFaultToleranceEnabled shouldBe false
    }
  }

  "ApplicationConfig scheduling" should "load schedule-generator defaults" in {
    ifUnset("SCHEDULE_GENERATOR_MAX_CONCURRENT_REGIONS")(
      ApplicationConfig.maxConcurrentRegions shouldBe 1
    )
    ifUnset("SCHEDULE_GENERATOR_USE_GLOBAL_SEARCH")(
      ApplicationConfig.useGlobalSearch shouldBe false
    )
    ifUnset("SCHEDULE_GENERATOR_USE_TOP_DOWN_SEARCH")(
      ApplicationConfig.useTopDownSearch shouldBe false
    )
    ifUnset("SCHEDULE_GENERATOR_SEARCH_TIMEOUT_MILLISECONDS")(
      ApplicationConfig.searchTimeoutMilliseconds shouldBe 1000
    )
  }

  "ApplicationConfig storage cleanup" should "load result-cleanup TTL and interval defaults" in {
    ifUnset("RESULT_CLEANUP_TTL_IN_SECONDS")(ApplicationConfig.sinkStorageTTLInSecs shouldBe 86400)
    ifUnset("RESULT_CLEANUP_COLLECTION_CHECK_INTERVAL_IN_SECONDS")(
      ApplicationConfig.sinkStorageCleanUpCheckIntervalInSecs shouldBe 86400
    )
  }

  "ApplicationConfig web server" should "load web-server defaults" in {
    ifUnset("WEB_SERVER_PYTHON_CONSOLE_BUFFER_SIZE")(
      ApplicationConfig.operatorConsoleBufferSize shouldBe 100
    )
    ifUnset("WEB_SERVER_CONSOLE_MESSAGE_MAX_DISPLAY_LENGTH")(
      ApplicationConfig.consoleMessageDisplayLength shouldBe 100
    )
    ifUnset("WEB_SERVER_WORKFLOW_RESULT_PULLING_IN_SECONDS")(
      ApplicationConfig.executionResultPollingInSecs shouldBe 3
    )
    ifUnset("WEB_SERVER_WORKFLOW_STATE_CLEANUP_IN_SECONDS")(
      ApplicationConfig.executionStateCleanUpInSecs shouldBe 30
    )
    ifUnset("WEB_SERVER_CLEAN_ALL_EXECUTION_RESULTS_ON_SERVER_START")(
      ApplicationConfig.cleanupAllExecutionResults shouldBe false
    )
    ifUnset("MAX_WORKFLOW_WEBSOCKET_REQUEST_PAYLOAD_SIZE_KB")(
      ApplicationConfig.maxWorkflowWebsocketRequestPayloadSizeKb shouldBe 64
    )
  }

  "ApplicationConfig AI assistant" should "expose the ai-assistant-server config block" in {
    ApplicationConfig.aiAssistantConfig should not be empty
    ifUnset("AI_ASSISTANT_SERVER_ASSISTANT")(
      ApplicationConfig.aiAssistantConfig.get.getString("assistant") shouldBe "none"
    )
  }
}
