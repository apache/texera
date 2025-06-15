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

import java.io.File
import java.net.URI

object ApplicationConfig {

  private val configFile: File = new File("src/main/resources/application.conf")
  private var lastModifiedTime: Long = 0
  private var conf: Config = ConfigFactory.load()

  // Perform lazy reload
  private def getConfSource: Config = {
    if (lastModifiedTime == configFile.lastModified()) {
      conf.resolve()
    } else {
      lastModifiedTime = configFile.lastModified()
      conf = ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.load())
      conf.resolve()
    }
  }

  // Constants
  val loggingQueueSizeInterval: Int = getConfSource.getInt("constants.logging-queue-size-interval")
  val MAX_RESOLUTION_ROWS: Int = getConfSource.getInt("constants.max-resolution-rows")
  val MAX_RESOLUTION_COLUMNS: Int = getConfSource.getInt("constants.max-resolution-columns")
  val numWorkerPerOperatorByDefault: Int = getConfSource.getInt("constants.num-worker-per-operator")
  val getStatusUpdateIntervalInMs: Long = getConfSource.getLong("constants.status-update-interval")

  // Flow control
  val maxCreditAllowedInBytesPerChannel: Long =
    getConfSource.getLong("flow-control.max-credit-allowed-in-bytes-per-channel") match {
      case -1L       => Long.MaxValue
      case maxCredit => maxCredit
    }

  val creditPollingIntervalInMs: Int =
    getConfSource.getInt("flow-control.credit-poll-interval-in-ms")

  // Network buffering
  val defaultDataTransferBatchSize: Int =
    getConfSource.getInt("network-buffering.default-data-transfer-batch-size")
  val enableAdaptiveNetworkBuffering: Boolean =
    getConfSource.getBoolean("network-buffering.enable-adaptive-buffering")
  val adaptiveBufferingTimeoutMs: Int =
    getConfSource.getInt("network-buffering.adaptive-buffering-timeout-ms")

  // Reconfiguration
  val enableTransactionalReconfiguration: Boolean =
    getConfSource.getBoolean("reconfiguration.enable-transactional-reconfiguration")

  // Fault tolerance
  val faultToleranceLogFlushIntervalInMs: Long =
    getConfSource.getLong("fault-tolerance.log-flush-interval-ms")

  val faultToleranceLogRootFolder: Option[URI] = {
    Option(getConfSource.getString("fault-tolerance.log-storage-uri"))
      .filter(_.nonEmpty)
      .map(new URI(_))
  }

  val isFaultToleranceEnabled: Boolean = faultToleranceLogRootFolder.nonEmpty

  // Scheduling
  val enableCostBasedScheduleGenerator: Boolean =
    getConfSource.getBoolean("schedule-generator.enable-cost-based-schedule-generator")
  val useGlobalSearch: Boolean = getConfSource.getBoolean("schedule-generator.use-global-search")
  val useTopDownSearch: Boolean = getConfSource.getBoolean("schedule-generator.use-top-down-search")
  val searchTimeoutMilliseconds: Int =
    getConfSource.getInt("schedule-generator.search-timeout-milliseconds")

  // Storage cleanup
  val sinkStorageTTLInSecs: Int = getConfSource.getInt("result-cleanup.ttl-in-seconds")
  val sinkStorageCleanUpCheckIntervalInSecs: Int =
    getConfSource.getInt("result-cleanup.collection-check-interval-in-seconds")

  // Web server
  val operatorConsoleBufferSize: Int = getConfSource.getInt("web-server.python-console-buffer-size")
  val consoleMessageDisplayLength: Int =
    getConfSource.getInt("web-server.console-message-max-display-length")
  val executionResultPollingInSecs: Int =
    getConfSource.getInt("web-server.workflow-result-pulling-in-seconds")
  val executionStateCleanUpInSecs: Int =
    getConfSource.getInt("web-server.workflow-state-cleanup-in-seconds")
  val cleanupAllExecutionResults: Boolean =
    getConfSource.getBoolean("web-server.clean-all-execution-results-on-server-start")
  val maxWorkflowWebsocketRequestPayloadSizeKb: Int =
    getConfSource.getInt("web-server.max-workflow-websocket-request-payload-size-kb")

  // AI Assistant
  val aiAssistantConfig: Option[Config] =
    if (getConfSource.hasPath("ai-assistant-server"))
      Some(getConfSource.getConfig("ai-assistant-server"))
    else None
}
