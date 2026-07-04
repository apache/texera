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

  "ApplicationConfig constants" should "load the default constant values" in {
    ApplicationConfig.loggingQueueSizeInterval shouldBe 30000
    ApplicationConfig.MAX_RESOLUTION_ROWS shouldBe 2000
    ApplicationConfig.MAX_RESOLUTION_COLUMNS shouldBe 2000
    ApplicationConfig.numWorkerPerOperatorByDefault shouldBe 2
    ApplicationConfig.getStatusUpdateIntervalInMs shouldBe 500L
    ApplicationConfig.getRuntimeStatisticsPersistenceIntervalInMs shouldBe 2000L
  }

  "ApplicationConfig flow control" should "load credit and polling defaults" in {
    ApplicationConfig.maxCreditAllowedInBytesPerChannel shouldBe 1600000000L
    ApplicationConfig.creditPollingIntervalInMs shouldBe 200
  }

  "ApplicationConfig network buffering" should "load batch size and adaptive buffering defaults" in {
    ApplicationConfig.defaultDataTransferBatchSize shouldBe 400
    ApplicationConfig.enableAdaptiveNetworkBuffering shouldBe true
    ApplicationConfig.adaptiveBufferingTimeoutMs shouldBe 500
  }

  "ApplicationConfig reconfiguration" should "default transactional reconfiguration to false" in {
    ApplicationConfig.enableTransactionalReconfiguration shouldBe false
  }

  "ApplicationConfig fault tolerance" should "disable logging with an empty log-storage-uri" in {
    ApplicationConfig.faultToleranceLogFlushIntervalInMs shouldBe 0L
    if (sys.env.get("FAULT_TOLERANCE_LOG_STORAGE_URI").forall(_.isEmpty)) {
      ApplicationConfig.faultToleranceLogRootFolder shouldBe None
      ApplicationConfig.isFaultToleranceEnabled shouldBe false
    }
  }

  "ApplicationConfig scheduling" should "load schedule-generator defaults" in {
    ApplicationConfig.maxConcurrentRegions shouldBe 1
    ApplicationConfig.useGlobalSearch shouldBe false
    ApplicationConfig.useTopDownSearch shouldBe false
    ApplicationConfig.searchTimeoutMilliseconds shouldBe 1000
  }

  "ApplicationConfig storage cleanup" should "load result-cleanup TTL and interval defaults" in {
    ApplicationConfig.sinkStorageTTLInSecs shouldBe 86400
    ApplicationConfig.sinkStorageCleanUpCheckIntervalInSecs shouldBe 86400
  }

  "ApplicationConfig web server" should "load web-server defaults" in {
    ApplicationConfig.operatorConsoleBufferSize shouldBe 100
    ApplicationConfig.consoleMessageDisplayLength shouldBe 100
    ApplicationConfig.executionResultPollingInSecs shouldBe 3
    ApplicationConfig.executionStateCleanUpInSecs shouldBe 30
    ApplicationConfig.cleanupAllExecutionResults shouldBe false
    ApplicationConfig.maxWorkflowWebsocketRequestPayloadSizeKb shouldBe 64
  }

  "ApplicationConfig AI assistant" should "expose the ai-assistant-server config block" in {
    ApplicationConfig.aiAssistantConfig should not be empty
    ApplicationConfig.aiAssistantConfig.get.getString("assistant") shouldBe "none"
  }
}
