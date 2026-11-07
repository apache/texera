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

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Typed view over observability.conf. Each field carries the HOCON default
  * already merged with its OTEL_* environment override, so OtelInit reads its
  * settings from one place instead of calling System.getenv directly. Values
  * are kept as strings and interpreted by OtelInit, which tolerates malformed
  * input without throwing.
  */
object ObservabilityConfig {
  private val conf: Config = ConfigFactory.parseResources("observability.conf").resolve()

  val sdkDisabled: String = conf.getString("observability.sdk-disabled")
  val endpoint: String = conf.getString("observability.endpoint")
  val resourceAttributes: String = conf.getString("observability.resource-attributes")
  val allowedHosts: String = conf.getString("observability.allowed-hosts")
  val metricExportIntervalMs: String = conf.getString("observability.metric-export-interval-ms")
}
