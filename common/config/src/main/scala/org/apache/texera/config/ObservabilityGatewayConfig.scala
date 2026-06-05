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

package org.apache.texera.config

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Query endpoints for the observability dashboard's backend gateway.
  *
  * Source of truth is `observability-gateway.conf`, which defaults to the
  * host-local stack and lets each URL be overridden per deployment via the
  * `TEXERA_OBS_*_URL` env vars (docker-compose sets these to the bridge-network
  * service names). This is the query side; the OTLP export endpoint that
  * services push to is configured separately in [[org.apache.texera.observability.OtelInit]].
  */
object ObservabilityGatewayConfig {
  private val conf: Config =
    ConfigFactory.parseResources("observability-gateway.conf").resolve()

  // VictoriaLogs LogsQL query API.
  val logsUrl: String = conf.getString("observability-gateway.logs-url")

  // VictoriaMetrics MetricsQL query API.
  val metricsUrl: String = conf.getString("observability-gateway.metrics-url")

  // Jaeger query API.
  val tracesUrl: String = conf.getString("observability-gateway.traces-url")

  // Parca pprof query API (gRPC-Web).
  val profilesUrl: String = conf.getString("observability-gateway.profiles-url")
}
