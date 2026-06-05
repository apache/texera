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

package org.apache.texera.web.observability.gateway

import org.apache.texera.config.ObservabilityGatewayConfig

/**
  * Single bag of collaborators every observability resource needs.
  * Centralises wiring so the resource constructors stay short and so
  * tests can override one collaborator (e.g. stub the scope resolver)
  * without touching the rest.
  */
case class GatewayContext(
    scopeResolver: ScopeResolver,
    perUserLimiter: RateLimiter,
    perIpLimiter: RateLimiter,
    logsClient: BackendClient,
    metricsClient: BackendClient,
    tracesClient: BackendClient,
    profilesClient: BackendClient,
    // Parca's API is gRPC-only — the BackendClient (HTTP/1.1 JSON) can't
    // reach it. We keep the BackendClient field above for symmetry and
    // for the reachability check, but the actual query path uses
    // [[ParcaClient]] which talks gRPC-Web. Both pull from the same config
    // key so operators have one URL to set.
    profilesBaseUrl: String
)

object GatewayContext {

  /** Build the production context. The backend query URLs come from
    *  [[ObservabilityGatewayConfig]] (`observability-gateway.conf`), which
    *  defaults to the host-local stack and is overridden inside docker via
    *  the TEXERA_OBS_*_URL env vars set in bin/single-node/.env. A natively
    *  run backend therefore reaches the loopback-published backends with no
    *  extra configuration.
    */
  def default(): GatewayContext = {
    GatewayContext(
      scopeResolver = new ScopeResolver.Jooq(),
      perUserLimiter = RateLimiter.defaultPerUser(),
      perIpLimiter = RateLimiter.defaultPerIp(),
      logsClient = new BackendClient(ObservabilityGatewayConfig.logsUrl),
      metricsClient = new BackendClient(ObservabilityGatewayConfig.metricsUrl),
      tracesClient = new BackendClient(ObservabilityGatewayConfig.tracesUrl),
      profilesClient = new BackendClient(ObservabilityGatewayConfig.profilesUrl),
      profilesBaseUrl = ObservabilityGatewayConfig.profilesUrl
    )
  }
}
