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

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType, Response}
import javax.servlet.http.HttpServletRequest
import org.apache.texera.auth.SessionUser
import org.apache.texera.web.observability.gateway.dtos._

/**
  * Dropwizard resources for the observability gateway.
  *
  * Every endpoint runs the same five-step skeleton (see DESIGN.md):
  *   1. Auth      — handled by @Auth + @RolesAllowed.
  *   2. Rate limit — token bucket per user, per IP.
  *   3. Scope     — resolved from the SessionUser via ScopeResolver.
  *   4. Validate  — typed DTO validators reject anything out of range.
  *   5. Query     — typed builders only, then BackendClient with the
  *      AccountID/ProjectID headers. Response goes through redaction
  *      before reaching JSON.
  *
  * The resources are intentionally small — most of the security logic
  * lives in the dtos / builders / scope objects so it's unit-testable
  * without a Dropwizard fixture.
  */

/** Tiny helper that turns a [[GatewayError]] into a Dropwizard
  *  Response. Kept here so every resource uses the same shape.
  */
private[gateway] object Respond extends LazyLogging {
  def err(e: GatewayError): Response = {
    // One breadcrumb per rejected request, at a level keyed to severity:
    //   5xx  — the gateway or a backend misbehaved → WARN (operator should look)
    //   4xx  — the caller was rejected as designed (rate limit, forbidden,
    //          bad input) → DEBUG (expected, only useful when tracing a
    //          specific user's failing request).
    val line = s"observability request rejected: ${e.code} (HTTP ${e.status}) — ${e.message}"
    if (e.status >= 500) logger.warn(line) else logger.debug(line)
    Response
      .status(e.status)
      .entity(Map("code" -> e.code, "message" -> e.message))
      .`type`(MediaType.APPLICATION_JSON)
      .build()
  }

  def json(value: Any): Response =
    Response.ok(value).`type`(MediaType.APPLICATION_JSON).build()
}

/** Shared pre-flight: rate limit + scope resolution. */
private[gateway] object Preflight extends LazyLogging {
  def run(
      ctx: GatewayContext,
      user: SessionUser,
      req: HttpServletRequest
  ): Either[GatewayError, GatewayScope] = {
    val ip = Option(req.getRemoteAddr).getOrElse("unknown")
    val userKey = s"user:${user.getUid}"
    val ipKey = s"ip:$ip"
    if (!ctx.perUserLimiter.tryAcquire(userKey)) {
      logger.warn(s"observability rate limit hit for user ${user.getUid} (per-user bucket)")
      return Left(GatewayError.RateLimited)
    }
    if (!ctx.perIpLimiter.tryAcquire(ipKey)) {
      logger.warn(s"observability rate limit hit for ip $ip (per-ip bucket)")
      return Left(GatewayError.RateLimited)
    }
    val scope = ctx.scopeResolver.resolve(user)
    logger.debug(
      s"observability preflight ok for user ${user.getUid} from $ip — " +
        s"scope: ${scope.allowedWorkflowIds.size} workflow(s), ${scope.allowedProjectIds.size} project(s)"
    )
    Right(scope)
  }
}

@Path("/observability/health")
@Produces(Array(MediaType.APPLICATION_JSON))
class ObservabilityHealthResource(ctx: GatewayContext) extends LazyLogging {

  @GET
  def health(@Auth user: SessionUser): Response = {
    // Light-touch reachability — used by the dashboard to render
    // "Disabled" / "Unreachable" panels. No backend query; just a
    // HEAD-style ping that surfaces typed-status only.
    val checks = Map(
      "logs" -> reachable(ctx.logsClient),
      "metrics" -> reachable(ctx.metricsClient),
      "traces" -> reachable(ctx.tracesClient),
      "profiles" -> reachable(ctx.profilesClient)
    )
    val unreachable = checks.collect { case (signal, false) => signal }.toSeq.sorted
    if (unreachable.nonEmpty)
      logger.warn(
        s"observability health check: unreachable backend(s): ${unreachable.mkString(", ")}"
      )
    else
      logger.debug(s"observability health check: all backends reachable")
    Respond.json(Map("status" -> "ok", "checks" -> checks))
  }

  private def reachable(client: BackendClient): Boolean = {
    client
      .get(
        "/",
        GatewayScope(userId = 0L, allowedWorkflowIds = Set.empty, allowedProjectIds = Set.empty),
        "health"
      )
      .isRight
  }
}
