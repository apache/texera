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
 *  Response. Kept here so every resource uses the same shape. */
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

@Path("/observability/logs")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class LogsResource(ctx: GatewayContext) extends LazyLogging {

  @POST
  @Path("/search")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def search(
      request: RawLogsSearchRequest,
      @Auth user: SessionUser,
      @Context httpReq: HttpServletRequest
  ): Response = {
    logger.debug(s"logs search requested by user ${user.getUid}")
    Preflight.run(ctx, user, httpReq) match {
      case Left(err) => Respond.err(err)
      case Right(scope) =>
        validate(request) match {
          case Invalid(err) => Respond.err(err)
          case Valid(valid) =>
            // Tenancy assertion: a caller-supplied workflowId must
            // be in the resolved allow-set. No widening possible.
            if (!ctx.scopeResolver.assertWorkflowAllowed(scope, valid.workflowId)) {
              logger.warn(
                s"logs search DENIED: user ${user.getUid} requested workflow " +
                  s"${valid.workflowId.getOrElse("<none>")} outside their allowed scope"
              )
              Respond.err(GatewayError.Forbidden)
            } else
              runLogsQuery(valid, scope, httpReq, user)
        }
    }
  }

  private def runLogsQuery(
      req: ValidatedLogsRequest,
      scope: GatewayScope,
      httpReq: HttpServletRequest,
      user: SessionUser
  ): Response = {
    val query = LogsQLBuilder.build(req, scope)
    logger.debug(
      s"logs query for user ${user.getUid}: workflow=${req.workflowId.getOrElse("*")} " +
        s"level=${req.level.map(_.toString).getOrElse("*")} pageSize=${req.pageSize.value} — LogsQL: $query"
    )
    // VictoriaLogs defaults the `start`/`end` URL params to "the last
    // 5 minutes" when omitted, which silently shrinks every search
    // regardless of what the user picked in the time picker. We pass
    // both explicitly so the picked window actually drives the query.
    // The seconds form is what /select/logsql/query expects.
    val startSec = req.window.from.getEpochSecond
    val endSec = req.window.to.getEpochSecond
    val path =
      s"/select/logsql/query?query=${java.net.URLEncoder.encode(query, "UTF-8")}" +
        s"&start=$startSec&end=$endSec"
    ctx.logsClient.get(path, scope, "logs") match {
      case Left(err) => Respond.err(err)
      case Right(resp) if !resp.isOk =>
        Respond.err(GatewayError.BackendError("logs", resp.status))
      case Right(resp) =>
        // Pass the RAW NDJSON body. LogSanitizer strips C0 control
        // characters including '\n' (intentionally — prevents log
        // forging in OTel-bridged records); applying it to the
        // whole body would collapse every line of VictoriaLogs's
        // NDJSON output into a single line and break parsing.
        // ResponseParsers.parseLogs redacts secrets per-field
        // after splitting lines.
        ResponseParsers.parseLogs(resp.body, req.pageSize.value) match {
          case Left(err) => Respond.err(err)
          case Right(parsed) =>
            // If the response is exactly pageSize entries, more pages
            // probably exist — surface the next offset as the cursor.
            // (A short page means we definitely reached the end.) We
            // can't reliably distinguish "exactly fits the page" from
            // "more available" without a server total; assuming more
            // is the conservative UX choice.
            val pageFull = parsed.entries.size >= req.pageSize.value
            val withCursor =
              if (pageFull) parsed.copy(nextCursor = Some((req.offset + req.pageSize.value).toString))
              else parsed
            logger.info(
              s"logs search ok for user ${user.getUid}: ${withCursor.total} entr(ies)" +
                (if (pageFull) " (more pages available)" else "")
            )
            AuditLogger.record(
              AuditLogger.Entry(
                userId = user.getUid.longValue(),
                remoteIp = Option(httpReq.getRemoteAddr).getOrElse("unknown"),
                endpoint = "/observability/logs/search",
                signal = "logs",
                scope = scope,
                query = query,
                fromMs = req.window.from.toEpochMilli,
                toMs = req.window.to.toEpochMilli,
                hits = withCursor.total
              )
            )
            Respond.json(withCursor)
        }
    }
  }

  /**
   * Distinct filter values currently present in the logs store —
   * powers the UI's autofill dropdowns (service / workflow / CU).
   *
   * GET because it has no body and no side effects, and so the
   * frontend can cache it. The handler still goes through the
   * standard preflight (rate limit + scope resolution) so an
   * unauthenticated caller can't enumerate the workflow ids of
   * other tenants.
   */
  @GET
  @Path("/sources")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def sources(
      @Auth user: SessionUser,
      @Context httpReq: HttpServletRequest
  ): Response = {
    Preflight.run(ctx, user, httpReq) match {
      case Left(err) => Respond.err(err)
      case Right(scope) =>
        // 7-day window is plenty for autofill — older streams aren't
        // useful to operators debugging "right now".
        val path = "/select/logsql/streams?query=*&start=7d"
        ctx.logsClient.get(path, scope, "logs") match {
          case Left(err) => Respond.err(err)
          case Right(resp) if !resp.isOk =>
            Respond.err(GatewayError.BackendError("logs", resp.status))
          case Right(resp) =>
            ResponseParsers.parseLogSources(resp.body, scope.allowedWorkflowIds) match {
              case Left(err) => Respond.err(err)
              case Right(parsed) =>
                logger.debug(
                  s"log sources for user ${user.getUid}: ${parsed.services.size} service(s), " +
                    s"${parsed.workflowIds.size} workflow(s), ${parsed.computingUnitIds.size} CU(s), " +
                    s"${parsed.userIds.size} user(s)"
                )
                Respond.json(parsed)
            }
        }
    }
  }

  /** Force an `Option[Long]` from Jackson into a real boxed Long.
   *  Jackson Scala deserializes a JSON number that fits in 32 bits
   *  as `java.lang.Integer` regardless of the declared type
   *  parameter (which is erased on the JVM). When downstream code
   *  uses the value, Scala's runtime unbox throws ClassCastException.
   *
   *  Implementation: we MUST go through `Option[Any]` before calling
   *  `.map` — `Option[Long].map` is specialized to `JFunction1$mcJJ$sp`
   *  which unboxes the value to Long via BoxesRunTime BEFORE the
   *  closure body runs, so any inline asInstanceOf there is too late.
   *  Casting to `Option[Any]` selects the generic, non-specialized
   *  apply path, which keeps the value boxed and hands it to the
   *  closure as-is.
   */
  private def normaliseLong(opt: Option[Long]): Option[Long] = {
    val anyOpt: Option[Any] = opt.asInstanceOf[Option[Any]]
    anyOpt.map {
      case n: java.lang.Number => n.longValue()
      case other               => other.toString.toLong
    }
  }

  private def validate(raw: RawLogsSearchRequest): ValidationResult[ValidatedLogsRequest] = {
    TimeWindow.validate(Signal.Logs, raw.fromMs, raw.toMs) match {
      case Invalid(e) => Invalid(e)
      case Valid(window) =>
        PageSize.validate(raw.pageSize) match {
          case Invalid(e) => Invalid(e)
          case Valid(pageSize) =>
            val level: Option[LogLevel] = raw.level.flatMap(LogLevel.parse)
            if (raw.level.isDefined && level.isEmpty) Invalid(GatewayError.BadLevel)
            else
              FreeText.validate(raw.query) match {
                case Invalid(e) => Invalid(e)
                case Valid(freeText) =>
                  ServiceName.validateMany(raw.services) match {
                    case Invalid(e) => Invalid(e)
                    case Valid(services) =>
                      // Sort: parse closed enum or fall back to default.
                      val sortOrErr = raw.sort match {
                        case None    => Right(LogSort.Default)
                        case Some(s) => LogSort.parse(s).toRight(GatewayError.BadSort)
                      }
                      // Page cursor: opaque string in the wire shape;
                      // we treat it as a Long offset internally. Empty
                      // or absent → offset 0. Non-numeric → bad_cursor.
                      val offsetOrErr: Either[GatewayError, Long] =
                        raw.pageCursor match {
                          case None | Some("") => Right(0L)
                          case Some(s) =>
                            try {
                              val n = s.trim.toLong
                              if (n < 0L) Left(GatewayError.BadCursor) else Right(n)
                            } catch {
                              case _: NumberFormatException => Left(GatewayError.BadCursor)
                            }
                        }
                      (sortOrErr, offsetOrErr) match {
                        case (Left(e), _) => Invalid(e)
                        case (_, Left(e)) => Invalid(e)
                        case (Right(sort), Right(offset)) =>
                          Valid(
                            ValidatedLogsRequest(
                              workflowId = normaliseLong(raw.workflowId),
                              executionId = normaliseLong(raw.executionId),
                              computingUnitId = normaliseLong(raw.computingUnitId),
                              userId = normaliseLong(raw.userId),
                              services = services,
                              level = level,
                              query = freeText,
                              sort = sort,
                              window = window,
                              pageSize = pageSize,
                              offset = offset,
                              pageCursor = raw.pageCursor
                            )
                          )
                      }
                  }
              }
        }
    }
  }
}

@Path("/observability/metrics")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class MetricsResource(ctx: GatewayContext) extends LazyLogging {

  @POST
  @Path("/query")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def query(
      request: RawMetricsQueryRequest,
      @Auth user: SessionUser,
      @Context httpReq: HttpServletRequest
  ): Response = {
    logger.debug(s"metrics query '${request.name}' requested by user ${user.getUid}")
    Preflight.run(ctx, user, httpReq) match {
      case Left(err) => Respond.err(err)
      case Right(scope) =>
        validate(request) match {
          case Invalid(err) => Respond.err(err)
          case Valid(valid) =>
            val q = MetricsQLBuilder.build(valid)
            logger.debug(s"metrics query '${valid.metric.name}' step=${valid.stepSec}s — MetricsQL: $q")
            val path = s"/api/v1/query_range?query=${java.net.URLEncoder.encode(q, "UTF-8")}" +
              s"&start=${valid.window.from.getEpochSecond}&end=${valid.window.to.getEpochSecond}" +
              s"&step=${valid.stepSec}"
            ctx.metricsClient.get(path, scope, "metrics") match {
              case Left(err) => Respond.err(err)
              case Right(resp) if !resp.isOk =>
                Respond.err(GatewayError.BackendError("metrics", resp.status))
              case Right(resp) =>
                // Don't run resp.redacted here: metrics responses are numeric
                // time-series (epoch seconds + float values) with server-built
                // series labels — there are no secrets to scrub. The log-line
                // sanitizer's 16 KiB body cap truncates any larger query_range
                // payload mid-value and appends "...[truncated]", whose leading
                // '.' lands where JSON expects a value → bad_backend_response.
                ResponseParsers.parseMetrics(resp.body, valid.metric.name) match {
                  case Left(err) => Respond.err(err)
                  case Right(parsed) =>
                    logger.info(
                      s"metrics query '${valid.metric.name}' ok for user ${user.getUid}: " +
                        s"${parsed.points.size} point(s)"
                    )
                    AuditLogger.record(
                      AuditLogger.Entry(
                        userId = user.getUid.longValue(),
                        remoteIp = Option(httpReq.getRemoteAddr).getOrElse("unknown"),
                        endpoint = "/observability/metrics/query",
                        signal = "metrics",
                        scope = scope,
                        query = q,
                        fromMs = valid.window.from.toEpochMilli,
                        toMs = valid.window.to.toEpochMilli,
                        hits = parsed.points.size.toLong
                      )
                    )
                    Respond.json(parsed)
                }
            }
        }
    }
  }

  private def validate(raw: RawMetricsQueryRequest): ValidationResult[ValidatedMetricsRequest] = {
    NamedMetric.parse(raw.name) match {
      case None => Invalid(GatewayError("bad_metric_name", "unknown metric name", 400))
      case Some(metric) =>
        TimeWindow.validate(Signal.Metrics, raw.fromMs, raw.toMs) match {
          case Invalid(e) => Invalid(e)
          case Valid(window) =>
            val step = raw.stepSec.getOrElse(60).max(1).min(3600) // clamp 1s..1h
            Valid(ValidatedMetricsRequest(metric, window, step))
        }
    }
  }
}

@Path("/observability/traces")
@Produces(Array(MediaType.APPLICATION_JSON))
class TracesResource(ctx: GatewayContext) extends LazyLogging {

  @GET
  @Path("/{traceId}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def get(
      @PathParam("traceId") traceId: String,
      @Auth user: SessionUser,
      @Context httpReq: HttpServletRequest
  ): Response = {
    logger.debug(s"trace fetch requested by user ${user.getUid} for traceId=$traceId")
    Preflight.run(ctx, user, httpReq) match {
      case Left(err) => Respond.err(err)
      case Right(scope) =>
        ValidatedTracesGetRequest.validate(RawTracesGetRequest(traceId)) match {
          case Invalid(err) => Respond.err(err)
          case Valid(valid) =>
            val path = JaegerQueryBuilder.tracePath(valid)
            ctx.tracesClient.get(path, scope, "traces") match {
              case Left(err) => Respond.err(err)
              case Right(resp) if !resp.isOk =>
                Respond.err(GatewayError.BackendError("traces", resp.status))
              case Right(resp) =>
                // Same reason as metrics: whole-body redaction truncates any
                // trace over 16 KiB and corrupts the JSON. parseTraces scrubs
                // secrets per span attribute instead (as parseLogs does).
                ResponseParsers.parseTraces(resp.body, valid.traceId) match {
                  case Left(err) => Respond.err(err)
                  case Right(parsed) =>
                    logger.info(
                      s"trace fetch ok for user ${user.getUid}: traceId=${valid.traceId} " +
                        s"with ${parsed.spans.size} span(s)"
                    )
                    AuditLogger.record(
                      AuditLogger.Entry(
                        userId = user.getUid.longValue(),
                        remoteIp = Option(httpReq.getRemoteAddr).getOrElse("unknown"),
                        endpoint = s"/observability/traces/${valid.traceId}",
                        signal = "traces",
                        scope = scope,
                        query = valid.traceId,
                        fromMs = 0L,
                        toMs = 0L,
                        hits = parsed.spans.size.toLong
                      )
                    )
                    Respond.json(parsed)
                }
            }
        }
    }
  }
}

@Path("/observability/profiles")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class ProfilesResource(ctx: GatewayContext) extends LazyLogging {

  @POST
  @Path("/query")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def query(
      request: RawProfilesQueryRequest,
      @Auth user: SessionUser,
      @Context httpReq: HttpServletRequest
  ): Response = {
    logger.debug(s"profiles query requested by user ${user.getUid}")
    Preflight.run(ctx, user, httpReq) match {
      case Left(err) => Respond.err(err)
      case Right(scope) =>
        validate(request) match {
          case Invalid(err) => Respond.err(err)
          case Valid(valid) =>
            if (!ctx.scopeResolver.assertWorkflowAllowed(scope, valid.workflowId)) {
              logger.warn(
                s"profiles query DENIED: user ${user.getUid} requested workflow " +
                  s"${valid.workflowId.getOrElse("<none>")} outside their allowed scope"
              )
              Respond.err(GatewayError.Forbidden)
            } else {
              val q = ParcaQueryBuilder.build(valid, scope)
              // Parca v0.28 only ships a gRPC API — no JSON gateway. We
              // talk to it over gRPC-Web with the BaseUrl from
              // GatewayContext (so test overrides still work). The
              // QueryRange RPC returns a real per-series sample
              // histogram, which we summarise into a one-deep flame
              // tree so the dashboard panel reflects live Parca data.
              // Full nested flamegraph parsing requires the Function /
              // Location / Mapping schema and is a separate PR.
              ParcaClient.queryRange(
                baseUrl = ctx.profilesBaseUrl,
                profileQuery = q,
                startMs = valid.window.from.toEpochMilli,
                endMs = valid.window.to.toEpochMilli
              ) match {
                case Left(err) => Respond.err(err)
                case Right(summary) =>
                  val parsed = ParcaSummary.toProfilesResponse(summary)
                  logger.info(
                    s"profiles query ok for user ${user.getUid}: ${parsed.totalSamples} sample(s)"
                  )
                  AuditLogger.record(
                    AuditLogger.Entry(
                      userId = user.getUid.longValue(),
                      remoteIp = Option(httpReq.getRemoteAddr).getOrElse("unknown"),
                      endpoint = "/observability/profiles/query",
                      signal = "profiles",
                      scope = scope,
                      query = q,
                      fromMs = valid.window.from.toEpochMilli,
                      toMs = valid.window.to.toEpochMilli,
                      hits = parsed.totalSamples
                    )
                  )
                  Respond.json(parsed)
              }
            }
        }
    }
  }

  private def validate(raw: RawProfilesQueryRequest): ValidationResult[ValidatedProfilesRequest] = {
    TimeWindow.validate(Signal.Profiles, raw.fromMs, raw.toMs) match {
      case Invalid(e) => Invalid(e)
      case Valid(window) =>
        Valid(ValidatedProfilesRequest(raw.workflowId, raw.executionId, window))
    }
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
      logger.warn(s"observability health check: unreachable backend(s): ${unreachable.mkString(", ")}")
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
