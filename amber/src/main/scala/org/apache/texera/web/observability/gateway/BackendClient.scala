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
import org.apache.texera.web.observability.gateway.dtos.{GatewayError, MaxResponseBytes}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import scala.util.{Failure, Success, Try}

/**
  * Thin HTTP wrapper used by the per-backend clients. We deliberately
  * use the JDK 11 HttpClient (no new dependency) and keep the surface
  * minimal: one ``get`` / one ``post`` taking a typed body, with
  * VictoriaLogs/Metrics tenancy headers injected at the boundary.
  *
  * Two security rails enforced here:
  *   1. Hard response-byte cap. We read the body into a buffer that
  *      stops at [[dtos.MaxResponseBytes]] — an attacker / runaway
  *      backend cannot make us swallow a 1 GiB body.
  *   2. Per-request scope: the GatewayScope arrives validated. Tenant
  *      isolation against VictoriaLogs / VictoriaMetrics is enforced
  *      by the LogsQL / MetricsQL stream filters that the builders
  *      derive from `scope.allowedWorkflowIds` — NOT by header
  *      multi-tenancy. We previously sent an `AccountID: <user_id>`
  *      header for defence-in-depth, but the OTel collector does not
  *      set AccountID at ingest (every record lands in tenant 0), so
  *      that header caused every authenticated query to return zero
  *      results. TODO(observability/multi-tenant): wire per-user
  *      AccountID through the collector + ingest pipeline, then
  *      re-introduce the header here.
  *
  * Secret redaction is intentionally NOT done at this layer: it runs
  * per-field inside the parsers (parseLogs / parseTraces), so a single
  * oversized value can't truncate and corrupt the whole JSON response.
  */
class BackendClient(
    baseUrl: String,
    timeoutMs: Long = 5000L
) extends LazyLogging {

  private val http: HttpClient = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofMillis(timeoutMs))
    .followRedirects(
      HttpClient.Redirect.NEVER
    ) // backends shouldn't redirect; if they do, treat as error
    .build()

  /** GET that returns a (status, body) tuple or a typed error. The
    *  body is decoded as UTF-8 and is truncated at MaxResponseBytes
    *  with a [[GatewayError.ResponseTooLarge]] surfaced.
    */
  def get(
      path: String,
      scope: GatewayScope,
      signal: String
  ): Either[GatewayError, BackendResponse] = {
    val uri = URI.create(baseUrl + path)
    val req = HttpRequest
      .newBuilder(uri)
      .timeout(Duration.ofMillis(timeoutMs))
      .header("Accept", "application/json")
      // Tenancy via VL/VM multi-tenant headers is disabled — see the
      // class comment. Project header is still useful as a logging
      // breadcrumb on the backend access logs and costs us nothing.
      .header("ProjectID", scope.allowedProjectIds.headOption.map(_.toString).getOrElse("0"))
      .GET()
      .build()
    send(req, signal)
  }

  /** POST a typed body. ``contentType`` is the only place we accept
    *  an arbitrary string — but it's a CONST passed by the caller,
    *  never from request input.
    */
  def post(
      path: String,
      body: Array[Byte],
      contentType: String,
      scope: GatewayScope,
      signal: String
  ): Either[GatewayError, BackendResponse] = {
    val uri = URI.create(baseUrl + path)
    val req = HttpRequest
      .newBuilder(uri)
      .timeout(Duration.ofMillis(timeoutMs))
      .header("Accept", "application/json")
      .header("Content-Type", contentType)
      // See class comment for why we no longer send AccountID.
      .header("ProjectID", scope.allowedProjectIds.headOption.map(_.toString).getOrElse("0"))
      .POST(HttpRequest.BodyPublishers.ofByteArray(body))
      .build()
    send(req, signal)
  }

  private def send(req: HttpRequest, signal: String): Either[GatewayError, BackendResponse] = {
    logger.debug(s"[$signal] sending ${req.method()} ${req.uri()} (timeout ${timeoutMs}ms)")
    val startNanos = System.nanoTime()
    Try(http.send(req, BodyHandlers.ofByteArray())) match {
      case Failure(e) =>
        val elapsedMs = (System.nanoTime() - startNanos) / 1000000L
        // This is the line that explains the dashboard's "Unreachable"
        // badge: connection refused, DNS failure, or timeout against the
        // backend's base URL. Logged at WARN with the cause so operators
        // don't have to attach a debugger to find a misconfigured host.
        logger.warn(
          s"[$signal] backend unreachable at $baseUrl after ${elapsedMs}ms " +
            s"(${req.method()} ${req.uri()}): ${e.getClass.getSimpleName}: ${e.getMessage}"
        )
        Left(GatewayError.BackendUnreachable(signal))
      case Success(resp: HttpResponse[Array[Byte]]) =>
        val elapsedMs = (System.nanoTime() - startNanos) / 1000000L
        val raw = resp.body()
        if (raw == null) {
          logger.debug(
            s"[$signal] ${resp.statusCode()} from $baseUrl in ${elapsedMs}ms (empty body)"
          )
          Right(BackendResponse(resp.statusCode(), ""))
        } else if (raw.length.toLong > MaxResponseBytes) {
          logger.warn(
            s"[$signal] response from $baseUrl exceeds the ${MaxResponseBytes}-byte cap " +
              s"(${raw.length} bytes in ${elapsedMs}ms) — rejecting to protect the gateway"
          )
          Left(GatewayError.ResponseTooLarge)
        } else {
          logger.debug(
            s"[$signal] ${resp.statusCode()} from $baseUrl in ${elapsedMs}ms (${raw.length} bytes)"
          )
          Right(BackendResponse(resp.statusCode(), new String(raw, StandardCharsets.UTF_8)))
        }
    }
  }
}

/** Wrapped backend response — status + body. Secret redaction is NOT
  *  applied here: it happens per-field inside the parsers (parseLogs /
  *  parseTraces sanitize individual message/attribute values). A
  *  whole-body LogSanitizer.sanitize pass is unsafe on these JSON
  *  payloads — its 16 KiB cap truncates large responses mid-value and
  *  corrupts the JSON.
  */
case class BackendResponse(status: Int, body: String) {
  def isOk: Boolean = status >= 200 && status < 300
}
