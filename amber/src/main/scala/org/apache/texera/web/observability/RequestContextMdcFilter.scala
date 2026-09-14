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

package org.apache.texera.web.observability

import org.slf4j.MDC

import javax.servlet._
import javax.servlet.http.HttpServletRequest

/**
  * Servlet filter that pushes request-scoped IDs into the SLF4J MDC
  * so every log record emitted while handling a request carries
  * `texera.workflow.id`, `texera.execution.id`, and
  * `texera.computing_unit.id` when those are visible in the URL or
  * request headers.
  *
  * Without this, the OTel log appender bridges every Logback event
  * with no per-request context, so the dashboard's CU/workflow
  * filters match nothing live — the only records carrying those keys
  * are seed data pushed via the VictoriaLogs ingest API.
  *
  * Sources of IDs, in priority order:
  *   1. HTTP headers `X-Texera-Workflow-Id`, `X-Texera-Execution-Id`,
  *      `X-Texera-Computing-Unit-Id` — set explicitly by the
  *      Angular client when known.
  *   2. URL path segments matching `/workflow/<digits>`,
  *      `/execution/<digits>`, `/computing-unit/<digits>` (also the
  *      `/{wid}` / `/{cuid}` patterns used by some resources).
  *
  * MDC is ALWAYS cleared in a `finally` block so a thread reused for
  * a different request doesn't leak the previous request's labels.
  *
  * Keys here must stay in sync with [[LogSanitizer.AllowedMdcKeys]] —
  * any new key added here must be allowlisted there or the OTel
  * appender will silently drop it.
  */
class RequestContextMdcFilter extends Filter {

  override def init(filterConfig: FilterConfig): Unit = ()
  override def destroy(): Unit = ()

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain
  ): Unit = {
    val pushed = scala.collection.mutable.ArrayBuffer.empty[String]
    request match {
      case http: HttpServletRequest =>
        // Header overrides come first — an explicit client header is
        // the most precise signal we have.
        applyHeader(http, "X-Texera-Workflow-Id", "texera.workflow.id", pushed)
        applyHeader(http, "X-Texera-Execution-Id", "texera.execution.id", pushed)
        applyHeader(http, "X-Texera-Computing-Unit-Id", "texera.computing_unit.id", pushed)

        // URL extraction — only fill keys that weren't already set
        // by headers.
        val path = Option(http.getRequestURI).getOrElse("")
        applyPath(path, RequestContextMdcFilter.WorkflowPattern, "texera.workflow.id", pushed)
        applyPath(path, RequestContextMdcFilter.ExecutionPattern, "texera.execution.id", pushed)
        applyPath(path, RequestContextMdcFilter.CuPattern, "texera.computing_unit.id", pushed)

        // Query-parameter extraction — the WebSocket upgrade URL is
        // `/wsapi/workflow-websocket?wid=<w>&cuid=<c>` and Texera
        // also accepts `?cuid=N` on the REST PVE endpoints. Filling
        // the MDC here means every record emitted during the WS
        // handshake AND the long-running session that follows
        // carries the ids the client supplied.
        applyParam(http, "wid", "texera.workflow.id", pushed)
        applyParam(http, "eid", "texera.execution.id", pushed)
        applyParam(http, "cuid", "texera.computing_unit.id", pushed)
      case _ =>
      // Non-HTTP request (websocket upgrades fall through here on
      // some Servlet versions). No MDC context to add.
    }
    try {
      chain.doFilter(request, response)
    } finally {
      // Defence in depth: clear only what THIS filter set so we don't
      // step on MDC populated by other middleware.
      pushed.foreach(MDC.remove)
    }
  }

  private def applyHeader(
      req: HttpServletRequest,
      headerName: String,
      mdcKey: String,
      pushed: scala.collection.mutable.ArrayBuffer[String]
  ): Unit = {
    val raw = Option(req.getHeader(headerName)).map(_.trim).filter(_.nonEmpty)
    raw.foreach { value =>
      // Cheap allowlist: only digits get through. Prevents log
      // forging via a CRLF in the header value.
      if (value.forall(_.isDigit) && value.length <= 19) {
        MDC.put(mdcKey, value)
        pushed += mdcKey
      }
    }
  }

  private def applyPath(
      path: String,
      pattern: scala.util.matching.Regex,
      mdcKey: String,
      pushed: scala.collection.mutable.ArrayBuffer[String]
  ): Unit = {
    if (MDC.get(mdcKey) == null) {
      pattern.findFirstMatchIn(path).foreach { m =>
        val v = m.group(1)
        MDC.put(mdcKey, v)
        pushed += mdcKey
      }
    }
  }

  /** Read a query parameter, validate it's a positive integer, push
    *  to MDC. Same shape as applyHeader: numeric-only allowlist and a
    *  length cap so a forged value can't inject newlines into the
    *  rendered log line.
    */
  private def applyParam(
      req: HttpServletRequest,
      paramName: String,
      mdcKey: String,
      pushed: scala.collection.mutable.ArrayBuffer[String]
  ): Unit = {
    if (MDC.get(mdcKey) == null) {
      val raw = Option(req.getParameter(paramName)).map(_.trim).filter(_.nonEmpty)
      raw.foreach { value =>
        if (value.forall(_.isDigit) && value.length <= 19) {
          MDC.put(mdcKey, value)
          pushed += mdcKey
        }
      }
    }
  }
}

object RequestContextMdcFilter {
  // The URL patterns cover the existing JAX-RS @Path templates that
  // embed numeric IDs. `(\d+)` is the only allowlisted shape because
  // every Texera id is a positive integer; non-digit segments don't
  // need MDC propagation.
  val WorkflowPattern: scala.util.matching.Regex = """/workflow/(\d+)""".r
  val ExecutionPattern: scala.util.matching.Regex = """/execution/(\d+)""".r
  val CuPattern: scala.util.matching.Regex = """/computing-unit/(\d+)""".r
}
