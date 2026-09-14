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

import java.time.{Duration, Instant}

/**
  * Strongly-typed request / response DTOs for the gateway.
  *
  * Every field is either a typed primitive (Long, Instant, enum) or
  * a length-/range-validated wrapper. There is no public field that
  * accepts an arbitrary string and lets it through to a backend
  * query language verbatim — the typed builders in [[builders]]
  * receive only validated values from these DTOs.
  *
  * Time-window caps differ per signal, per the PR plan:
  *   logs     ≤ 7  days
  *   metrics  ≤ 90 days
  *   traces   ≤ 24 hours
  *   profiles ≤ 7  days
  *
  * Page size is server-clamped at [[MaxPageSize]] for every signal.
  */
object dtos {

  val MaxPageSize: Int = 1000
  val MaxFreeTextLen: Int = 256
  val MaxResponseBytes: Long = 10L * 1024L * 1024L // 10 MiB hard cap

  /** Log severity, exposed to the UI as a closed enum. The wire
    *  value is the standard OTel severity-text string.
    */
  sealed abstract class LogLevel(val name: String)
  object LogLevel {
    case object TRACE extends LogLevel("TRACE")
    case object DEBUG extends LogLevel("DEBUG")
    case object INFO extends LogLevel("INFO")
    case object WARN extends LogLevel("WARN")
    case object ERROR extends LogLevel("ERROR")

    val all: Seq[LogLevel] = Seq(TRACE, DEBUG, INFO, WARN, ERROR)
    def parse(raw: String): Option[LogLevel] =
      Option(raw).flatMap(s => all.find(_.name.equalsIgnoreCase(s.trim)))
  }

  // ---- typed unions for validator results -------------------------------

  /** Result of validating an inbound request. Either a clean
    *  typed value, or a GatewayError with a stable code + message
    *  shape suitable for serializing to JSON.
    */
  sealed trait ValidationResult[+T]
  case class Valid[T](value: T) extends ValidationResult[T]
  case class Invalid(error: GatewayError) extends ValidationResult[Nothing]

  /** Stable error shape returned to the UI. ``code`` is a short
    *  machine-readable token, ``message`` is human-readable and
    *  redaction-safe (we never echo raw user input here).
    */
  case class GatewayError(code: String, message: String, status: Int)

  object GatewayError {
    // No upper bound on the window: the DB-backed count has no retention
    // limit, and the metrics/logs/traces/profiles backends simply return
    // whatever they still retain for the requested range. The only invalid
    // window is an empty or inverted one (to must be after from).
    val BadTimeWindow: GatewayError =
      GatewayError("bad_time_window", "time window must be > 0 (end must be after start)", 400)
    val BadPageSize: GatewayError =
      GatewayError("bad_page_size", s"pageSize must be in [1, ${MaxPageSize}]", 400)
    val BadLevel: GatewayError =
      GatewayError("bad_level", "level must be one of TRACE/DEBUG/INFO/WARN/ERROR", 400)
    val FreeTextTooLong: GatewayError =
      GatewayError("free_text_too_long", s"query must be <= ${MaxFreeTextLen} chars", 400)
    val Forbidden: GatewayError =
      GatewayError("forbidden", "no access to that scope", 403)
    val RateLimited: GatewayError =
      GatewayError("rate_limited", "too many requests", 429)
    val BackendUnreachable: (String => GatewayError) = (signal: String) =>
      GatewayError("backend_unreachable", s"$signal backend is unreachable", 503)
    val ResponseTooLarge: GatewayError =
      GatewayError("response_too_large", s"response exceeded ${MaxResponseBytes} bytes", 502)
    // Surfaced when a backend returns a non-2xx status. We deliberately do
    // NOT include the backend's body in the message — VictoriaLogs/Jaeger
    // echo the rejected query back, which would leak whatever filters the
    // caller supplied. Operators see the body in the audit log.
    val BackendError: ((String, Int) => GatewayError) = (signal: String, status: Int) =>
      GatewayError("backend_error", s"$signal backend returned HTTP $status", 502)
    val BadSort: GatewayError =
      GatewayError("bad_sort", "sort must be one of newest/oldest/severity/service", 400)
    val BadCursor: GatewayError =
      GatewayError("bad_cursor", "pageCursor must be a non-negative integer", 400)
  }

  // ---- shared bits ------------------------------------------------------

  /** Validated time window. Both ends are Instants, range bounded
    *  per signal, ``to > from`` strictly.
    */
  case class TimeWindow(from: Instant, to: Instant)

  object TimeWindow {
    def validate(
        fromMs: Long,
        toMs: Long
    ): ValidationResult[TimeWindow] = {
      val from = Instant.ofEpochMilli(fromMs)
      val to = Instant.ofEpochMilli(toMs)
      val seconds = Duration.between(from, to).toSeconds
      // No maximum: only reject an empty or inverted window.
      if (seconds <= 0) Invalid(GatewayError.BadTimeWindow)
      else Valid(TimeWindow(from, to))
    }
  }

  /** Free text used as a *value* (never as syntax) in a backend
    *  query. Length-capped and CRLF-stripped before reaching a
    *  builder. ``None`` for absent input.
    */
  case class FreeText(value: String)

  object FreeText {
    def validate(raw: Option[String]): ValidationResult[Option[FreeText]] = {
      raw match {
        case None => Valid(None)
        case Some(s) =>
          if (s.length > MaxFreeTextLen) Invalid(GatewayError.FreeTextTooLong)
          else {
            val stripped = s.filter(c => c >= 0x20 && c != 0x7f)
            if (stripped.isEmpty) Valid(None) else Valid(Some(FreeText(stripped)))
          }
      }
    }
  }

  /** Validated page size in [1, MaxPageSize]. */
  case class PageSize(value: Int)

  object PageSize {
    def validate(raw: Int): ValidationResult[PageSize] =
      if (raw < 1 || raw > MaxPageSize) Invalid(GatewayError.BadPageSize)
      else Valid(PageSize(raw))
  }

  // ---- per-signal request DTOs (validated) -----------------------------

  /** Inbound logs search request before validation. Strings/longs
    *  only — never reaches a query builder unvalidated.
    */
  case class RawLogsSearchRequest(
      workflowId: Option[Long],
      executionId: Option[Long],
      computingUnitId: Option[Long],
      userId: Option[Long],
      services: Option[Seq[String]],
      level: Option[String],
      query: Option[String],
      sort: Option[String],
      fromMs: Long,
      toMs: Long,
      pageSize: Int,
      pageCursor: Option[String]
  )

  /** Validated and ready to hand to LogsQLBuilder. */
  case class ValidatedLogsRequest(
      workflowId: Option[Long],
      executionId: Option[Long],
      computingUnitId: Option[Long],
      userId: Option[Long],
      services: Seq[ServiceName],
      level: Option[LogLevel],
      query: Option[FreeText],
      sort: LogSort,
      window: TimeWindow,
      pageSize: PageSize,
      // Page offset (records skipped). The wire shape stays as String
      // (it's an opaque cursor on the UI) but we parse it as Long
      // here so the builder gets a typed value.
      offset: Long,
      pageCursor: Option[String]
  )

  /** Closed enum of sort orders. Backed by a LogsQL `| sort by (...)`
    *  clause; the LogsQL fragment is in [[LogsQLBuilder]] so this DTO
    *  stays storage-agnostic.
    */
  sealed abstract class LogSort(val name: String)
  object LogSort {
    case object NewestFirst extends LogSort("newest")
    case object OldestFirst extends LogSort("oldest")
    case object SeverityHigh extends LogSort("severity")
    case object ServiceAsc extends LogSort("service")
    val all: Seq[LogSort] = Seq(NewestFirst, OldestFirst, SeverityHigh, ServiceAsc)
    val Default: LogSort = NewestFirst
    def parse(raw: String): Option[LogSort] =
      Option(raw).flatMap(s => all.find(_.name.equalsIgnoreCase(s.trim)))
  }

  /** Validated service name. Texera service names are emitted by the
    *  OTel resource attribute `service.name` — we know they match the
    *  pattern `texera-?[a-z0-9-]+` because the JVM bootstrap controls
    *  them. We enforce that pattern here so a forged value cannot
    *  inject LogsQL syntax via the service filter.
    */
  case class ServiceName(value: String)

  object ServiceName {
    // Conservative: lowercase letters, digits, dash. Length-capped at
    // 64 to keep stream labels bounded.
    private val ServicePattern = "^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$".r.pattern

    def parse(raw: String): Option[ServiceName] =
      Option(raw)
        .map(_.trim.toLowerCase)
        .filter(s => s.nonEmpty && s.length <= 64 && ServicePattern.matcher(s).matches())
        .map(ServiceName.apply)

    def validateMany(raws: Option[Seq[String]]): ValidationResult[Seq[ServiceName]] = {
      raws match {
        case None => Valid(Seq.empty)
        case Some(list) =>
          val parsed = list.iterator.flatMap(s => parse(s).iterator).toSeq.distinct.take(32)
          // We accept the parsed subset silently; a caller supplying
          // a malformed service name simply gets fewer filters, not
          // a 400. The UI's multi-select cannot produce such values
          // because its options come from /logs/sources.
          Valid(parsed)
      }
    }
  }

  case class LogEntryResponse(
      timestampMs: Long,
      level: String,
      body: String,
      traceId: Option[String],
      spanId: Option[String],
      attributes: Map[String, String]
  )

  case class LogsSearchResponse(
      entries: Seq[LogEntryResponse],
      total: Long,
      nextCursor: Option[String]
  )

  /** Distinct filter values currently present in the logs store.
    *  Powers the UI's autofill dropdowns for service / workflow id /
    *  CU id / user id. Service names are returned as raw strings
    *  (the UI doesn't need typed parsing — it just renders the chip
    *  and passes the value back).
    */
  case class LogSourcesResponse(
      services: Seq[String],
      workflowIds: Seq[Long],
      computingUnitIds: Seq[Long],
      userIds: Seq[Long],
      // id -> display name for the user-id dropdown; ids without a
      // resolved name are absent and the UI falls back to the id.
      userNames: Map[Long, String] = Map.empty
  )

  // ---- metrics ---------------------------------------------------------

  /** Named server-side metric query. We do not let the client send
    *  raw MetricsQL — they pick from a fixed enum.
    */
  sealed abstract class NamedMetric(val name: String)
  object NamedMetric {
    case object RunsPerDay extends NamedMetric("runsPerDay")
    case object TotalRuns extends NamedMetric("totalRuns")
    case object ActiveWorkflows extends NamedMetric("activeWorkflows")
    case object SuccessRate extends NamedMetric("successRate")
    case object FailureRate extends NamedMetric("failureRate")
    case object AvgDuration extends NamedMetric("avgDuration")
    case object P50Duration extends NamedMetric("p50Duration")
    case object P95Duration extends NamedMetric("p95Duration")
    case object P99Duration extends NamedMetric("p99Duration")

    val all: Seq[NamedMetric] =
      Seq(
        RunsPerDay,
        TotalRuns,
        ActiveWorkflows,
        SuccessRate,
        FailureRate,
        AvgDuration,
        P50Duration,
        P95Duration,
        P99Duration
      )
    def parse(raw: String): Option[NamedMetric] =
      Option(raw).flatMap(s => all.find(_.name == s))
  }

  case class RawMetricsQueryRequest(
      name: String,
      fromMs: Long,
      toMs: Long,
      stepSec: Option[Int]
  )

  case class ValidatedMetricsRequest(
      metric: NamedMetric,
      window: TimeWindow,
      stepSec: Int
  )

  case class MetricPoint(timestampMs: Long, value: Double)

  case class MetricsQueryResponse(
      metric: String,
      points: Seq[MetricPoint]
  )

  // ---- traces ----------------------------------------------------------

  /** Inbound trace lookup. ``traceId`` must match the regex
    *  ``^[0-9a-f]{32}$`` (same as W3C trace-id).
    */
  case class RawTracesGetRequest(traceId: String)

  case class ValidatedTracesGetRequest(traceId: String)

  object ValidatedTracesGetRequest {
    private val TraceIdPattern = "^[0-9a-f]{32}$".r.pattern
    def validate(raw: RawTracesGetRequest): ValidationResult[ValidatedTracesGetRequest] = {
      if (raw.traceId != null && TraceIdPattern.matcher(raw.traceId).matches())
        Valid(ValidatedTracesGetRequest(raw.traceId))
      else
        Invalid(GatewayError("bad_trace_id", "traceId must be 32 lowercase hex chars", 400))
    }
  }

  case class TraceSpanResponse(
      spanId: String,
      parentSpanId: Option[String],
      name: String,
      startMs: Long,
      endMs: Long,
      attributes: Map[String, String]
  )

  case class TracesGetResponse(traceId: String, spans: Seq[TraceSpanResponse])

  // ---- profiles --------------------------------------------------------

  case class RawProfilesQueryRequest(
      workflowId: Option[Long],
      executionId: Option[Long],
      fromMs: Long,
      toMs: Long
  )

  case class ValidatedProfilesRequest(
      workflowId: Option[Long],
      executionId: Option[Long],
      window: TimeWindow
  )

  /** Profiles are returned as a tree of frames. We render the tree
    *  in the UI; the gateway is only responsible for shape + size.
    */
  case class FlameFrame(name: String, value: Long, children: Seq[FlameFrame])

  case class ProfilesQueryResponse(root: Option[FlameFrame], totalSamples: Long)
}
