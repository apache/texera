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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.observability.LogSanitizer
import org.apache.texera.web.observability.gateway.dtos._

import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * Pure response parsers for the four observability backends.
 *
 * Each method takes the post-redaction body string and returns either
 * a typed response DTO or a [[GatewayError]] with code
 * "bad_backend_response" (HTTP 502). Parsers never throw — they trap
 * Jackson errors and turn them into Lefts so the resource layer can
 * surface a clean error to the UI.
 *
 * Design notes:
 *   - JsonNode traversal (not bind-to-case-class) so unexpected extra
 *     fields are ignored rather than failing the whole response.
 *   - Hard caps via [[dtos.MaxPageSize]] on every collection field —
 *     a backend that returns a million records cannot OOM us.
 *   - Pure functions: no I/O, no clock reads, no global state. The
 *     spec exercises every branch with captured fixtures.
 */
object ResponseParsers extends LazyLogging {

  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private def bad(detail: String): GatewayError = {
    // Single chokepoint for every "the backend sent us something we
    // couldn't parse" path. WARN (not ERROR) because the gateway itself
    // is healthy — the upstream payload is the problem — but an operator
    // debugging an empty/garbled panel needs to see exactly what broke.
    logger.warn(s"unparseable observability backend response: $detail")
    GatewayError("bad_backend_response", s"backend response could not be parsed: $detail", 502)
  }

  // ---- logs (VictoriaLogs LogsQL NDJSON) ------------------------------

  /** Keys that map to first-class fields on [[LogEntryResponse]] and
   *  therefore should NOT be re-emitted as attributes. */
  private val LogReservedKeys: Set[String] =
    Set("_msg", "_time", "trace_id", "span_id", "severity_text", "severity_number", "_stream", "_stream_id")

  def parseLogs(body: String, pageSize: Int): Either[GatewayError, LogsSearchResponse] = {
    val cap = pageSize.max(1).min(MaxPageSize)
    Try {
      body.linesIterator
        .filter(_.trim.nonEmpty)
        .take(cap)
        .map(line => redactLogEntry(parseLogEntry(mapper.readTree(line))))
        .toSeq
    } match {
      case Success(entries) =>
        Right(LogsSearchResponse(entries = entries, total = entries.size.toLong, nextCursor = None))
      case Failure(e) =>
        Left(bad(s"VictoriaLogs: ${e.getMessage}"))
    }
  }

  // ---- log sources (VictoriaLogs /streams) --------------------------

  /** Pattern that extracts a single label=value pair out of a LogsQL
   *  stream selector body. Stream labels are written
   *  ``key="value"`` (or ``key=value`` for legacy data). We require
   *  the equals sign; anything that doesn't look like a label is
   *  silently skipped — never throws.
   */
  private val StreamLabelPattern = """([a-zA-Z0-9_.-]+)\s*=\s*"?([^",}]*)"?""".r

  def parseLogSources(
      body: String,
      allowedWorkflowIds: Set[Long]
  ): Either[GatewayError, LogSourcesResponse] = {
    Try(mapper.readTree(body)) match {
      case Failure(e) => Left(bad(s"VictoriaLogs/streams: ${e.getMessage}"))
      case Success(root) =>
        val values = root.path("values")
        val services = scala.collection.mutable.LinkedHashSet.empty[String]
        val workflowIds = scala.collection.mutable.LinkedHashSet.empty[Long]
        val cuIds = scala.collection.mutable.LinkedHashSet.empty[Long]
        val userIds = scala.collection.mutable.LinkedHashSet.empty[Long]
        if (values.isArray) {
          val it = values.iterator().asScala
          while (it.hasNext) {
            val streamLabel = it.next().path("value").asText("")
            // Strip leading { and trailing } so the label-pattern walker
            // doesn't try to match the braces as label characters.
            val inner = streamLabel.stripPrefix("{").stripSuffix("}")
            StreamLabelPattern.findAllMatchIn(inner).foreach { m =>
              val key = m.group(1)
              val value = m.group(2)
              key match {
                case "service" | "service.name" =>
                  if (value.nonEmpty) services += value
                case "texera.workflow.id" =>
                  Try(value.toLong).toOption.foreach(workflowIds += _)
                case "texera.computing_unit.id" =>
                  Try(value.toLong).toOption.foreach(cuIds += _)
                case "texera.user.id" =>
                  Try(value.toLong).toOption.foreach(userIds += _)
                case _ => ()
              }
            }
          }
        }
        // Tenancy filter: even though the underlying VL query is
        // tenancy-aware on the search path, /streams returns *every*
        // stream label across all tenants. Drop workflow ids the
        // caller isn't allowed to see. CU/user ids stay (the user
        // will only ever see those CUs' / users' log records via
        // the same scope path).
        val allowedWorkflows = workflowIds.iterator.filter(allowedWorkflowIds.contains).toSeq
        Right(LogSourcesResponse(
          services = services.toSeq.sorted,
          workflowIds = allowedWorkflows.sorted,
          computingUnitIds = cuIds.toSeq.sorted,
          userIds = userIds.toSeq.sorted
        ))
    }
  }

  /** Per-entry redaction. Replaces the whole-body sanitize that used
   *  to run before parsing — that approach stripped '\n' from the
   *  NDJSON stream and collapsed every record into a single blob.
   *  Sanitizing each field after parsing preserves line boundaries
   *  and still scrubs secret patterns / oversized bodies. */
  private def redactLogEntry(entry: LogEntryResponse): LogEntryResponse =
    entry.copy(
      body = LogSanitizer.sanitize(entry.body),
      attributes = entry.attributes.iterator.map { case (k, v) => k -> LogSanitizer.sanitize(v) }.toMap
    )

  private def parseLogEntry(node: JsonNode): LogEntryResponse = {
    val timestampMs = textOpt(node, "_time").flatMap(toEpochMillis).getOrElse(0L)
    val level = textOpt(node, "severity_text").getOrElse("")
    val body = textOpt(node, "_msg").getOrElse("")
    val traceId = textOpt(node, "trace_id").filter(_.nonEmpty)
    val spanId = textOpt(node, "span_id").filter(_.nonEmpty)
    val attributes = scala.collection.mutable.Map.empty[String, String]
    node.fields().asScala.foreach { e =>
      val k = e.getKey
      if (!LogReservedKeys.contains(k)) {
        attributes.put(k, e.getValue.asText(""))
      }
    }
    LogEntryResponse(timestampMs, level, body, traceId, spanId, attributes.toMap)
  }

  // ---- metrics (Prometheus query_range / query) ----------------------

  def parseMetrics(body: String, metricName: String): Either[GatewayError, MetricsQueryResponse] = {
    Try(mapper.readTree(body)) match {
      case Failure(e) => Left(bad(s"VictoriaMetrics: ${e.getMessage}"))
      case Success(root) =>
        val status = root.path("status").asText("")
        if (status != "success") {
          Left(bad(s"VictoriaMetrics: status='$status'"))
        } else {
          val series = root.path("data").path("result")
          val points = if (series.isArray && series.size() > 0) {
            // Named-metric templates produce a single time series; if a
            // backend ever returns more, we take the first deterministically.
            val first = series.get(0)
            val matrix = first.path("values")
            val vector = first.path("value")
            if (matrix.isArray) {
              matrix.iterator().asScala.take(MaxPageSize).flatMap(parseMetricPoint).toSeq
            } else if (vector.isArray) {
              parseMetricPoint(vector).toSeq
            } else {
              Seq.empty[MetricPoint]
            }
          } else {
            Seq.empty[MetricPoint]
          }
          Right(MetricsQueryResponse(metric = metricName, points = points))
        }
    }
  }

  private def parseMetricPoint(node: JsonNode): Option[MetricPoint] = {
    if (!node.isArray || node.size() < 2) None
    else {
      // Prometheus convention: [timestamp_seconds, "string_value"].
      val tsSec = node.get(0).asDouble(0.0)
      val rawValue = node.get(1).asText("")
      Try(rawValue.toDouble).toOption.map { d =>
        MetricPoint(timestampMs = math.round(tsSec * 1000.0), value = d)
      }
    }
  }

  // ---- traces (Jaeger Query API) -------------------------------------

  def parseTraces(body: String, traceId: String): Either[GatewayError, TracesGetResponse] = {
    Try(mapper.readTree(body)) match {
      case Failure(e) => Left(bad(s"Jaeger: ${e.getMessage}"))
      case Success(root) =>
        val data = root.path("data")
        val spans = if (data.isArray && data.size() > 0) {
          val spanArr = data.get(0).path("spans")
          if (spanArr.isArray) {
            spanArr.iterator().asScala.take(MaxPageSize).map(parseSpan).toSeq
          } else Seq.empty
        } else Seq.empty
        Right(TracesGetResponse(traceId = traceId, spans = spans))
    }
  }

  private def parseSpan(node: JsonNode): TraceSpanResponse = {
    val spanId = node.path("spanID").asText("")
    val name = node.path("operationName").asText("")
    val startUs = node.path("startTime").asLong(0L)
    val durationUs = node.path("duration").asLong(0L)
    val startMs = startUs / 1000L
    val endMs = (startUs + durationUs) / 1000L
    val parentSpanId = {
      val refs = node.path("references")
      if (refs.isArray) {
        refs.iterator().asScala
          .find(r => r.path("refType").asText("") == "CHILD_OF")
          .map(_.path("spanID").asText(""))
          .filter(_.nonEmpty)
      } else None
    }
    val attributes = scala.collection.mutable.Map.empty[String, String]
    val tags = node.path("tags")
    if (tags.isArray) {
      tags.iterator().asScala.foreach { t =>
        val k = t.path("key").asText("")
        // Per-field secret scrub: tag values are the only free-form,
        // potentially-sensitive data on a span. Sanitizing per value
        // (rather than whole-body) keeps the JSON intact for large traces.
        if (k.nonEmpty) attributes.put(k, LogSanitizer.sanitize(t.path("value").asText("")))
      }
    }
    TraceSpanResponse(spanId, parentSpanId, name, startMs, endMs, attributes.toMap)
  }

  // ---- profiles (Parca) ----------------------------------------------

  /**
   * Parca's primary query API is Connect-RPC at
   * /parca.query.v1alpha1.QueryService/Query. The gateway currently
   * hits a Prometheus-style path that returns Parca's SPA HTML for
   * the UI route, so in practice this parser sees non-JSON and
   * returns an empty profile. When the gateway is upgraded to call
   * the Connect-RPC endpoint, the same parser handles the flamegraph
   * report shape `{report:{flamegraph:{root, total, ...}}}`.
   */
  def parseProfiles(body: String): Either[GatewayError, ProfilesQueryResponse] = {
    val trimmed = body.trim
    if (trimmed.isEmpty || trimmed.charAt(0) != '{') {
      // Non-JSON (HTML SPA, empty body, ...). Not a parse error — just
      // no profile data. Pre-empts the JSON parser so we don't 502
      // on a backend that's serving its UI on the same port.
      Right(ProfilesQueryResponse(root = None, totalSamples = 0L))
    } else {
      Try(mapper.readTree(trimmed)) match {
        case Failure(e) => Left(bad(s"Parca: ${e.getMessage}"))
        case Success(root) =>
          val flame = {
            val nested = root.path("report").path("flamegraph")
            if (!nested.isMissingNode && !nested.isNull) nested
            else root.path("flamegraph")
          }
          if (flame.isMissingNode || flame.isNull) {
            Right(ProfilesQueryResponse(root = None, totalSamples = 0L))
          } else {
            val total = flame.path("total").asLong(0L)
            Right(ProfilesQueryResponse(root = parseFrame(flame.path("root")), totalSamples = total))
          }
      }
    }
  }

  private def parseFrame(node: JsonNode): Option[FlameFrame] = {
    if (node.isMissingNode || node.isNull) None
    else {
      val name = node.path("name").asText("")
      // Parca's flamegraph nodes use "cumulative" for total samples
      // under that frame; older shapes use "value". Accept either.
      val value =
        if (!node.path("cumulative").isMissingNode) node.path("cumulative").asLong(0L)
        else node.path("value").asLong(0L)
      val kids = node.path("children")
      val children =
        if (kids.isArray) kids.iterator().asScala.take(MaxPageSize).flatMap(parseFrame).toSeq
        else Seq.empty
      Some(FlameFrame(name, value, children))
    }
  }

  // ---- small helpers -------------------------------------------------

  private def textOpt(node: JsonNode, field: String): Option[String] = {
    val v = node.get(field)
    if (v == null || v.isNull) None else Some(v.asText(""))
  }

  private def toEpochMillis(iso: String): Option[Long] =
    Try(Instant.parse(iso).toEpochMilli).toOption
}
