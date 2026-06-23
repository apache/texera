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

import org.apache.texera.web.observability.gateway.dtos._

/**
  * Typed query builders. Each takes a validated DTO + the caller's
  * resolved scope and returns a backend-specific query string + the
  * query parameters that should accompany it.
  *
  * Security invariant: no field of the input DTO is concatenated into
  * the output query without first passing through a typed accessor.
  * Even free-text fields are emitted only as escaped *values* in the
  * query DSL — never as DSL syntax.
  *
  * Each builder is pure (no side effects, no I/O). Exhaustive
  * injection tests live in BuildersSpec.
  */

/** Tenancy / scope envelope. Computed by [[ObservabilityScope]];
  *  every builder consumes it so the caller cannot widen scope.
  */
case class GatewayScope(
    userId: Long,
    allowedWorkflowIds: Set[Long],
    allowedProjectIds: Set[Long]
) {

  /** Allowed list joined as the typed parameter to a backend query.
    *  Empty allowed-set yields "0" (a workflow id that cannot exist),
    *  which produces a zero-result query without breaking syntax.
    */
  def workflowIdsCsv: String = {
    if (allowedWorkflowIds.isEmpty) "0"
    else allowedWorkflowIds.toSeq.sorted.mkString(",")
  }

  /** Allowed list joined as a regex-alternation body (no anchors, no
    *  parens). For use inside a LogsQL stream filter as
    *  ``field=~"^(<body>)$"``. Empty allow-set yields "0" — a numeric
    *  literal that matches nothing real and keeps regex syntax valid.
    */
  def workflowIdsRegexAlt: String = {
    if (allowedWorkflowIds.isEmpty) "0"
    else allowedWorkflowIds.toSeq.sorted.mkString("|")
  }
}

object LogsQLBuilder {

  /** Build a LogsQL query for VictoriaLogs. Returned string is safe
    *  to ship as the ``query`` URL parameter to /select/logsql/query.
    *
    *  Layout: stream selector (label filters) then optional pipe
    *  filters for body text. Free text is passed only to the
    *  ``contains`` filter as a *value*, with backslashes and quotes
    *  escaped.
    */
  def build(req: ValidatedLogsRequest, scope: GatewayScope): String = {
    val sb = new StringBuilder
    // Start with the wildcard so a query without any filter clauses
    // is still valid LogsQL. The `*` matches every record; subsequent
    // `{field=...}` blocks narrow it. If a service filter is added,
    // it replaces the wildcard (the parenthesised stream selector
    // implies its own match set).
    var hasStreamFilter = false

    // Service filter. Two label keys exist in storage:
    //   * `service` — used by the seed scripts and any service that
    //     sets the field explicitly at ingest;
    //   * `service.name` — the OTel resource attribute name, used by
    //     anything bridged through the OTel collector (in particular
    //     the running JVM's Logback → OTel pipeline).
    //
    // When the user picks specific services, we OR the two label
    // variants so JVM-emitted records (service.name) and seed records
    // (service) both match. The OR must be parenthesised so subsequent
    // `{field=value}` filters apply to BOTH branches — otherwise
    // LogsQL binds the trailing filter only to the second branch and
    // a CU/workflow narrow leaks every record under `service=` past
    // the filter.
    //
    // When the user has NOT picked specific services, we skip the
    // service-key filter entirely. The earlier behaviour anchored on
    // `texera-*`, but the seed scripts emit names like
    // `dashboard-service` (no prefix) and only one running JVM
    // (texera-web) uses the prefix — so anchoring excluded almost
    // everything. The default view is meant to be "all logs the user
    // can see", which is what no-filter gives.
    if (req.services.nonEmpty) {
      val alt = req.services.iterator.map(_.value).mkString("|")
      sb.append(s"""(_stream:{service=~"^($alt)$$"} OR _stream:{service.name=~"^($alt)$$"})""")
      hasStreamFilter = true
    }

    // All non-service field filters go through the pipe-filter form
    // (`| filter key:value`) rather than the stream-selector form
    // (`{key=value}`). The stream form only matches when the field is
    // a configured stream label — which holds for seed records
    // (texera.workflow.id, texera.computing_unit.id are stream labels)
    // but NOT for OTel-bridged records (those keep the same fields as
    // record attributes only). Pipe filters match both, so the same
    // builder works against either data shape.
    //
    // Pipe filters must come AFTER the optional stream selector and
    // are buffered separately so we can wedge `*` in front when no
    // stream filter precedes them.
    val pipes = new StringBuilder
    // Option[Long]#foreach specializes to mcVJ$sp which unboxes the
    // value to Long via BoxesRunTime BEFORE the closure runs. Jackson
    // stores small JSON numbers as Integer regardless of the declared
    // type — the unbox then crashes with ClassCastException. We
    // launder through Option[Any] so the foreach uses the generic
    // apply path and keeps the value boxed.
    def appendId(field: String, id: Option[Long]): Unit =
      id.asInstanceOf[Option[Any]].foreach(v => pipes.append(s" | filter $field:$v"))
    appendId("texera.workflow.id", req.workflowId)
    appendId("texera.execution.id", req.executionId)
    appendId("texera.computing_unit.id", req.computingUnitId)
    appendId("texera.user.id", req.userId)
    req.level.foreach(l => pipes.append(s" | filter severity_text:${l.name}"))
    req.query.foreach { ft =>
      // Phrase filter on the message body. `contains_str(...)` is not
      // valid LogsQL (VictoriaLogs rejects it); a quoted phrase against
      // _msg is the correct form. escapeForLogsQL handles the \ and "
      // that the phrase literal needs.
      val escaped = escapeForLogsQL(ft.value)
      pipes.append(s""" | filter _msg:"$escaped"""")
    }

    // Workflow filter — applied only when the user explicitly picks a
    // workflow id. The earlier behaviour auto-applied the caller's
    // entire scope.workflowIdsRegexAlt as a stream-label filter, which
    // forced every matching record to carry a `texera.workflow.id`
    // stream label. JVM-emitted OTel records don't carry that label
    // (only attribute), so the default view excluded all live
    // microservice logs — count stayed at the seed-only number even
    // while workflows were running. Explicit pick is still authorised
    // via ScopeResolver.assertWorkflowAllowed before we get here.
    // ClassCastException trap: Jackson Scala deserializes JSON numbers
    // that fit in 32 bits as java.lang.Integer, regardless of the
    // declared Option[Long] type parameter (the parameter is erased on
    // the JVM). A typed `id: Long` closure then unboxes via
    // BoxesRunTime.unboxToLong and fails with Integer→Long cast. We
    // sidestep the issue by treating each id as a String — the field
    // is numeric in storage and string interpolation works either way.
    // No stream filter at all? Prepend the wildcard so LogsQL has
    // something to match against — `| filter ...` or `| sort by ...`
    // alone is a parse error. The wildcard runs across every stream,
    // which is exactly "show me everything" for the default tab.
    if (!hasStreamFilter) sb.insert(0, "*")

    // Now drop the buffered pipe filters in.
    sb.append(pipes)

    // Sort pipe — uses LogsQL's `| sort by(...)` syntax. Field names
    // come from the closed [[LogSort]] enum so no client input
    // reaches the pipe.
    sb.append(" ").append(sortPipe(req.sort))

    // Pagination: `| offset N` skips records, `| limit M` caps the
    // page. The offset is a non-negative Long parsed from the
    // pageCursor; the validator clamps it.
    if (req.offset > 0L) sb.append(s" | offset ${req.offset}")
    sb.append(s" | limit ${req.pageSize.value}")
    sb.toString
  }

  /** LogsQL `| sort by (...)` fragment for the requested order. */
  private[gateway] def sortPipe(sort: LogSort): String =
    sort match {
      case LogSort.NewestFirst  => "| sort by (_time desc)"
      case LogSort.OldestFirst  => "| sort by (_time asc)"
      case LogSort.SeverityHigh => "| sort by (severity_number desc, _time desc)"
      case LogSort.ServiceAsc   => "| sort by (service asc, _time desc)"
    }

  /** Escape a value for embedding inside a LogsQL double-quoted
    *  string. Backslash and double-quote are the only metacharacters
    *  we need to handle.
    */
  private[gateway] def escapeForLogsQL(value: String): String = {
    val out = new StringBuilder(value.length + 8)
    var i = 0
    while (i < value.length) {
      val c = value.charAt(i)
      if (c == '\\' || c == '"') out.append('\\')
      out.append(c)
      i += 1
    }
    out.toString
  }
}

object MetricsQLBuilder {

  /** Server-side templates for the named queries we expose. The
    *  client picks the name; we substitute only the validated
    *  step + window parameters. There is no public path for the
    *  client to supply raw MetricsQL.
    */
  def build(req: ValidatedMetricsRequest): String = {
    // Per-bucket lookback window for the rate()/increase() family. Tied
    // to the caller's step so each plotted point summarises its own bucket.
    val w = s"${req.stepSec}s"
    // The completions counter carries texera_outcome={success|failure|…};
    // there is NO texera_workflow_failures_total series. Failure rate is
    // therefore derived as non-success ÷ all completions.
    val completions = "texera_workflow_completions_total"
    val durBucket = "texera_workflow_duration_seconds_bucket"
    req.metric match {
      case NamedMetric.RunsPerDay =>
        // Workflow starts in the trailing 24h, evaluated at each point.
        // increase() (a count) — NOT rate() (per-second), which the earlier
        // template used and so reported a value ~86400× too small.
        "sum(increase(texera_workflow_starts_total[1d]))"
      case NamedMetric.TotalRuns =>
        // Starts within each step bucket. The UI sums the series to show
        // the absolute run count over the whole selected window.
        s"sum(increase(texera_workflow_starts_total[$w]))"
      case NamedMetric.ActiveWorkflows =>
        // Live up-down gauge, summed across computing units.
        "sum(texera_workflow_active)"
      case NamedMetric.SuccessRate =>
        // % of completions that succeeded. `or vector(0)` so a window with
        // completions but zero successes reports 0%, not an empty series.
        s"""100 * (sum(rate($completions{texera_outcome="success"}[$w])) or vector(0)) / sum(rate($completions[$w]))"""
      case NamedMetric.FailureRate =>
        // % of completions that did NOT succeed (errored/killed/…). The
        // complement of success rate; `or vector(0)` for the no-failures case.
        s"""100 * (sum(rate($completions{texera_outcome!="success"}[$w])) or vector(0)) / sum(rate($completions[$w]))"""
      case NamedMetric.AvgDuration =>
        // Mean run duration (s): Σduration ÷ Σcount over the bucket.
        s"sum(rate(texera_workflow_duration_seconds_sum[$w])) / sum(rate(texera_workflow_duration_seconds_count[$w]))"
      case NamedMetric.P50Duration =>
        s"histogram_quantile(0.50, sum(rate($durBucket[$w])) by (le))"
      case NamedMetric.P95Duration =>
        // `sum(...) by (le)` is required: histogram_quantile needs the
        // bucket counts aggregated by the `le` label. The earlier template
        // omitted it, which is undefined across multiple bucket series.
        s"histogram_quantile(0.95, sum(rate($durBucket[$w])) by (le))"
      case NamedMetric.P99Duration =>
        s"histogram_quantile(0.99, sum(rate($durBucket[$w])) by (le))"
    }
  }
}
