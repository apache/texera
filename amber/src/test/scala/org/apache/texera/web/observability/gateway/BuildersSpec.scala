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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class BuildersSpec extends AnyFlatSpec with Matchers {

  private val anyWindow = TimeWindow(
    Instant.ofEpochMilli(0L),
    Instant.ofEpochMilli(60_000L)
  )

  private val scope =
    GatewayScope(userId = 42L, allowedWorkflowIds = Set(7L, 8L), allowedProjectIds = Set(1L))

  /** Convenience constructor so each LogsQL test can specify only the
    *  fields under test. Defaults are unconstrained (no workflow id,
    *  no CU id, no service filter, no level/query).
    */
  private def logsReq(
      workflowId: Option[Long] = None,
      executionId: Option[Long] = None,
      computingUnitId: Option[Long] = None,
      userId: Option[Long] = None,
      services: Seq[ServiceName] = Seq.empty,
      level: Option[LogLevel] = None,
      query: Option[FreeText] = None,
      sort: LogSort = LogSort.Default,
      pageSize: PageSize = PageSize(100),
      offset: Long = 0L
  ): ValidatedLogsRequest =
    ValidatedLogsRequest(
      workflowId = workflowId,
      executionId = executionId,
      computingUnitId = computingUnitId,
      userId = userId,
      services = services,
      level = level,
      query = query,
      sort = sort,
      window = anyWindow,
      pageSize = pageSize,
      offset = offset,
      pageCursor = None
    )

  // ----- LogsQLBuilder injection resistance ----------------------------

  "LogsQLBuilder" should "default to the wildcard `*` filter when nothing is picked (so all services surface)" in {
    val q = LogsQLBuilder.build(logsReq(), scope)
    // No service filter, no workflow filter, no level → match all
    // streams. The earlier behaviour anchored on `^texera-.*$` which
    // only matched the `texera-web` JVM and excluded every other
    // microservice that the seed scripts wrote with names like
    // `dashboard-service` / `config-service`.
    q should startWith("*")
    q should include("| sort by")
    q should include("| limit 100")
    q should not include "_stream:"
  }

  it should "filter on a specific workflow id when supplied (asserts already enforce membership)" in {
    val q = LogsQLBuilder.build(logsReq(workflowId = Some(7L)), scope)
    // Pipe filter, not a stream filter — matches both stream labels
    // (seed records) and record attributes (OTel-emitted records).
    // The earlier `{texera.workflow.id=7}` stream-filter form silently
    // missed every OTel record because workflow.id is only an
    // attribute there.
    q should include("| filter texera.workflow.id:7")
    // Without a stream filter, the wildcard prefix appears so VL has
    // something to match against before the pipe filter runs.
    q should startWith("*")
  }

  it should "NOT add a workflow-id filter when none is picked (so JVM logs without that label still surface)" in {
    val q = LogsQLBuilder.build(logsReq(), scope)
    // The earlier behaviour auto-applied scope.workflowIdsRegexAlt
    // here, which excluded every record that didn't carry a
    // texera.workflow.id stream label. That filter only belongs when
    // the user explicitly picks a workflow.
    q should not include "texera.workflow.id"
  }

  it should "escape backslashes and double-quotes inside free-text query" in {
    val q = LogsQLBuilder.build(
      logsReq(query = Some(FreeText("""evil"} | drop ; --"""))),
      scope
    )
    // The closing brace + pipe + drop attempt stay inside the quoted
    // _msg value, not turned into LogsQL syntax.
    q should include("""| filter _msg:"evil\"} | drop ; --"""")
  }

  it should "place pipe filters AFTER the stream selector so the wildcard prefix has scope" in {
    // The wildcard `*` needs to come before any `| filter ...` pipe —
    // otherwise LogsQL has nothing to filter against. The pipe filters
    // are buffered separately during build and appended at the end.
    val q = LogsQLBuilder.build(
      logsReq(level = Some(LogLevel.ERROR), workflowId = Some(7L)),
      scope
    )
    val wildIdx = q.indexOf("*")
    val firstFilter = q.indexOf("| filter")
    wildIdx shouldBe 0
    firstFilter should be > wildIdx
  }

  it should "fall back to wildcard match when the user's allow-set is empty" in {
    val emptyScope = GatewayScope(0L, Set.empty, Set.empty)
    val q = LogsQLBuilder.build(logsReq(), emptyScope)
    q should startWith("*")
    q should not include "texera.workflow.id"
  }

  it should "splat LogLevel enum value verbatim because the enum is closed" in {
    val q = LogsQLBuilder.build(logsReq(level = Some(LogLevel.ERROR)), scope)
    // Level filter targets the severity_text attribute, not a stream
    // label — that attribute is set by both seed records and the OTel
    // log appender, so pipe-form filter matches both.
    q should include("| filter severity_text:ERROR")
  }

  it should "filter on a specific computing-unit id when supplied" in {
    val q = LogsQLBuilder.build(logsReq(computingUnitId = Some(42L)), scope)
    q should include("| filter texera.computing_unit.id:42")
  }

  it should "filter on a specific user id when supplied" in {
    val q = LogsQLBuilder.build(logsReq(userId = Some(1L)), scope)
    q should include("| filter texera.user.id:1")
  }

  it should "narrow the service stream filter when the request picks one or more services" in {
    val q = LogsQLBuilder.build(
      logsReq(services = Seq(ServiceName("dashboard-service"), ServiceName("texera-web"))),
      scope
    )
    // The default `service=~"texera-.*"` filter is REPLACED so a
    // request for `dashboard-service` actually narrows storage.
    // The same regex is applied against both label keys (see
    // LogsQLBuilder.build comment) so JVM-emitted records under
    // `service.name` still surface.
    q should include("""_stream:{service=~"^(dashboard-service|texera-web)$"}""")
    q should include("""_stream:{service.name=~"^(dashboard-service|texera-web)$"}""")
    q should not include "texera-.*"
  }

  it should "wrap the service OR in parens so trailing filters apply to BOTH branches (CU narrow regression)" in {
    // Without the parens, LogsQL binds the trailing CU filter only
    // to the second (`service.name`) branch, and every record under
    // `service=` passes through the CU filter unchanged — the panel
    // returns ALL CUs even though the user picked CU 7. With pipe
    // filters that's less of an issue (the pipe runs over the joined
    // stream selection), but we keep the parens to make the
    // generated query unambiguous.
    val q = LogsQLBuilder.build(
      logsReq(
        services = Seq(ServiceName("dashboard-service")),
        computingUnitId = Some(7L)
      ),
      scope
    )
    val parenOpenIdx = q.indexOf("(_stream:")
    val parenCloseIdx = q.indexOf(")")
    val cuFilterIdx = q.indexOf("| filter texera.computing_unit.id:7")
    parenOpenIdx should be >= 0
    parenCloseIdx should be > parenOpenIdx
    cuFilterIdx should be > parenCloseIdx
  }

  it should "render each LogSort enum value as its own | sort by pipe" in {
    LogSort.all.foreach { s =>
      val q = LogsQLBuilder.build(logsReq(sort = s), scope)
      withClue(s"sort=$s, query=$q ") {
        q should include("| sort by")
      }
    }
    LogsQLBuilder.build(logsReq(sort = LogSort.NewestFirst), scope) should include("_time desc")
    LogsQLBuilder.build(logsReq(sort = LogSort.OldestFirst), scope) should include("_time asc")
    LogsQLBuilder.build(logsReq(sort = LogSort.SeverityHigh), scope) should include(
      "severity_number desc"
    )
    LogsQLBuilder.build(logsReq(sort = LogSort.ServiceAsc), scope) should include("service asc")
  }

  it should "append `| offset N` only when offset > 0 (so first page stays clean)" in {
    val first = LogsQLBuilder.build(logsReq(offset = 0L), scope)
    val second = LogsQLBuilder.build(logsReq(offset = 200L, pageSize = PageSize(200)), scope)
    first should not include "| offset"
    second should include("| offset 200")
    // limit always appears, regardless of offset.
    first should include("| limit 100")
    second should include("| limit 200")
  }

  it should "tolerate java.lang.Integer ids in Option[Long] without ClassCastException (regression)" in {
    // Jackson Scala deserializes small JSON numbers as Integer even
    // when the case-class field is declared Option[Long]. The earlier
    // builder used `Option#foreach(id: Long => ...)` and BoxesRunTime
    // crashed with ClassCastException at the first interpolation.
    // We dodge it by routing all ids through Object.toString.
    val intInOption: Option[Long] = Some(java.lang.Integer.valueOf(7).asInstanceOf[Long])
    noException should be thrownBy {
      LogsQLBuilder.build(logsReq(computingUnitId = intInOption), scope)
    }
  }

  it should "reject forged service names containing LogsQL syntax via ServiceName.parse" in {
    ServiceName.parse("""evil"} | drop""") shouldBe None
    ServiceName.parse("with spaces") shouldBe None
    ServiceName.parse("a" * 65) shouldBe None
    ServiceName.parse("") shouldBe None
    ServiceName.parse("dashboard-service") shouldBe Some(ServiceName("dashboard-service"))
    // We normalise to lowercase before pattern-matching — uppercase
    // input is accepted (the resulting value is lowercased), which
    // keeps the typed value usable in a LogsQL stream filter.
    ServiceName.parse("Dashboard-Service") shouldBe Some(ServiceName("dashboard-service"))
  }

  // ----- MetricsQLBuilder -----------------------------------------------

  "MetricsQLBuilder" should "build a non-empty fixed template for every named metric" in {
    for (m <- NamedMetric.all) {
      val q = MetricsQLBuilder.build(ValidatedMetricsRequest(m, anyWindow, stepSec = 60))
      withClue(s"metric=${m.name} ") { q.trim should not be empty }
    }
  }

  it should "derive failure rate from the completions counter (there is no failures_total series)" in {
    val q = MetricsQLBuilder.build(ValidatedMetricsRequest(NamedMetric.FailureRate, anyWindow, 60))
    q should not include "texera_workflow_failures_total"
    q should include("""texera_workflow_completions_total{texera_outcome!="success"}""")
    q should startWith("100 *") // expressed as a percentage
  }

  it should "aggregate histogram buckets by (le) for quantile metrics" in {
    for (m <- Seq(NamedMetric.P50Duration, NamedMetric.P95Duration, NamedMetric.P99Duration)) {
      val q = MetricsQLBuilder.build(ValidatedMetricsRequest(m, anyWindow, 60))
      withClue(s"metric=${m.name}, query=$q ") {
        q should include("histogram_quantile(")
        q should include("sum(rate(texera_workflow_duration_seconds_bucket[60s])) by (le)")
      }
    }
  }

  it should "never accept a metric name from the client (only the closed enum)" in {
    // NamedMetric.parse rejects unknown strings.
    NamedMetric.parse("rate(evil[1h])") shouldBe None
    NamedMetric.parse("../../../etc/passwd") shouldBe None
    NamedMetric.parse("runsPerDay") shouldBe Some(NamedMetric.RunsPerDay)
  }

  it should "produce only relative durations inside subquery ranges (regression: HTTP 422)" in {
    // The previous RunsPerDay template embedded an absolute Unix timestamp
    // as a subquery range, e.g. `[1779876877s:60s]`. Prometheus interprets
    // that as "the last 56 years" and 422s. Guard the family of templates
    // against any reintroduction by asserting no 10-digit second literal
    // ever appears inside a `[...:...s]` block.
    val tenDigitSecondsInRange = """\[\d{10,}s:""".r
    val window = TimeWindow(
      Instant.ofEpochMilli(1700000000_000L),
      Instant.ofEpochMilli(1700000060_000L)
    )
    for (m <- NamedMetric.all) {
      val q = MetricsQLBuilder.build(ValidatedMetricsRequest(m, window, stepSec = 60))
      withClue(s"metric=${m.name}, query=$q ") {
        tenDigitSecondsInRange.findFirstIn(q) shouldBe None
      }
    }
  }

  it should "count trailing-24h starts for RunsPerDay (increase, not per-second rate)" in {
    val validated = ValidatedMetricsRequest(
      metric = NamedMetric.RunsPerDay,
      window = anyWindow,
      stepSec = 60
    )
    // Must be increase()/count over [1d] — rate() would report a per-second
    // value ~86400× too small for a "runs per day" card.
    MetricsQLBuilder.build(validated) shouldBe "sum(increase(texera_workflow_starts_total[1d]))"
  }

  it should "mark TotalRuns dbBacked and keep a single-window increase() fallback template" in {
    // Live path is the exact DB count (dbBacked) in WorkflowRunCounter. The
    // MetricsQL template is retained as a metrics-only estimate: ONE
    // increase() over the whole window (anyWindow spans 60s, step floors the
    // range to 60s), never a sum of per-step buckets.
    val validated = ValidatedMetricsRequest(NamedMetric.TotalRuns, anyWindow, stepSec = 60)
    MetricsQLBuilder.build(validated) shouldBe "sum(increase(texera_workflow_starts_total[60s]))"
    NamedMetric.TotalRuns.dbBacked shouldBe true
    NamedMetric.TotalRuns.instant shouldBe true
    NamedMetric.RunsPerDay.dbBacked shouldBe false
    NamedMetric.RunsPerDay.instant shouldBe false
  }

  // ----- JaegerQueryBuilder --------------------------------------------

  "JaegerQueryBuilder" should "embed the validated trace id directly into the path" in {
    val v = ValidatedTracesGetRequest("0af7651916cd43dd8448eb211c80319c")
    JaegerQueryBuilder.tracePath(v) shouldBe "/api/traces/0af7651916cd43dd8448eb211c80319c"
  }

}
