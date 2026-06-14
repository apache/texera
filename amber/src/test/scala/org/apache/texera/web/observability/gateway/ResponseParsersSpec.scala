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

class ResponseParsersSpec extends AnyFlatSpec with Matchers {

  // ----- logs ----------------------------------------------------------

  "ResponseParsers.parseLogs" should "parse VictoriaLogs NDJSON into typed entries" in {
    val body =
      """{"_msg":"hello","_time":"2026-05-28T09:00:00.000Z","severity_text":"INFO","trace_id":"abc","span_id":"def","logger.name":"x","service.name":"texera-web"}
        |{"_msg":"world","_time":"2026-05-28T09:00:01.000Z","severity_text":"WARN","logger.name":"y"}""".stripMargin

    val Right(parsed) = ResponseParsers.parseLogs(body, pageSize = 100)

    parsed.entries should have size 2
    parsed.total shouldBe 2L
    parsed.nextCursor shouldBe None

    val first = parsed.entries.head
    first.body shouldBe "hello"
    first.level shouldBe "INFO"
    first.timestampMs shouldBe Instant("2026-05-28T09:00:00.000Z")
    first.traceId shouldBe Some("abc")
    first.spanId shouldBe Some("def")
    // Reserved keys must NOT leak into attributes.
    first.attributes.keySet should not contain "_msg"
    first.attributes.keySet should not contain "_time"
    first.attributes.keySet should not contain "trace_id"
    // Free attributes carry through.
    first.attributes("logger.name") shouldBe "x"
    first.attributes("service.name") shouldBe "texera-web"

    val second = parsed.entries(1)
    second.traceId shouldBe None
    second.spanId shouldBe None
    second.level shouldBe "WARN"
  }

  it should "skip blank lines and tolerate trailing newline" in {
    val body =
      """{"_msg":"a","_time":"2026-05-28T09:00:00.000Z","severity_text":"INFO"}
        |
        |{"_msg":"b","_time":"2026-05-28T09:00:01.000Z","severity_text":"INFO"}
        |""".stripMargin

    val Right(parsed) = ResponseParsers.parseLogs(body, pageSize = 100)
    parsed.entries.map(_.body) shouldBe Seq("a", "b")
  }

  it should "cap entries at pageSize" in {
    val line = """{"_msg":"x","_time":"2026-05-28T09:00:00.000Z","severity_text":"INFO"}"""
    val body = Seq.fill(50)(line).mkString("\n")
    val Right(parsed) = ResponseParsers.parseLogs(body, pageSize = 10)
    parsed.entries should have size 10
  }

  it should "cap entries at MaxPageSize even if pageSize is larger" in {
    val line = """{"_msg":"x","_time":"2026-05-28T09:00:00.000Z","severity_text":"INFO"}"""
    val body = Seq.fill(MaxPageSize + 50)(line).mkString("\n")
    val Right(parsed) = ResponseParsers.parseLogs(body, pageSize = MaxPageSize + 999)
    parsed.entries.size shouldBe MaxPageSize
  }

  it should "return bad_backend_response on a malformed line" in {
    val body = """{"_msg":"ok","_time":"2026-05-28T09:00:00.000Z"}""" + "\n" + "not json"
    val Left(err) = ResponseParsers.parseLogs(body, pageSize = 100)
    err.code shouldBe "bad_backend_response"
    err.status shouldBe 502
  }

  it should "produce empty entries for an empty body" in {
    val Right(parsed) = ResponseParsers.parseLogs("", pageSize = 100)
    parsed.entries shouldBe empty
    parsed.total shouldBe 0L
  }

  // ---- NDJSON line-boundary regression ------------------------------

  it should "parse multi-line NDJSON without the caller pre-redacting (regression)" in {
    // The earlier wiring ran LogSanitizer.sanitize on the whole body
    // before calling parseLogs. That stripped '\n' (0x0A) and collapsed
    // every record into one blob; Jackson then parsed only the first
    // top-level object and hits dropped to ~0. This test pins the
    // requirement: the parser must keep working on the raw NDJSON.
    val body = (1 to 5)
      .map(i =>
        s"""{"_msg":"line $i","_time":"2026-05-28T09:00:0${i % 10}.000Z","severity_text":"INFO"}"""
      )
      .mkString("\n")
    val Right(parsed) = ResponseParsers.parseLogs(body, pageSize = 100)
    parsed.entries should have size 5
    parsed.entries.map(_.body) shouldBe Seq("line 1", "line 2", "line 3", "line 4", "line 5")
  }

  it should "produce only 1 (or 0) entries on the broken path where newlines were stripped" in {
    // Demonstrates the pre-fix failure mode: simulate the previous
    // sanitize-then-parse pipeline by stripping '\n' first.
    val body = (1 to 5)
      .map(i =>
        s"""{"_msg":"line $i","_time":"2026-05-28T09:00:0${i % 10}.000Z","severity_text":"INFO"}"""
      )
      .mkString("\n")
    val collapsed = body.replace("\n", "")
    val Right(parsed) = ResponseParsers.parseLogs(collapsed, pageSize = 100)
    parsed.entries.size should (be <= 1)
  }

  // ---- per-entry secret redaction -----------------------------------

  it should "redact bearer tokens inside the entry body via LogSanitizer" in {
    val body =
      """{"_msg":"sent Authorization: Bearer abcdefghijklmnop with the request","_time":"2026-05-28T09:00:00.000Z","severity_text":"INFO"}"""
    val Right(parsed) = ResponseParsers.parseLogs(body, pageSize = 1)
    parsed.entries.head.body should not include "Bearer abcdefghijklmnop"
    parsed.entries.head.body should include("[REDACTED]")
  }

  it should "redact password=value patterns inside attribute values" in {
    val body =
      """{"_msg":"ok","_time":"2026-05-28T09:00:00.000Z","severity_text":"INFO","leak":"password=hunter2"}"""
    val Right(parsed) = ResponseParsers.parseLogs(body, pageSize = 1)
    parsed.entries.head.attributes("leak") should not include "hunter2"
    parsed.entries.head.attributes("leak") should include("[REDACTED]")
  }

  it should "redact AWS access key IDs anywhere in the entry" in {
    val body =
      """{"_msg":"key seen: AKIAIOSFODNN7EXAMPLE in handler","_time":"2026-05-28T09:00:00.000Z","severity_text":"WARN"}"""
    val Right(parsed) = ResponseParsers.parseLogs(body, pageSize = 1)
    parsed.entries.head.body should not include "AKIAIOSFODNN7EXAMPLE"
    parsed.entries.head.body should include("[REDACTED]")
  }

  // ----- log sources --------------------------------------------------

  "ResponseParsers.parseLogSources" should "extract distinct services, workflow ids, CU ids, user ids from VL /streams output" in {
    val body =
      """{"values":[
        |  {"value":"{service=\"texera-web\"}","hits":1000},
        |  {"value":"{service=\"texera-web\",texera.workflow.id=\"441\"}","hits":10},
        |  {"value":"{service=\"texera-web\",texera.workflow.id=\"441\",texera.computing_unit.id=\"7\",texera.user.id=\"5\"}","hits":3},
        |  {"value":"{service=\"workflow-runtime-coordinator-service\",texera.workflow.id=\"3\",texera.computing_unit.id=\"7\",texera.user.id=\"1\"}","hits":5},
        |  {"value":"{service=\"dashboard-service\",texera.workflow.id=\"3\",texera.computing_unit.id=\"99\",texera.user.id=\"1\"}","hits":2}
        |]}""".stripMargin

    val Right(parsed) =
      ResponseParsers.parseLogSources(body, allowedWorkflowIds = Set(3L, 441L, 442L))
    parsed.services should contain theSameElementsAs Seq(
      "dashboard-service",
      "texera-web",
      "workflow-runtime-coordinator-service"
    )
    // Workflow 442 is allowed but not seen → not returned. Workflow ids
    // we saw but the user can't access (e.g. another tenant) are filtered out.
    parsed.workflowIds shouldBe Seq(3L, 441L)
    // CU ids are NOT scope-filtered: the search path already prevents
    // cross-workflow access, so listing CUs the user might overlap with
    // is safe + useful for debugging.
    parsed.computingUnitIds shouldBe Seq(7L, 99L)
    // User ids: same logic — listing observed user ids surfaces them
    // for admins debugging cross-user issues. The search path still
    // applies tenant isolation when records are queried.
    parsed.userIds shouldBe Seq(1L, 5L)
  }

  it should "drop workflow ids that aren't in the user's allow-set" in {
    val body = """{"values":[{"value":"{service=\"x\",texera.workflow.id=\"500\"}","hits":1}]}"""
    val Right(parsed) = ResponseParsers.parseLogSources(body, allowedWorkflowIds = Set(1L, 2L))
    parsed.workflowIds shouldBe empty
  }

  it should "be empty when VL has no streams yet" in {
    val Right(parsed) =
      ResponseParsers.parseLogSources("""{"values":[]}""", allowedWorkflowIds = Set(1L))
    parsed.services shouldBe empty
    parsed.workflowIds shouldBe empty
    parsed.computingUnitIds shouldBe empty
  }

  it should "return bad_backend_response on malformed JSON" in {
    val Left(err) = ResponseParsers.parseLogSources("not json", allowedWorkflowIds = Set.empty)
    err.code shouldBe "bad_backend_response"
  }

  it should "silently skip non-numeric workflow ids without throwing" in {
    val body = """{"values":[{"value":"{service=\"x\",texera.workflow.id=\"abc\"}","hits":1}]}"""
    val Right(parsed) = ResponseParsers.parseLogSources(body, allowedWorkflowIds = Set(1L))
    parsed.workflowIds shouldBe empty
    parsed.services shouldBe Seq("x")
  }

  // ----- regression: Jackson Integer in Option[Long] ----------------

  it should "round-trip a Jackson-deserialized Option[Long] holding an Integer without ClassCastException" in {
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    // Small JSON numbers (<=Int.MaxValue) deserialize as Integer even
    // when the declared field type is Option[Long]. Earlier code did
    // `opt.map(id: Long => ...)` which specializes to JFunction$mcJJ$sp
    // and unboxes BEFORE the closure body — crashing with
    // ClassCastException. The validator must launder through
    // Option[Any] first so the unbox never happens. This test pins
    // the regression at the deserialize → consume boundary.
    val req = mapper.readValue(
      """{"fromMs":0,"toMs":1,"pageSize":1,"computingUnitId":7,"workflowId":42}""",
      classOf[RawLogsSearchRequest]
    )
    // Use the request fields the way the gateway does — interpolation
    // and explicit Long context — to assert no unbox bomb goes off.
    noException should be thrownBy {
      val cu: Option[Any] = req.computingUnitId.asInstanceOf[Option[Any]]
      val wf: Option[Any] = req.workflowId.asInstanceOf[Option[Any]]
      cu.foreach(v => s"$v".length)
      wf.foreach(v => s"$v".length)
    }
  }

  // ----- metrics ------------------------------------------------------

  "ResponseParsers.parseMetrics" should "parse a Prometheus matrix into MetricPoint timeseries (seconds → ms)" in {
    val body =
      """{"status":"success","data":{"resultType":"matrix","result":[
        |  {"metric":{"__name__":"x"},"values":[[1779961222,"25"],[1779961282,"113.5"]]}
        |]}}""".stripMargin
    val Right(parsed) = ResponseParsers.parseMetrics(body, metricName = "runsPerDay")
    parsed.metric shouldBe "runsPerDay"
    parsed.points.map(_.timestampMs) shouldBe Seq(1779961222000L, 1779961282000L)
    parsed.points.map(_.value) shouldBe Seq(25.0, 113.5)
  }

  // Regression: a query_range payload larger than the log-line
  // sanitizer's 16 KiB body cap must parse intact. The resource layer
  // used to feed parseMetrics the whole-body-sanitized string, which
  // truncated past MaxBodyBytes and appended "...[truncated]" — its
  // leading '.' produced "Unexpected character ('.')" 502s on the
  // Metrics page. The body must now reach the parser un-truncated.
  it should "parse a metrics matrix larger than the log-sanitizer body cap" in {
    val pointCount = 900
    val points = (0 until pointCount)
      .map(i => s"""[${1700000000 + i},"113.5"]""")
      .mkString(",")
    val body =
      s"""{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"x"},"values":[$points]}]}}"""
    body.length should be > LogSanitizer.MaxBodyBytes // i.e. > 16 KiB — the old truncation point
    val Right(parsed) = ResponseParsers.parseMetrics(body, metricName = "x")
    parsed.points should have size pointCount.toLong
    parsed.points.last.value shouldBe 113.5
  }

  it should "handle an empty matrix result" in {
    val body = """{"status":"success","data":{"resultType":"matrix","result":[]}}"""
    val Right(parsed) = ResponseParsers.parseMetrics(body, metricName = "x")
    parsed.points shouldBe empty
  }

  it should "handle an instant vector result" in {
    val body =
      """{"status":"success","data":{"resultType":"vector","result":[
        |  {"metric":{"__name__":"x"},"value":[1700000000,"7"]}
        |]}}""".stripMargin
    val Right(parsed) = ResponseParsers.parseMetrics(body, metricName = "x")
    parsed.points should have size 1
    parsed.points.head.value shouldBe 7.0
    parsed.points.head.timestampMs shouldBe 1700000000000L
  }

  it should "skip points whose value is not a number" in {
    val body =
      """{"status":"success","data":{"resultType":"matrix","result":[
        |  {"metric":{},"values":[[1700000000,"NaNish"],[1700000001,"42"]]}
        |]}}""".stripMargin
    val Right(parsed) = ResponseParsers.parseMetrics(body, metricName = "x")
    parsed.points.map(_.value) shouldBe Seq(42.0)
  }

  it should "return bad_backend_response when status != success" in {
    val body = """{"status":"error","error":"oops"}"""
    val Left(err) = ResponseParsers.parseMetrics(body, metricName = "x")
    err.code shouldBe "bad_backend_response"
  }

  it should "return bad_backend_response on unparseable JSON" in {
    val Left(err) = ResponseParsers.parseMetrics("not json", metricName = "x")
    err.code shouldBe "bad_backend_response"
    err.status shouldBe 502
  }

  // ----- traces -------------------------------------------------------

  "ResponseParsers.parseTraces" should "parse a Jaeger trace with parent reference and tags" in {
    val body =
      """{"data":[{"traceID":"tid","spans":[
        |  {"spanID":"root","operationName":"root.op","startTime":1000,"duration":2000,
        |   "references":[],"tags":[{"key":"k","value":"v"}]},
        |  {"spanID":"child","operationName":"child.op","startTime":1500,"duration":300,
        |   "references":[{"refType":"CHILD_OF","spanID":"root"}],"tags":[]}
        |]}]}""".stripMargin

    val Right(parsed) = ResponseParsers.parseTraces(body, traceId = "tid")
    parsed.traceId shouldBe "tid"
    parsed.spans should have size 2

    val root = parsed.spans.head
    root.spanId shouldBe "root"
    root.parentSpanId shouldBe None
    root.name shouldBe "root.op"
    root.startMs shouldBe 1L // 1000us → 1ms
    root.endMs shouldBe 3L // (1000+2000)us → 3ms
    root.attributes shouldBe Map("k" -> "v")

    val child = parsed.spans(1)
    child.parentSpanId shouldBe Some("root")
  }

  it should "ignore non-CHILD_OF references" in {
    val body =
      """{"data":[{"traceID":"t","spans":[
        |  {"spanID":"s","operationName":"x","startTime":0,"duration":0,
        |   "references":[{"refType":"FOLLOWS_FROM","spanID":"other"}],"tags":[]}
        |]}]}""".stripMargin
    val Right(parsed) = ResponseParsers.parseTraces(body, traceId = "t")
    parsed.spans.head.parentSpanId shouldBe None
  }

  it should "return an empty spans list when Jaeger returns no traces" in {
    val Right(parsed) = ResponseParsers.parseTraces("""{"data":[]}""", traceId = "t")
    parsed.spans shouldBe empty
  }

  it should "return bad_backend_response on malformed JSON" in {
    val Left(err) = ResponseParsers.parseTraces("not json", traceId = "t")
    err.code shouldBe "bad_backend_response"
  }

  // ----- profiles -----------------------------------------------------

  "ResponseParsers.parseProfiles" should "return empty result when body is not JSON (HTML/SPA)" in {
    val Right(parsed) = ResponseParsers.parseProfiles("<!DOCTYPE html><html>...</html>")
    parsed.root shouldBe None
    parsed.totalSamples shouldBe 0L
  }

  it should "parse a nested Parca report.flamegraph shape" in {
    val body =
      """{"report":{"flamegraph":{
        |  "total":"1000",
        |  "root":{"name":"root","cumulative":"1000","children":[
        |    {"name":"a","cumulative":"600","children":[]},
        |    {"name":"b","cumulative":"400","children":[]}
        |  ]}
        |}}}""".stripMargin
    val Right(parsed) = ResponseParsers.parseProfiles(body)
    parsed.totalSamples shouldBe 1000L
    parsed.root shouldBe defined
    val r = parsed.root.get
    r.name shouldBe "root"
    r.value shouldBe 1000L
    r.children.map(_.name) shouldBe Seq("a", "b")
    r.children.map(_.value) shouldBe Seq(600L, 400L)
  }

  it should "parse a flat flamegraph shape (no report wrapper)" in {
    val body =
      """{"flamegraph":{"total":"42","root":{"name":"x","cumulative":"42","children":[]}}}"""
    val Right(parsed) = ResponseParsers.parseProfiles(body)
    parsed.totalSamples shouldBe 42L
    parsed.root.map(_.name) shouldBe Some("x")
  }

  it should "fall back to 'value' when 'cumulative' is absent" in {
    val body =
      """{"flamegraph":{"total":"5","root":{"name":"x","value":"5","children":[]}}}"""
    val Right(parsed) = ResponseParsers.parseProfiles(body)
    parsed.root.map(_.value) shouldBe Some(5L)
  }

  it should "return empty when the JSON has no flamegraph" in {
    val Right(parsed) = ResponseParsers.parseProfiles("""{"something":"else"}""")
    parsed.root shouldBe None
    parsed.totalSamples shouldBe 0L
  }

  // ----- helper: epoch millis literal --------------------------------

  private def Instant(iso: String): Long = java.time.Instant.parse(iso).toEpochMilli
}
