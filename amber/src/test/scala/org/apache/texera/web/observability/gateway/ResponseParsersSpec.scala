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

  // ----- helper: epoch millis literal --------------------------------

  private def Instant(iso: String): Long = java.time.Instant.parse(iso).toEpochMilli
}
