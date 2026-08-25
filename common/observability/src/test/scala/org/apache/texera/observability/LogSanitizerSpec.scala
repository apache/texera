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

package org.apache.texera.observability

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class LogSanitizerSpec extends AnyFlatSpec with Matchers {

  // ----- sanitize: control characters ----------------------------------

  "sanitize" should "strip CR/LF so a user-supplied message cannot forge a new log line" in {
    val crlfPayload = "hello\r\nFAKE LOG LINE\r\nworld"
    LogSanitizer.sanitize(crlfPayload) shouldBe "helloFAKE LOG LINEworld"
  }

  it should "strip other C0 control characters but preserve TAB" in {
    val payload = "before\u0000NUL\u0007BEL\tTAB\u001bafter\u007fdel"
    LogSanitizer.sanitize(payload) shouldBe "beforeNULBEL\tTABafterdel"
  }

  it should "handle empty / null bodies cleanly" in {
    LogSanitizer.sanitize("") shouldBe ""
    LogSanitizer.sanitize(null) shouldBe ""
  }

  // ----- sanitize: secret scrubbing ------------------------------------

  it should "redact Bearer tokens regardless of case" in {
    LogSanitizer.sanitize("Authorization: Bearer abc123XYZ.foo") should include("[REDACTED]")
    LogSanitizer.sanitize("Authorization: Bearer abc123XYZ.foo") should not include "abc123XYZ"
    LogSanitizer.sanitize("auth = bearer eyJhbGci.tok") should include("[REDACTED]")
  }

  it should "redact password=... key/value forms" in {
    val out = LogSanitizer.sanitize("connecting: user=alice password=hunter2 host=db")
    out should include("[REDACTED]")
    out should not include "hunter2"
    // surrounding context preserved
    out should include("user=alice")
    out should include("host=db")
  }

  it should "redact AWS access key IDs" in {
    val out = LogSanitizer.sanitize("found key AKIAIOSFODNN7EXAMPLE in env")
    out should include("[REDACTED]")
    out should not include "AKIAIOSFODNN7EXAMPLE"
  }

  it should "redact AWS secret access keys when explicitly labelled" in {
    val out = LogSanitizer.sanitize(
      "aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY some text"
    )
    out should include("[REDACTED]")
    out should not include "wJalrXUtnFEMI"
  }

  it should "leave already-redacted content alone (idempotent)" in {
    val once = LogSanitizer.sanitize("Authorization: Bearer abc12345.deadbeef")
    val twice = LogSanitizer.sanitize(once)
    twice shouldBe once
  }

  // ----- sanitize: size cap --------------------------------------------

  it should "truncate bodies larger than MaxBodyBytes and append the marker" in {
    val oversize = "a" * (LogSanitizer.MaxBodyBytes * 4) // ~64 KiB
    val out = LogSanitizer.sanitize(oversize)
    out.length shouldBe LogSanitizer.MaxBodyBytes
    out should endWith(LogSanitizer.TruncatedMarker)
  }

  it should "leave bodies at or below the cap unchanged in length" in {
    val rightAtCap = "x" * LogSanitizer.MaxBodyBytes
    LogSanitizer.sanitize(rightAtCap).length shouldBe LogSanitizer.MaxBodyBytes
  }

  // ----- filterMdc -----------------------------------------------------

  "filterMdc" should "drop denied Pekko keys and pass every other key through" in {
    val mdc = Map(
      "trace_id" -> "abc",
      "span_id" -> "def",
      "texera.workflow.id" -> "42",
      "app.new.tag" -> "kept",
      "sourceThread" -> "dispatcher-3",
      "pekkoSource" -> "akka://sys/user/actor"
    ).asJava
    val out = LogSanitizer.filterMdc(mdc)
    out.keySet shouldBe Set("trace_id", "span_id", "texera.workflow.id", "app.new.tag")
  }

  it should "scrub secret-shaped values on keys that pass through" in {
    val mdc = Map("note" -> "password=p4ssw0rd").asJava
    LogSanitizer.filterMdc(mdc)("note") should not include "p4ssw0rd"
  }

  it should "redact the value of a credential-named key even when the value itself is benign" in {
    val mdc = Map(
      "password" -> "hunter2",
      "user.api_key" -> "abcdef",
      "authToken" -> "xyz",
      "trace_id" -> "keep-me"
    ).asJava
    val out = LogSanitizer.filterMdc(mdc)
    out("password") shouldBe "[REDACTED]"
    out("user.api_key") shouldBe "[REDACTED]"
    out("authToken") shouldBe "[REDACTED]"
    out("trace_id") shouldBe "keep-me"
  }

  it should "tolerate null map and null values" in {
    LogSanitizer.filterMdc(null) shouldBe empty

    val javaMap = new java.util.HashMap[String, String]()
    javaMap.put("trace_id", null)
    javaMap.put("texera.user.id", "7")
    val out = LogSanitizer.filterMdc(javaMap)
    out shouldBe Map("texera.user.id" -> "7")
  }
}
