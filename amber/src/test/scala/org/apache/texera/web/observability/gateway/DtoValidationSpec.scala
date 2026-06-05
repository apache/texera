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

class DtoValidationSpec extends AnyFlatSpec with Matchers {

  // ----- TimeWindow ----------------------------------------------------

  "TimeWindow.validate" should "accept a 1-hour window for logs" in {
    val hourMs = 3_600_000L
    TimeWindow.validate(Signal.Logs, 0L, hourMs) shouldBe a[Valid[_]]
  }

  it should "reject a window > 7 days for logs" in {
    val eightDaysMs = 8L * 24L * 3_600_000L
    val result = TimeWindow.validate(Signal.Logs, 0L, eightDaysMs)
    result shouldBe an[Invalid]
  }

  it should "accept a 30-day window for metrics" in {
    val thirtyDaysMs = 30L * 24L * 3_600_000L
    TimeWindow.validate(Signal.Metrics, 0L, thirtyDaysMs) shouldBe a[Valid[_]]
  }

  it should "reject a 25-hour window for traces" in {
    val twentyFiveHrMs = 25L * 3_600_000L
    TimeWindow.validate(Signal.Traces, 0L, twentyFiveHrMs) shouldBe an[Invalid]
  }

  it should "reject zero and negative windows" in {
    TimeWindow.validate(Signal.Logs, 0L, 0L) shouldBe an[Invalid]
    TimeWindow.validate(Signal.Logs, 100L, 50L) shouldBe an[Invalid]
  }

  // ----- PageSize ------------------------------------------------------

  "PageSize.validate" should "clamp into [1, 1000]" in {
    PageSize.validate(1) shouldBe a[Valid[_]]
    PageSize.validate(1000) shouldBe a[Valid[_]]
    PageSize.validate(0) shouldBe an[Invalid]
    PageSize.validate(1001) shouldBe an[Invalid]
    PageSize.validate(-5) shouldBe an[Invalid]
  }

  // ----- FreeText ------------------------------------------------------

  "FreeText.validate" should "accept a normal-length query" in {
    val r = FreeText.validate(Some("hello world"))
    r shouldBe Valid(Some(FreeText("hello world")))
  }

  it should "reject text longer than MaxFreeTextLen" in {
    val tooLong = "x" * (MaxFreeTextLen + 1)
    FreeText.validate(Some(tooLong)) shouldBe an[Invalid]
  }

  it should "strip control characters before reaching a builder" in {
    val r = FreeText.validate(Some("hello\r\nworld"))
    r match {
      case Valid(Some(ft)) => ft.value shouldBe "helloworld"
      case other           => fail(s"unexpected: $other")
    }
  }

  it should "return Valid(None) when control-stripping reduces input to empty" in {
    FreeText.validate(Some("\r\n\t")) match {
      case Valid(None) => succeed
      case other       => fail(s"unexpected: $other")
    }
  }

  it should "treat absent input as Valid(None)" in {
    FreeText.validate(None) shouldBe Valid(None)
  }

  // ----- ValidatedTracesGetRequest ------------------------------------

  "ValidatedTracesGetRequest.validate" should "accept a 32-hex-char id" in {
    val r = ValidatedTracesGetRequest.validate(
      RawTracesGetRequest("0af7651916cd43dd8448eb211c80319c")
    )
    r shouldBe Valid(ValidatedTracesGetRequest("0af7651916cd43dd8448eb211c80319c"))
  }

  it should "reject UPPERCASE hex" in {
    val r = ValidatedTracesGetRequest.validate(
      RawTracesGetRequest("0AF7651916CD43DD8448EB211C80319C")
    )
    r shouldBe an[Invalid]
  }

  it should "reject path-traversal-style trace ids" in {
    val r = ValidatedTracesGetRequest.validate(
      RawTracesGetRequest("../../etc/passwd")
    )
    r shouldBe an[Invalid]
  }

  it should "reject wrong-length ids" in {
    ValidatedTracesGetRequest.validate(RawTracesGetRequest("0af7")) shouldBe an[Invalid]
    ValidatedTracesGetRequest.validate(
      RawTracesGetRequest("0af7651916cd43dd8448eb211c80319c0")
    ) shouldBe an[Invalid]
  }

  // ----- LogLevel parsing --------------------------------------------

  "LogLevel.parse" should "be case-insensitive and reject unknowns" in {
    LogLevel.parse("info") shouldBe Some(LogLevel.INFO)
    LogLevel.parse("ERROR") shouldBe Some(LogLevel.ERROR)
    LogLevel.parse("WaRn") shouldBe Some(LogLevel.WARN)
    LogLevel.parse("DELETE") shouldBe None
    LogLevel.parse(null) shouldBe None
  }

  // ----- NamedMetric parsing ------------------------------------------

  "NamedMetric.parse" should "accept only the allowlist of names" in {
    NamedMetric.parse("runsPerDay") shouldBe Some(NamedMetric.RunsPerDay)
    NamedMetric.parse("failureRate") shouldBe Some(NamedMetric.FailureRate)
    // Case-sensitive on purpose — UI passes the canonical form.
    NamedMetric.parse("runsperday") shouldBe None
    NamedMetric.parse("evilQuery") shouldBe None
  }
}
