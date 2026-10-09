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

import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class SpanAttrsSpec extends AnyFlatSpec with Matchers {

  // ----- sanitizeFreeText: pure ----------------------------------------

  "sanitizeFreeText" should "return null for null / empty input" in {
    SpanAttrs.sanitizeFreeText(null) shouldBe null
    SpanAttrs.sanitizeFreeText("") shouldBe null
  }

  it should "strip CR/LF and other control characters" in {
    SpanAttrs.sanitizeFreeText("hello\r\nworld") shouldBe "helloworld"
    SpanAttrs.sanitizeFreeText("a\u0000b\u0001c\u001fd\u007fe") shouldBe "abcde"
  }

  it should "preserve printable text including spaces" in {
    SpanAttrs.sanitizeFreeText("my-op v1 (alpha)") shouldBe "my-op v1 (alpha)"
  }

  it should "cap at FreeTextMaxLen" in {
    val long = "x" * (SpanAttrs.FreeTextMaxLen * 4)
    val out = SpanAttrs.sanitizeFreeText(long)
    out.length shouldBe SpanAttrs.FreeTextMaxLen
  }

  it should "return null when stripping reduces the input to empty" in {
    SpanAttrs.sanitizeFreeText("\r\n\u0000\u0001") shouldBe null
  }

  // ----- standard label keys land via the plain OTel API ---------------

  "the standard label keys" should "set correctly through the OTel builder API" in {
    val (exporter, tracer) = newTracer()
    val sb = tracer.spanBuilder("workflow.execute")
    sb.setAttribute(SpanAttrs.WorkflowId, java.lang.Long.valueOf(42L))
    sb.setAttribute(SpanAttrs.ExecutionId, java.lang.Long.valueOf(7L))
    sb.setAttribute(SpanAttrs.OperatorName, SpanAttrs.sanitizeFreeText("good\r\nFAKE LINE"))
    sb.startSpan().end()

    val attrs = exporter.getFinishedSpanItems.asScala.head.getAttributes
    attrs.get(SpanAttrs.WorkflowId) shouldBe 42L
    attrs.get(SpanAttrs.ExecutionId) shouldBe 7L
    attrs.get(SpanAttrs.OperatorName) shouldBe "goodFAKE LINE"
  }

  it should "expose stable key names so callsites share one label vocabulary" in {
    SpanAttrs.WorkflowId.getKey shouldBe "texera.workflow.id"
    SpanAttrs.ExecutionId.getKey shouldBe "texera.execution.id"
  }

  // ----- attribute-count cap is the OTel SDK default (128) -------------

  it should "respect the SDK's per-span attribute cap of 128" in {
    val (exporter, tracer) = newTracer()
    val sb = tracer.spanBuilder("many")
    // Set 200 distinct keys; SDK should drop everything past the
    // default cap of 128 without crashing the test.
    (0 until 200).foreach { i =>
      sb.setAttribute(s"k$i", s"v$i")
    }
    sb.startSpan().end()

    val attrs = exporter.getFinishedSpanItems.asScala.head.getAttributes
    attrs.size should be <= 128
  }

  private def newTracer(): (InMemorySpanExporter, io.opentelemetry.api.trace.Tracer) = {
    val exporter = InMemorySpanExporter.create()
    val tp = SdkTracerProvider
      .builder()
      .addSpanProcessor(SimpleSpanProcessor.create(exporter))
      .build()
    val sdk = OpenTelemetrySdk.builder().setTracerProvider(tp).build()
    (exporter, sdk.getTracer("test"))
  }
}
