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

import ch.qos.logback.classic.{Level, Logger, LoggerContext}
import ch.qos.logback.classic.spi.LoggingEvent
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.logs.Severity
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.logs.SdkLoggerProvider
import io.opentelemetry.sdk.logs.`export`.SimpleLogRecordProcessor
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

class TexeraOtelLogAppenderSpec extends AnyFlatSpec with Matchers {

  /** Build an OpenTelemetry SDK whose LoggerProvider drains to the
   *  given in-memory exporter via the synchronous SimpleLogRecordProcessor,
   *  so tests don't depend on batch timing. */
  private def newFixture(): (OpenTelemetry, InMemoryLogRecordExporter, TexeraOtelLogAppender) = {
    val exporter = InMemoryLogRecordExporter.create()
    val lp = SdkLoggerProvider
      .builder()
      .addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter))
      .build()
    val sdk = OpenTelemetrySdk.builder().setLoggerProvider(lp).build()
    val appender = new TexeraOtelLogAppender()
    appender.setContext(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext])
    appender.bind(sdk)
    appender.start()
    (sdk, exporter, appender)
  }

  private def makeEvent(
      message: String,
      level: Level = Level.INFO,
      mdc: Map[String, String] = Map.empty
  ): LoggingEvent = {
    val ctx = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val logger = ctx.getLogger("test.logger").asInstanceOf[Logger]
    val ev = new LoggingEvent("fqcn", logger, level, message, null, null)
    if (mdc.nonEmpty) ev.setMDCPropertyMap(mdc.asJava)
    ev
  }

  // ----- positive paths -------------------------------------------------

  "TexeraOtelLogAppender" should "emit an INFO record with body + severity" in {
    val (_, exporter, appender) = newFixture()
    appender.doAppend(makeEvent("hello world"))

    val records = exporter.getFinishedLogRecordItems.asScala
    records should have size 1
    records.head.getBodyValue.asString shouldBe "hello world"
    records.head.getSeverity shouldBe Severity.INFO
    records.head.getSeverityText shouldBe "INFO"
  }

  it should "map every log level to a distinct OTel severity" in {
    val (_, exporter, appender) = newFixture()
    Seq(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR).foreach { lvl =>
      appender.doAppend(makeEvent(s"msg-$lvl", lvl))
    }
    val severities = exporter.getFinishedLogRecordItems.asScala.map(_.getSeverity).toSet
    severities shouldBe Set(Severity.TRACE, Severity.DEBUG, Severity.INFO, Severity.WARN, Severity.ERROR)
  }

  // ----- security: sanitisation happens at the boundary -----------------

  it should "strip CRLF from a forged log-injection payload before emission" in {
    val (_, exporter, appender) = newFixture()
    appender.doAppend(makeEvent("hello\r\nFAKE LOG LINE\r\nworld"))

    val body = exporter.getFinishedLogRecordItems.asScala.head.getBodyValue.asString
    body shouldBe "helloFAKE LOG LINEworld"
    body should not include "\n"
    body should not include "\r"
  }

  it should "redact Bearer tokens at emission time" in {
    val (_, exporter, appender) = newFixture()
    appender.doAppend(makeEvent("Authorization: Bearer abc123XYZ.foo"))

    val body = exporter.getFinishedLogRecordItems.asScala.head.getBodyValue.asString
    body should include("[REDACTED]")
    body should not include "abc123XYZ"
  }

  it should "truncate a 1 MiB body to MaxBodyBytes with the marker" in {
    val (_, exporter, appender) = newFixture()
    val oversize = "x" * (1024 * 1024)
    appender.doAppend(makeEvent(oversize))

    val body = exporter.getFinishedLogRecordItems.asScala.head.getBodyValue.asString
    body.length shouldBe LogSanitizer.MaxBodyBytes
    body should endWith(LogSanitizer.TruncatedMarker)
  }

  // ----- security: MDC allowlist ----------------------------------------

  it should "forward only allowlisted MDC keys as log attributes" in {
    val (_, exporter, appender) = newFixture()
    appender.doAppend(
      makeEvent(
        "msg",
        mdc = Map(
          "trace_id" -> "abc",
          "texera.workflow.id" -> "42",
          "secret" -> "should-not-leak",
          "password" -> "p4ssw0rd"
        )
      )
    )

    val record = exporter.getFinishedLogRecordItems.asScala.head
    val attrs = record.getAttributes.asMap.asScala.iterator.map {
      case (k, v) => k.getKey -> v.toString
    }.toMap

    attrs.keySet should contain allOf ("trace_id", "texera.workflow.id")
    attrs.keySet should not contain ("secret")
    attrs.keySet should not contain ("password")
    attrs.values should contain noElementsOf Seq("should-not-leak", "p4ssw0rd")
  }

  // ----- lifecycle ------------------------------------------------------

  it should "be a silent no-op when not yet bound to an OpenTelemetry instance" in {
    val unbound = new TexeraOtelLogAppender()
    unbound.setContext(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext])
    unbound.start()
    // Should not throw, even though no SDK is wired.
    noException should be thrownBy unbound.doAppend(makeEvent("hello"))
  }

  it should "stop emitting after stop() is called" in {
    val (_, exporter, appender) = newFixture()
    appender.doAppend(makeEvent("first"))
    appender.stop()
    appender.doAppend(makeEvent("second"))

    val bodies = exporter.getFinishedLogRecordItems.asScala.map(_.getBodyValue.asString)
    bodies should contain only "first"
  }
}
