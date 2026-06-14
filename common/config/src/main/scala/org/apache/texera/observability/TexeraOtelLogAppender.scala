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

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.{ILoggingEvent, IThrowableProxy, ThrowableProxyUtil}
import ch.qos.logback.core.UnsynchronizedAppenderBase
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.logs.{Logger, Severity}
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context

import java.util.concurrent.TimeUnit

/**
  * Logback appender that forwards every event through [[LogSanitizer]]
  * before emitting it as an OTel LogRecord.
  *
  * Lifecycle:
  *  - Construct with no args (Logback / programmatic instantiation).
  *  - Call [[bind]] once with the active [[OpenTelemetry]] instance
  *    (done by [[OtelInit]] after the SDK is built). Until then,
  *    [[append]] is a silent no-op — log events keep flowing to
  *    stdout/file unimpeded.
  *  - Stopping the appender unbinds; subsequent events drop.
  *
  * This is intentionally a thin shim. All security-critical logic
  * lives in [[LogSanitizer]] so it can be tested without a Logback
  * fixture.
  */
class TexeraOtelLogAppender extends UnsynchronizedAppenderBase[ILoggingEvent] {

  // @volatile so a late [[bind]] is visible to appender threads
  // without taking a lock on the hot path.
  @volatile private var otelLogger: Option[Logger] = None

  def bind(otel: OpenTelemetry): Unit = {
    otelLogger = Some(otel.getLogsBridge.get("texera.logback"))
  }

  override def stop(): Unit = {
    otelLogger = None
    super.stop()
  }

  override def append(event: ILoggingEvent): Unit = {
    otelLogger match {
      case None => () // disabled or not yet wired
      case Some(logger) =>
        try {
          emit(logger, event)
        } catch {
          // Logback's addStatus contract: errors from inside an
          // appender must not throw out into the calling thread.
          case t: Throwable =>
            addError("OTel log emission failed", t)
        }
    }
  }

  private def emit(logger: Logger, event: ILoggingEvent): Unit = {
    // Append the throwable's full stack trace to the body when one is
    // attached. Without this, Dropwizard's LoggingExceptionMapper logs
    // "Error handling a request: <id>" and the exception itself never
    // reaches the observability backend — making 500s impossible to
    // diagnose from the dashboard. ThrowableProxyUtil emits a Logback-
    // formatted trace that fits inside a single log record.
    val baseBody = LogSanitizer.sanitize(event.getFormattedMessage)
    val body = Option(event.getThrowableProxy) match {
      case Some(proxy) =>
        // Trusted JVM frames: skip the C0 strip so newlines survive,
        // but still cap length (the OTel SDK does not bound the body).
        LogSanitizer.truncate(baseBody + "\n" + formatThrowable(proxy))
      case None => baseBody
    }
    val builder = logger
      .logRecordBuilder()
      .setBody(body)
      .setSeverity(severityFromLevel(event.getLevel))
      .setSeverityText(event.getLevel.toString)
      .setTimestamp(event.getTimeStamp, TimeUnit.MILLISECONDS)

    // MDC subset: typed AttributeKeys only, so no string injection
    // path exists for downstream consumers.
    LogSanitizer.filterMdc(event.getMDCPropertyMap).foreach {
      case (k, v) => builder.setAttribute(AttributeKey.stringKey(k), v)
    }

    builder.setAttribute(AttributeKey.stringKey("logger.name"), event.getLoggerName)
    builder.setAttribute(AttributeKey.stringKey("thread.name"), event.getThreadName)

    // Attach the current trace context so the SDK populates trace_id /
    // span_id on the LogRecord automatically when a span is active.
    val span = Span.current()
    if (span.getSpanContext.isValid) {
      builder.setContext(Context.current())
    }

    builder.emit()
  }

  /** Pretty-print a Logback throwable proxy. Matches what Logback's
    *  default pattern layout would produce for `%ex` — class name,
    *  message, full stack frames, then walks the cause chain.
    */
  private def formatThrowable(proxy: IThrowableProxy): String =
    ThrowableProxyUtil.asString(proxy)

  private def severityFromLevel(level: Level): Severity = {
    if (level == null) return Severity.UNDEFINED_SEVERITY_NUMBER
    level.toInt match {
      case Level.TRACE_INT => Severity.TRACE
      case Level.DEBUG_INT => Severity.DEBUG
      case Level.INFO_INT  => Severity.INFO
      case Level.WARN_INT  => Severity.WARN
      case Level.ERROR_INT => Severity.ERROR
      case _               => Severity.UNDEFINED_SEVERITY_NUMBER
    }
  }
}
