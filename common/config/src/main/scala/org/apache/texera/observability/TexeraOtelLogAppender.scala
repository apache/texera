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
  * Logback appender that sanitizes each event via [[LogSanitizer]] and
  * emits it as an OTel LogRecord. [[append]] is a no-op until [[bind]]
  * is called and after [[stop]].
  */
class TexeraOtelLogAppender extends UnsynchronizedAppenderBase[ILoggingEvent] {

  // @volatile so a late bind() is visible to appender threads.
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
      case None => () // not bound
      case Some(logger) =>
        try {
          emit(logger, event)
        } catch {
          // An appender must not throw into the calling thread.
          case t: Throwable =>
            addError("OTel log emission failed", t)
        }
    }
  }

  private def emit(logger: Logger, event: ILoggingEvent): Unit = {
    // Append the stack trace to the body when a throwable is attached.
    val baseBody = LogSanitizer.sanitize(event.getFormattedMessage)
    val body = Option(event.getThrowableProxy) match {
      case Some(proxy) =>
        // Skip the C0 strip so trace newlines survive, but still cap.
        LogSanitizer.truncate(baseBody + "\n" + formatThrowable(proxy))
      case None => baseBody
    }
    val builder = logger
      .logRecordBuilder()
      .setBody(body)
      .setSeverity(severityFromLevel(event.getLevel))
      .setSeverityText(event.getLevel.toString)
      .setTimestamp(event.getTimeStamp, TimeUnit.MILLISECONDS)

    // Allowlisted MDC keys as typed attributes.
    LogSanitizer.filterMdc(event.getMDCPropertyMap).foreach {
      case (k, v) => builder.setAttribute(AttributeKey.stringKey(k), v)
    }

    builder.setAttribute(AttributeKey.stringKey("logger.name"), event.getLoggerName)
    builder.setAttribute(AttributeKey.stringKey("thread.name"), event.getThreadName)

    // Attach trace context so the SDK sets trace_id / span_id.
    val span = Span.current()
    if (span.getSpanContext.isValid) {
      builder.setContext(Context.current())
    }

    builder.emit()
  }

  /** Format a throwable proxy as a Logback-style stack trace. */
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
