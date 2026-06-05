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

import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.{GlobalOpenTelemetry, OpenTelemetry}
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.logs.SdkLoggerProvider
import io.opentelemetry.sdk.logs.`export`.{BatchLogRecordProcessor, LogRecordExporter}
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.`export`.{MetricExporter, PeriodicMetricReader}
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.{BatchSpanProcessor, SpanExporter}

import java.net.URI
import java.time.Duration
import scala.util.{Failure, Success, Try}

/**
  * Bootstraps the OpenTelemetry SDK for a Texera service.
  *
  * Design notes:
  *  - Default-disabled. Sets up nothing unless `OTEL_SDK_DISABLED=false`.
  *  - We deliberately do not use the autoconfigure SPI: the security model
  *    requires endpoint + resource-attribute filtering to happen BEFORE
  *    any exporter is constructed. Autoconfigure would parse env vars
  *    behind our back.
  *  - Validation is a single pure function so it can be unit-tested
  *    without spinning the SDK.
  *  - On any validation failure we log one WARN and return None. We
  *    do NOT throw — observability is opt-in plumbing; misconfiguration
  *    must never crash the service.
  *  - This is the only place in Texera that reads `OTEL_*` environment
  *    variables. Other modules consume the returned `OpenTelemetry`
  *    instance directly.
  */
object OtelInit extends LazyLogging {

  /** Resource attribute keys we accept from OTEL_RESOURCE_ATTRIBUTES.
    *  Resource attrs ride on every record this JVM emits (logs,
    *  metrics, traces) — so for a per-CU JVM (ComputingUnitMaster /
    *  ComputingUnitWorker) setting `texera.computing_unit.id=N` at
    *  boot is enough to tag every record without per-request MDC
    *  plumbing. Workflow/execution ids vary per task and still need
    *  MDC at the message boundary, but exposing them in the allowlist
    *  lets test harnesses + future per-task code populate them via
    *  the same mechanism.
    */
  private[observability] val AllowedResourceKeys: Set[String] = Set(
    "service.name",
    "service.version",
    "deployment.environment",
    "texera.computing_unit.id",
    "texera.workflow.id",
    "texera.execution.id"
  )

  /** Endpoint schemes we accept. */
  private[observability] val AllowedSchemes: Set[String] = Set("http", "https", "grpc")

  /** Hosts we accept for the OTLP endpoint by default. */
  private[observability] val DefaultAllowedHosts: Set[String] = Set(
    "localhost",
    "127.0.0.1",
    "::1",
    "[::1]"
  )

  /** Default endpoint when SDK is enabled but no endpoint set explicitly.
    *  Uses 127.0.0.1 (not "localhost") so a natively-run service reaches the
    *  IPv4-only collector port published by docker-compose — on dual-stack
    *  hosts "localhost" resolves to ::1 first and the OTLP export silently
    *  fails. Inside docker the endpoint is overridden to otel-collector:4317.
    */
  private val DefaultEndpoint = "http://127.0.0.1:4317"

  /** Metric export interval bounds. Values outside this range get
    *  clamped to the default with a one-shot WARN. The lower bound
    *  prevents an attacker tipping the exporter into busy-loop mode;
    *  the upper bound keeps metrics useful for human operators.
    */
  private[observability] val MinMetricIntervalMs: Long = 1000L
  private[observability] val MaxMetricIntervalMs: Long = 10L * 60L * 1000L
  private[observability] val DefaultMetricIntervalMs: Long = 60L * 1000L

  // Idempotency guard. The SDK installs global handlers and a shutdown
  // hook; calling init() repeatedly must be a no-op after the first call.
  @volatile private var initialized: Option[OpenTelemetry] = None

  /**
    * Initialize the SDK for the given service name.
    * Returns Some(sdk) on success, None on disabled / invalid config.
    *
    * Side effect when enabled: attaches a [[TexeraOtelLogAppender]] to
    * the Logback ROOT logger so application logs are mirrored to the
    * OTel collector, with the security guards in [[LogSanitizer]]
    * applied to every record.
    */
  def init(serviceName: String): Option[OpenTelemetry] =
    synchronized {
      if (initialized.isDefined) return initialized

      val env = (key: String) => Option(System.getenv(key))
      val result = initInternal(
        serviceName = serviceName,
        envProvider = env,
        spanExporterFactory = buildOtlpSpanExporter,
        logExporterFactory = endpoint => Some(buildOtlpLogExporter(endpoint)),
        metricExporterFactory = endpoint => Some(buildOtlpMetricExporter(endpoint)),
        logbackAttacher = LogbackBinder.attach
      )
      // Register globally so [[TexeraTracer]] and any other OTel-aware
      // code can call ``GlobalOpenTelemetry.getTracer(...)`` without
      // threading the SDK through every callsite. set() throws on a
      // second call within the same JVM — our outer ``initialized``
      // guard makes that unreachable, but wrap defensively. The test
      // path deliberately skips this so multiple isolated SDKs can be
      // built within one JVM.
      result.foreach { sdk =>
        Try(GlobalOpenTelemetry.set(sdk)).failed.foreach { t =>
          logger.warn(
            s"GlobalOpenTelemetry already set; using the existing instance: ${t.getMessage}"
          )
        }
      }
      result
    }

  /**
    * Test-only entry point. Allows the test to inject an env-var map
    * and a span exporter so the SDK does not attempt a real network
    * connection. The Logback appender is NOT attached in tests —
    * appender tests construct it directly with an in-memory log
    * exporter.
    */
  private[observability] def initForTest(
      serviceName: String,
      envOverride: Map[String, String],
      exporter: SpanExporter,
      metricExporter: Option[MetricExporter] = None
  ): Option[OpenTelemetry] =
    synchronized {
      initInternal(
        serviceName = serviceName,
        envProvider = envOverride.get,
        spanExporterFactory = _ => exporter,
        logExporterFactory = _ => None,
        metricExporterFactory = _ => metricExporter,
        logbackAttacher = (_, _) => () // no-op in tests
      )
    }

  /** Test-only: forget any previously-installed SDK. Does not unregister
    * shutdown hooks (the previous SDK is closed instead).
    */
  private[observability] def resetForTest(): Unit =
    synchronized {
      initialized.foreach {
        case sdk: OpenTelemetrySdk =>
          Try(sdk.getSdkTracerProvider.close())
          Try(sdk.getSdkLoggerProvider.close())
          Try(sdk.getSdkMeterProvider.close())
        case _ => ()
      }
      initialized = None
    }

  private def initInternal(
      serviceName: String,
      envProvider: String => Option[String],
      spanExporterFactory: String => SpanExporter,
      logExporterFactory: String => Option[LogRecordExporter],
      metricExporterFactory: String => Option[MetricExporter],
      logbackAttacher: (String, OpenTelemetry) => Unit
  ): Option[OpenTelemetry] = {
    if (initialized.isDefined) return initialized

    // Default to ENABLED so an `sbt run` of any Texera service emits
    // telemetry without per-JVM env-var configuration. Operators who
    // need to silence telemetry (CI, embedded-tests, security-locked
    // deployments) set OTEL_SDK_DISABLED=true explicitly. If the
    // configured endpoint isn't reachable, the OTel SDK's
    // BatchProcessor logs a single error and drops records — it
    // does NOT crash the host service, so a missing collector at
    // dev time is a quiet no-op rather than a startup failure.
    val disabled = envProvider("OTEL_SDK_DISABLED").getOrElse("false")
    if (disabled.equalsIgnoreCase("true")) {
      logger.info(
        "OpenTelemetry SDK disabled (OTEL_SDK_DISABLED=true). No telemetry will be emitted."
      )
      return None
    }

    val endpoint = envProvider("OTEL_EXPORTER_OTLP_ENDPOINT").getOrElse(DefaultEndpoint)
    val extraAllowed = envProvider("TEXERA_OTEL_ALLOWED_HOSTS")
      .map(_.split(',').iterator.map(_.trim.toLowerCase).filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)
    val allowedHosts = DefaultAllowedHosts ++ extraAllowed

    validateEndpoint(endpoint, allowedHosts) match {
      case Left(reason) =>
        // One WARN, no further detail (endpoint is not echoed beyond what
        // the operator already knows). No spans will be emitted.
        logger.warn(
          s"OpenTelemetry SDK disabled: invalid OTEL_EXPORTER_OTLP_ENDPOINT — $reason. " +
            "Set TEXERA_OTEL_ALLOWED_HOSTS to extend the allowlist."
        )
        return None
      case Right(_) => // ok
    }

    val rawAttrs = envProvider("OTEL_RESOURCE_ATTRIBUTES").getOrElse("")
    val resource = buildResource(serviceName, rawAttrs)

    val spanExporter = spanExporterFactory(endpoint)
    val tracerProvider = SdkTracerProvider
      .builder()
      .setResource(resource)
      .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
      .build()

    val sdkBuilder = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider)

    // Logger provider is optional — controlled by the factory. Skipped
    // in tests so the appender path can be exercised independently.
    val loggerProviderOpt = logExporterFactory(endpoint).map { logExporter =>
      val lp = SdkLoggerProvider
        .builder()
        .setResource(resource)
        .addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter).build())
        .build()
      sdkBuilder.setLoggerProvider(lp)
      lp
    }

    // Meter provider is optional too. Export interval is clamped to
    // [MinMetricIntervalMs, MaxMetricIntervalMs]; an out-of-range
    // value gets reset to the default with one WARN — keeps an
    // attacker from coaxing the reader into busy-loop mode by
    // setting OTEL_METRIC_EXPORT_INTERVAL to a tiny value.
    val intervalMs = clampIntervalMs(envProvider("OTEL_METRIC_EXPORT_INTERVAL"))
    val meterProviderOpt = metricExporterFactory(endpoint).map { metricExporter =>
      val reader = PeriodicMetricReader
        .builder(metricExporter)
        .setInterval(Duration.ofMillis(intervalMs))
        .build()
      val mp = SdkMeterProvider
        .builder()
        .setResource(resource)
        .registerMetricReader(reader)
        .build()
      sdkBuilder.setMeterProvider(mp)
      mp
    }

    val sdk = sdkBuilder.build()

    // One startup span. Carries only service.name (no env, host, or
    // version data beyond the allowlisted resource attrs).
    val span = sdk.getTracer("texera.bootstrap").spanBuilder("service.start").startSpan()
    Try(span.setAttribute("service.name", serviceName))
    span.end()

    // Wire the Logback appender so subsequent application logs flow to
    // the collector with sanitisation applied. Failure here must never
    // crash the service — observability is opt-in.
    Try(logbackAttacher(serviceName, sdk)).failed.foreach { t =>
      logger.warn(s"Failed to attach OTel Logback appender (logs not exported): ${t.getMessage}")
    }

    // Make sure providers flush on shutdown. We add the hook only after
    // the SDK has been fully built so a panic during init doesn't leave
    // a dangling hook pointing at a half-constructed provider.
    Runtime.getRuntime.addShutdownHook(
      new Thread(
        () => {
          Try(tracerProvider.close())
          loggerProviderOpt.foreach(lp => Try(lp.close()))
          meterProviderOpt.foreach(mp => Try(mp.close()))
          ()
        },
        "otel-shutdown"
      )
    )

    initialized = Some(sdk)
    logger.info(s"OpenTelemetry SDK initialized for service '$serviceName' (endpoint=$endpoint).")
    initialized
  }

  /**
    * Validate that the endpoint is parseable, uses an allowlisted scheme,
    * and resolves to an allowlisted host. Pure function — safe to test
    * without standing up the SDK.
    */
  private[observability] def validateEndpoint(
      endpoint: String,
      allowedHosts: Set[String]
  ): Either[String, Unit] = {
    Try(URI.create(endpoint)) match {
      case Failure(e) =>
        Left(s"unparseable URI (${e.getClass.getSimpleName})")
      case Success(uri) =>
        val scheme = Option(uri.getScheme).map(_.toLowerCase).getOrElse("")
        if (scheme.isEmpty) {
          Left("missing scheme")
        } else if (!AllowedSchemes.contains(scheme)) {
          Left(
            s"scheme '$scheme' not in allowlist ${AllowedSchemes.toSeq.sorted.mkString("{", ",", "}")}"
          )
        } else {
          val host = Option(uri.getHost).map(_.toLowerCase).getOrElse("")
          if (host.isEmpty) {
            Left("missing host")
          } else if (!allowedHosts.contains(host)) {
            Left(s"host '$host' not in allowlist")
          } else {
            Right(())
          }
        }
    }
  }

  /**
    * Build a Resource from the service name plus the allowlisted subset
    * of OTEL_RESOURCE_ATTRIBUTES. Unknown keys are dropped silently;
    * service.name from env is ignored in favour of the argument.
    */
  private[observability] def buildResource(serviceName: String, rawAttrs: String): Resource = {
    val builder = Attributes.builder()
    builder.put(AttributeKey.stringKey("service.name"), serviceName)

    parseAttrs(rawAttrs).foreach {
      case (key, value) if AllowedResourceKeys.contains(key) && key != "service.name" =>
        builder.put(AttributeKey.stringKey(key), value)
      case _ => // dropped — not in allowlist or overrides service.name
    }

    Resource.create(builder.build())
  }

  /** Parse a `k1=v1,k2=v2` string. Malformed entries are skipped. */
  private[observability] def parseAttrs(raw: String): Seq[(String, String)] = {
    if (raw == null || raw.isEmpty) return Seq.empty
    raw
      .split(',')
      .iterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap { entry =>
        val idx = entry.indexOf('=')
        if (idx <= 0 || idx == entry.length - 1) None
        else Some(entry.substring(0, idx).trim -> entry.substring(idx + 1).trim)
      }
      .toSeq
  }

  private def buildOtlpSpanExporter(endpoint: String): SpanExporter =
    OtlpGrpcSpanExporter.builder().setEndpoint(endpoint).build()

  private def buildOtlpLogExporter(endpoint: String): LogRecordExporter =
    OtlpGrpcLogRecordExporter.builder().setEndpoint(endpoint).build()

  private def buildOtlpMetricExporter(endpoint: String): MetricExporter =
    OtlpGrpcMetricExporter.builder().setEndpoint(endpoint).build()

  /**
    * Parse and clamp OTEL_METRIC_EXPORT_INTERVAL (milliseconds).
    * Out-of-range or unparseable input falls back to the default and
    * emits a single WARN. Pure-ish — easy to test without standing up
    * the meter SDK.
    */
  private[observability] def clampIntervalMs(raw: Option[String]): Long = {
    raw match {
      case None => DefaultMetricIntervalMs
      case Some(value) =>
        Try(value.trim.toLong) match {
          case Failure(_) =>
            logger.warn(
              s"OTEL_METRIC_EXPORT_INTERVAL '$value' is not a number; " +
                s"using default ${DefaultMetricIntervalMs}ms."
            )
            DefaultMetricIntervalMs
          case Success(ms) if ms < MinMetricIntervalMs || ms > MaxMetricIntervalMs =>
            logger.warn(
              s"OTEL_METRIC_EXPORT_INTERVAL=${ms}ms out of range " +
                s"[${MinMetricIntervalMs}, ${MaxMetricIntervalMs}]; " +
                s"using default ${DefaultMetricIntervalMs}ms."
            )
            DefaultMetricIntervalMs
          case Success(ms) => ms
        }
    }
  }
}

/**
  * Hides the Logback attach step behind a small object so [[OtelInit]]
  * doesn't import Logback types directly (keeps the SDK init testable
  * without a Logback dependency on the classpath in test runs that
  * inject a mock attacher).
  */
private[observability] object LogbackBinder extends LazyLogging {

  /** Attempts to find the Logback ROOT logger, attach a fresh
    *  [[TexeraOtelLogAppender]] bound to `otel`, and start it. If
    *  Logback is not the active SLF4J binding (or for any other
    *  classpath issue), emits one WARN and returns — never throws.
    */
  def attach(serviceName: String, otel: OpenTelemetry): Unit = {
    val factory = org.slf4j.LoggerFactory.getILoggerFactory
    factory match {
      case ctx: ch.qos.logback.classic.LoggerContext =>
        val root = ctx.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
        val appender = new TexeraOtelLogAppender()
        appender.setContext(ctx)
        appender.setName(s"texera-otel-$serviceName")
        appender.bind(otel)
        appender.start()
        root.addAppender(appender)
      case other =>
        logger.warn(
          s"SLF4J binding is not Logback (${other.getClass.getName}); " +
            "OTel log export is not wired. Application logs to stdout/file are unaffected."
        )
    }
  }
}
