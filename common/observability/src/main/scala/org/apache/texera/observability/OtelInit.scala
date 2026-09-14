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
import org.apache.texera.common.config.{EnvironmentalVariable, ObservabilityConfig}
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
  * Disabled by default; set OTEL_SDK_DISABLED=false to enable it. Reads its
  * settings from observability.conf (each defaulted, each OTEL_*-overridable),
  * validates the endpoint against an allowlist, builds tracer/log/metric
  * providers, and attaches a Logback appender. Returns None when disabled or
  * misconfigured; never throws.
  */
object OtelInit extends LazyLogging {

  /** Endpoint schemes we accept. OTLP-over-gRPC uses http/https endpoints;
    *  the exporter rejects a `grpc://` scheme outright, so it is not allowed.
    */
  private[observability] val AllowedSchemes: Set[String] = Set("http", "https")

  /** Hosts we accept for the OTLP endpoint by default. */
  private[observability] val DefaultAllowedHosts: Set[String] = Set(
    "localhost",
    "127.0.0.1",
    "[::1]"
  )

  /** Default endpoint. 127.0.0.1 (not "localhost") to force IPv4 so a
    *  natively-run service reaches the collector on dual-stack hosts.
    */
  private val DefaultEndpoint = "http://127.0.0.1:4317"

  /** Metric export interval bounds; out-of-range values fall back to the
    *  default (see clampIntervalMs).
    */
  private[observability] val MinMetricIntervalMs: Long = 1000L
  private[observability] val MaxMetricIntervalMs: Long = 10L * 60L * 1000L
  private[observability] val DefaultMetricIntervalMs: Long = 30L * 1000L

  // Idempotency guard: init() is a no-op after the first call.
  @volatile private var initialized: Option[OpenTelemetry] = None

  /**
    * Initialize the SDK for the given service name. Returns Some on
    * success, None when disabled or misconfigured. When enabled, also
    * attaches a [[TexeraOtelLogAppender]] to the Logback ROOT logger.
    */
  def init(serviceName: String): Option[OpenTelemetry] =
    synchronized {
      if (initialized.isDefined) return initialized

      // Source the OTEL_* settings from observability.conf (HOCON defaults
      // already merged with any env override); fall back to the raw environment
      // for anything else.
      val env = (key: String) =>
        key match {
          case EnvironmentalVariable.ENV_OTEL_SDK_DISABLED => Some(ObservabilityConfig.sdkDisabled)
          case EnvironmentalVariable.ENV_OTEL_EXPORTER_OTLP_ENDPOINT =>
            Some(ObservabilityConfig.endpoint)
          case EnvironmentalVariable.ENV_OTEL_RESOURCE_ATTRIBUTES =>
            Some(ObservabilityConfig.resourceAttributes)
          case EnvironmentalVariable.ENV_TEXERA_OTEL_ALLOWED_HOSTS =>
            Some(ObservabilityConfig.allowedHosts)
          case EnvironmentalVariable.ENV_OTEL_METRIC_EXPORT_INTERVAL =>
            Some(ObservabilityConfig.metricExportIntervalMs)
          case other => Option(System.getenv(other))
        }
      val result = initInternal(
        serviceName = serviceName,
        envProvider = env,
        spanExporterFactory = buildOtlpSpanExporter,
        logExporterFactory = endpoint => Some(buildOtlpLogExporter(endpoint)),
        metricExporterFactory = endpoint => Some(buildOtlpMetricExporter(endpoint)),
        logbackAttacher = LogbackBinder.attach
      )
      // Register globally so OTel-aware code can use GlobalOpenTelemetry
      // without threading the SDK through callsites. set() throws on a
      // second call; wrap defensively.
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
    * Test-only entry point: injects an env-var map and exporters so the
    * SDK makes no network connection. Does not attach the Logback appender.
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

    // Disabled by default (issue #5367): stay inert unless OTEL_SDK_DISABLED is
    // explicitly false. An unreachable endpoint drops records without crashing.
    val disabled = envProvider(EnvironmentalVariable.ENV_OTEL_SDK_DISABLED).getOrElse("true")
    if (!disabled.equalsIgnoreCase("false")) {
      logger.info(
        "OpenTelemetry SDK disabled (OTEL_SDK_DISABLED not false). No telemetry will be emitted."
      )
      return None
    }

    val endpoint =
      envProvider(EnvironmentalVariable.ENV_OTEL_EXPORTER_OTLP_ENDPOINT).getOrElse(DefaultEndpoint)
    val extraAllowed = envProvider(EnvironmentalVariable.ENV_TEXERA_OTEL_ALLOWED_HOSTS)
      .map(_.split(',').iterator.map(_.trim.toLowerCase).filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)
    val allowedHosts = DefaultAllowedHosts ++ extraAllowed

    validateEndpoint(endpoint, allowedHosts) match {
      case Left(reason) =>
        // One WARN; no telemetry is emitted.
        logger.warn(
          s"OpenTelemetry SDK disabled: invalid OTEL_EXPORTER_OTLP_ENDPOINT — $reason. " +
            "Set TEXERA_OTEL_ALLOWED_HOSTS to extend the allowlist."
        )
        return None
      case Right(_) => // ok
    }

    val rawAttrs = envProvider(EnvironmentalVariable.ENV_OTEL_RESOURCE_ATTRIBUTES).getOrElse("")
    val resource = buildResource(serviceName, rawAttrs)

    val spanExporter = spanExporterFactory(endpoint)
    val tracerProvider = SdkTracerProvider
      .builder()
      .setResource(resource)
      .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
      .build()

    val sdkBuilder = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider)

    // Logger provider is optional; the factory returns None in tests.
    val loggerProviderOpt = logExporterFactory(endpoint).map { logExporter =>
      val lp = SdkLoggerProvider
        .builder()
        .setResource(resource)
        .addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter).build())
        .build()
      sdkBuilder.setLoggerProvider(lp)
      lp
    }

    // Meter provider is optional too; interval falls back to the default
    // when out of range.
    val intervalMs =
      clampIntervalMs(envProvider(EnvironmentalVariable.ENV_OTEL_METRIC_EXPORT_INTERVAL))
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

    // One startup span carrying only service.name.
    val span = sdk.getTracer("texera.bootstrap").spanBuilder("service.start").startSpan()
    Try(span.setAttribute("service.name", serviceName))
    span.end()

    // Wire the Logback appender; failure here must not crash the service.
    Try(logbackAttacher(serviceName, sdk)).failed.foreach { t =>
      logger.warn(s"Failed to attach OTel Logback appender (logs not exported): ${t.getMessage}")
    }

    // Flush providers on shutdown. Added after the SDK is fully built.
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
    * Validate the endpoint is parseable and uses an allowlisted scheme
    * and host. Pure function.
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
    * Build a Resource from the service name and OTEL_RESOURCE_ATTRIBUTES.
    * Every parsed attribute is applied so new resource fields need no edit
    * here; the one exception is service.name, which the argument controls
    * and env cannot override.
    */
  private[observability] def buildResource(serviceName: String, rawAttrs: String): Resource = {
    val builder = Attributes.builder()
    builder.put(AttributeKey.stringKey("service.name"), serviceName)

    parseAttrs(rawAttrs).foreach {
      case (key, value) if key != "service.name" =>
        builder.put(AttributeKey.stringKey(key), value)
      case _ => // env cannot override service.name
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
    * Parse and clamp OTEL_METRIC_EXPORT_INTERVAL (ms). Out-of-range or
    * unparseable input falls back to the default with one WARN.
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
  * Isolates the Logback attach step so [[OtelInit]] does not import
  * Logback types directly, keeping SDK init testable with a mock attacher.
  */
private[observability] object LogbackBinder extends LazyLogging {

  /** Attach a [[TexeraOtelLogAppender]] bound to `otel` to the Logback
    *  ROOT logger. Emits one WARN and returns if Logback is not the
    *  active SLF4J binding.
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
