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

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class OtelInitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    OtelInit.resetForTest()
  }

  override def afterEach(): Unit = {
    OtelInit.resetForTest()
  }

  // ----- validateEndpoint: pure function, exhaustive cases -------------

  "validateEndpoint" should "accept loopback OTLP http(s) URLs" in {
    OtelInit.validateEndpoint("http://localhost:4317", OtelInit.DefaultAllowedHosts) shouldBe Right(
      ()
    )
    OtelInit.validateEndpoint("http://127.0.0.1:4317", OtelInit.DefaultAllowedHosts) shouldBe Right(
      ()
    )
    OtelInit.validateEndpoint(
      "https://localhost:4318",
      OtelInit.DefaultAllowedHosts
    ) shouldBe Right(())
  }

  it should "reject a grpc:// endpoint (the OTLP exporter accepts only http/https)" in {
    val result = OtelInit.validateEndpoint("grpc://127.0.0.1:4317", OtelInit.DefaultAllowedHosts)
    result.isLeft shouldBe true
    result.left.toOption.get should include("scheme")
  }

  it should "reject file:// schemes (path traversal style attack)" in {
    val result = OtelInit.validateEndpoint("file:///etc/passwd", OtelInit.DefaultAllowedHosts)
    result.isLeft shouldBe true
    result.left.toOption.get should include("scheme")
  }

  it should "reject arbitrary remote hosts not in the allowlist" in {
    val result = OtelInit.validateEndpoint(
      "http://attacker.example.com:4317",
      OtelInit.DefaultAllowedHosts
    )
    result.isLeft shouldBe true
    result.left.toOption.get should include("host")
  }

  it should "accept hosts added to the allowlist" in {
    val widened = OtelInit.DefaultAllowedHosts + "collector.internal"
    OtelInit.validateEndpoint("http://collector.internal:4317", widened) shouldBe Right(())
  }

  it should "reject endpoints with no scheme" in {
    val result = OtelInit.validateEndpoint("localhost:4317", OtelInit.DefaultAllowedHosts)
    result.isLeft shouldBe true
  }

  it should "reject endpoints with no host" in {
    val result = OtelInit.validateEndpoint("http:///path", OtelInit.DefaultAllowedHosts)
    result.isLeft shouldBe true
  }

  it should "reject completely malformed input" in {
    val result = OtelInit.validateEndpoint("not a uri at all :: bad", OtelInit.DefaultAllowedHosts)
    result.isLeft shouldBe true
  }

  // ----- buildResource: passthrough with service.name protected ---------

  "buildResource" should "always include the service.name from the argument" in {
    val r = OtelInit.buildResource("my-service", "")
    Option(
      r.getAttribute(io.opentelemetry.api.common.AttributeKey.stringKey("service.name"))
    ) shouldBe Some(
      "my-service"
    )
  }

  it should "honor keys from OTEL_RESOURCE_ATTRIBUTES" in {
    val r = OtelInit.buildResource("svc", "service.version=1.2.3,deployment.environment=staging")
    Option(
      r.getAttribute(io.opentelemetry.api.common.AttributeKey.stringKey("service.version"))
    ) shouldBe Some(
      "1.2.3"
    )
    Option(
      r.getAttribute(io.opentelemetry.api.common.AttributeKey.stringKey("deployment.environment"))
    ) shouldBe Some("staging")
  }

  it should "pass custom keys through so new resource fields need no code edit" in {
    val r = OtelInit.buildResource(
      "svc",
      "service.version=1.0,custom.tag=team-a,texera.region.id=us-west"
    )
    val attrs: Map[String, String] = r.getAttributes.asMap.asScala.iterator.map {
      case (k, v) => k.getKey -> v.toString
    }.toMap

    attrs("service.version") shouldBe "1.0"
    attrs("custom.tag") shouldBe "team-a"
    attrs("texera.region.id") shouldBe "us-west"
  }

  it should "refuse to let OTEL_RESOURCE_ATTRIBUTES override service.name" in {
    val r = OtelInit.buildResource("real-svc", "service.name=spoofed")
    Option(
      r.getAttribute(io.opentelemetry.api.common.AttributeKey.stringKey("service.name"))
    ) shouldBe Some(
      "real-svc"
    )
  }

  it should "carry the texera.* resource attrs so a CU JVM auto-tags every emitted record" in {
    val r = OtelInit.buildResource(
      "texera-computing-unit-master",
      "texera.computing_unit.id=8,texera.workflow.id=441,texera.execution.id=1234"
    )
    val attrs: Map[String, String] = r.getAttributes.asMap.asScala.iterator.map {
      case (k, v) => k.getKey -> v.toString
    }.toMap
    attrs("texera.computing_unit.id") shouldBe "8"
    attrs("texera.workflow.id") shouldBe "441"
    attrs("texera.execution.id") shouldBe "1234"
  }

  it should "ignore malformed pairs without crashing" in {
    val r = OtelInit.buildResource("svc", ",,,=,foo,service.version=,=bar,service.version=1.0,")
    Option(
      r.getAttribute(io.opentelemetry.api.common.AttributeKey.stringKey("service.version"))
    ) shouldBe Some(
      "1.0"
    )
  }

  it should "handle empty / null input cleanly" in {
    OtelInit.parseAttrs("") shouldBe empty
    OtelInit.parseAttrs(null) shouldBe empty
  }

  // ----- end-to-end init: span emission + disable behaviour -------------

  "init" should "be a no-op when OTEL_SDK_DISABLED is explicitly set to true" in {
    val exporter = InMemorySpanExporter.create()
    val result = OtelInit.initForTest("svc", Map("OTEL_SDK_DISABLED" -> "true"), exporter)
    result shouldBe None
    exporter.getFinishedSpanItems.asScala shouldBe empty
  }

  it should "stay inert by default when OTEL_SDK_DISABLED is unset (issue #5367)" in {
    val exporter = InMemorySpanExporter.create()
    val result = OtelInit.initForTest(
      "svc",
      Map(
        // OTEL_SDK_DISABLED omitted; the SDK stays disabled by default.
        "OTEL_EXPORTER_OTLP_ENDPOINT" -> "http://localhost:4317"
      ),
      exporter
    )
    result shouldBe None
    exporter.getFinishedSpanItems.asScala shouldBe empty
  }

  it should "stay inert for any OTEL_SDK_DISABLED value other than an explicit false" in {
    val exporter = InMemorySpanExporter.create()
    val result = OtelInit.initForTest(
      "svc",
      Map("OTEL_SDK_DISABLED" -> "", "OTEL_EXPORTER_OTLP_ENDPOINT" -> "http://localhost:4317"),
      exporter
    )
    result shouldBe None
  }

  it should "emit a single service.start span when enabled with a valid endpoint" in {
    val exporter = InMemorySpanExporter.create()
    val result = OtelInit.initForTest(
      "my-service",
      Map(
        "OTEL_SDK_DISABLED" -> "false",
        "OTEL_EXPORTER_OTLP_ENDPOINT" -> "http://localhost:4317"
      ),
      exporter
    )
    result.isDefined shouldBe true

    // BatchSpanProcessor is async; flush before reading.
    result.get
      .asInstanceOf[io.opentelemetry.sdk.OpenTelemetrySdk]
      .getSdkTracerProvider
      .forceFlush()
      .join(2, java.util.concurrent.TimeUnit.SECONDS)

    val spans = exporter.getFinishedSpanItems.asScala
    spans should have size 1
    spans.head.getName shouldBe "service.start"
  }

  it should "refuse to initialize when the endpoint scheme is file://" in {
    val exporter = InMemorySpanExporter.create()
    val result = OtelInit.initForTest(
      "svc",
      Map(
        "OTEL_SDK_DISABLED" -> "false",
        "OTEL_EXPORTER_OTLP_ENDPOINT" -> "file:///etc/passwd"
      ),
      exporter
    )
    result shouldBe None
    exporter.getFinishedSpanItems.asScala shouldBe empty
  }

  it should "refuse to initialize when the endpoint host is off-allowlist" in {
    val exporter = InMemorySpanExporter.create()
    val result = OtelInit.initForTest(
      "svc",
      Map(
        "OTEL_SDK_DISABLED" -> "false",
        "OTEL_EXPORTER_OTLP_ENDPOINT" -> "http://attacker.example.com:4317"
      ),
      exporter
    )
    result shouldBe None
    exporter.getFinishedSpanItems.asScala shouldBe empty
  }

  it should "be idempotent — second init returns the same instance" in {
    val exporter = InMemorySpanExporter.create()
    val env = Map(
      "OTEL_SDK_DISABLED" -> "false",
      "OTEL_EXPORTER_OTLP_ENDPOINT" -> "http://localhost:4317"
    )
    val first = OtelInit.initForTest("svc", env, exporter)
    val second = OtelInit.initForTest("svc", env, exporter)
    second shouldBe first
  }
}
