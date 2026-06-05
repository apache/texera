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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

/**
 * Smoke tests for the PR 6 docker-compose + collector config. Same
 * design as [[ParcaConfigSpec]] — string-level assertions, no YAML
 * parser, because the goal is to catch typos and licence-pin drift,
 * not to validate the upstream schemas.
 */
class ObservabilityComposeSpec extends AnyFlatSpec with Matchers {

  private def resolveBundled(relative: String): Path = {
    var dir = Paths.get("").toAbsolutePath
    var hops = 0
    while (hops < 10) {
      val candidate = dir.resolve(relative)
      if (Files.exists(candidate)) return candidate
      val parent = dir.getParent
      if (parent == null) return candidate
      dir = parent
      hops += 1
    }
    Paths.get(relative)
  }

  private def read(p: Path): String =
    new String(Files.readAllBytes(p), StandardCharsets.UTF_8)

  // ----- bundled paths --------------------------------------------------

  private val compose = resolveBundled("bin/single-node/docker-compose.yml")
  private val envFile = resolveBundled("bin/single-node/.env")
  private val upScript = resolveBundled("bin/single-node/up.sh")
  private val collector = resolveBundled("bin/observability/otel-collector/config.yaml")

  // ----- docker-compose: pinned images ---------------------------------

  "docker-compose.yml" should "exist" in {
    Files.exists(compose) shouldBe true
  }

  it should "pin the OSS observability image tags from LICENSING.md" in {
    val text = read(compose)
    text should include("otel/opentelemetry-collector-contrib:0.153.0")
    text should include("victoriametrics/victoria-logs:v1.50.0")
    text should include("victoriametrics/victoria-metrics:v1.144.0")
    text should include("jaegertracing/jaeger:2.18.0")
    text should include("ghcr.io/parca-dev/parca:v0.28.0")
    text should include("ghcr.io/parca-dev/parca-agent:v0.47.1")
  }

  it should "NOT reference the VictoriaMetrics enterprise images" in {
    // Tripwire: '-enterprise' images are not Apache-2.0. Texera ships
    // OSS only — anyone editing the tags must be reminded of that.
    val text = read(compose)
    text should not include "-enterprise"
  }

  it should "bind every observability host port to loopback (127.0.0.1)" in {
    val text = read(compose)
    // The four query/UI endpoints we expose to the host MUST use
    // the 127.0.0.1: prefix. Any "0.0.0.0:9428" or naked "9428:9428"
    // would publish to all interfaces — a misconfig that would
    // expose the data store to the network.
    Seq("9428", "8428", "16686", "7070").foreach { port =>
      val pattern = s""""127.0.0.1:$port:$port""""
      withClue(s"port $port should be loopback-only: ") {
        text should include(pattern)
      }
    }
  }

  it should "only ever bind the OTel collector OTLP receivers to loopback" in {
    // Dev mode publishes 127.0.0.1:4317-4318 so a Scala backend running
    // outside docker (sbt / IntelliJ) can emit telemetry. The hard
    // requirement is that the receiver is NEVER bound on a non-loopback
    // address — otherwise OTLP would be reachable from the LAN.
    val text = read(compose)
    text should not include "0.0.0.0:4317"
    text should not include "0.0.0.0:4318"
    // Also reject the bare `"4317:4317"` form which docker treats as
    // "bind on all interfaces". Only the explicit loopback form is OK.
    text should not include "\"4317:4317\""
    text should not include "\"4318:4318\""
  }

  it should "only mark the parca-agent privileged (eBPF needs CAP_SYS_ADMIN)" in {
    // privileged: true is dangerous; we want it on exactly one
    // service. If a future contributor copies the agent block as a
    // template for another service, this test trips.
    val text = read(compose)
    val priv = "privileged: true".r.findAllIn(text).length
    priv shouldBe 1
  }

  it should "give every observability backend a profile so it can be disabled" in {
    val text = read(compose)
    Seq(
      "observability-collector",
      "observability-logs",
      "observability-metrics",
      "observability-traces",
      "observability-profiles"
    ).foreach { profile =>
      text should include(profile)
    }
  }

  it should "set a memory limit on every observability service" in {
    // The new section lives below "Part 5: Observability stack".
    val text = read(compose).split("Part 5: Observability stack").last
    // Six services, each gets a `deploy.resources.limits.memory:` line.
    val memoryLimits = "memory:".r.findAllIn(text).length
    memoryLimits should be >= 6
  }

  // ----- .env defaults -------------------------------------------------

  ".env" should "default COMPOSE_PROFILES to include every observability profile" in {
    val text = read(envFile)
    text should include("COMPOSE_PROFILES=")
    Seq(
      "observability-collector",
      "observability-logs",
      "observability-metrics",
      "observability-traces",
      "observability-profiles"
    ).foreach { profile =>
      val grepCount = text.split('\n').count(line =>
        !line.trim.startsWith("#") && line.contains(profile)
      )
      withClue(s"$profile should be in default COMPOSE_PROFILES (non-comment): ")(
        grepCount should be >= 1
      )
    }
  }

  // ----- up.sh ----------------------------------------------------------

  "up.sh" should "honor each per-signal disable env var" in {
    val text = read(upScript)
    Seq(
      "TEXERA_OBSERVABILITY_LOGS",
      "TEXERA_OBSERVABILITY_METRICS",
      "TEXERA_OBSERVABILITY_TRACES",
      "TEXERA_OBSERVABILITY_PROFILES",
      "TEXERA_OBSERVABILITY_COLLECTOR"
    ).foreach { v =>
      text should include(v)
    }
  }

  // ----- otel-collector config -----------------------------------------

  "otel-collector/config.yaml" should "exist and declare all three signal pipelines" in {
    Files.exists(collector) shouldBe true
    val text = read(collector)
    // The Service block defines exactly three pipelines.
    text should include("logs:")
    text should include("metrics:")
    text should include("traces:")
  }

  it should "route metrics via prometheusremotewrite to VictoriaMetrics" in {
    val text = read(collector)
    text should include("prometheusremotewrite")
    text should include("victoriametrics:8428")
  }

  it should "route logs to VictoriaLogs over OTLP HTTP" in {
    val text = read(collector)
    text should include("otlphttp/victorialogs")
    text should include("victorialogs:9428")
  }

  it should "route traces to Jaeger over OTLP gRPC" in {
    val text = read(collector)
    text should include("otlp/jaeger")
    text should include("jaeger:4317")
  }

  it should "cap incoming OTLP message size (DoS guard)" in {
    val text = read(collector)
    text should include("max_recv_msg_size_mib")
  }

  it should "configure memory_limiter to bound collector memory" in {
    val text = read(collector)
    text should include("memory_limiter")
    text should include("limit_mib")
  }
}
