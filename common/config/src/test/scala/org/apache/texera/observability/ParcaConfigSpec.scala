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
 * Smoke test for the bundled Parca configuration files.
 *
 * Intentionally lightweight: we are guarding against accidental
 * deletion / emptying / tag drift, not validating Parca's schema.
 * The real config validation happens when the agent starts up
 * inside its container — but a unit-level smoke test catches typos
 * before a developer pushes them.
 */
class ParcaConfigSpec extends AnyFlatSpec with Matchers {

  // sbt runs tests with the module dir as CWD, but a developer who
  // runs `sbt test` from the project root has a different CWD. Walk
  // upwards until we find the file, bounded so a missing file fails
  // loudly rather than infinite-looping.
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
    Paths.get(relative) // will fail the existence assert below
  }

  private val parcaYaml = resolveBundled("bin/observability/parca/parca.yaml")
  private val agentEnv = resolveBundled("bin/observability/parca/parca-agent.env")
  private val readme = resolveBundled("bin/observability/parca/README.md")

  // ----- parca.yaml -----------------------------------------------------

  "parca.yaml" should "exist in bin/observability/parca/" in {
    Files.exists(parcaYaml) shouldBe true
  }

  it should "declare object_storage with the FILESYSTEM bucket type" in {
    val text = new String(Files.readAllBytes(parcaYaml), StandardCharsets.UTF_8)
    text should include("object_storage")
    text should include("FILESYSTEM")
    text should include("/var/lib/parca")
  }

  it should "include the ASF license header" in {
    val text = new String(Files.readAllBytes(parcaYaml), StandardCharsets.UTF_8)
    text should include("Apache License, Version 2.0")
  }

  // ----- parca-agent.env ------------------------------------------------

  "parca-agent.env" should "exist and pin the bundled image version reference" in {
    Files.exists(agentEnv) shouldBe true
    val text = new String(Files.readAllBytes(agentEnv), StandardCharsets.UTF_8)
    text should include("v0.47.1")
  }

  it should "point the agent at the bundled Parca server hostname" in {
    val text = new String(Files.readAllBytes(agentEnv), StandardCharsets.UTF_8)
    text should include("PARCA_AGENT_REMOTE_STORE_ADDRESS=parca:7070")
  }

  it should "carry the deployment label so the gateway can filter Texera processes" in {
    val text = new String(Files.readAllBytes(agentEnv), StandardCharsets.UTF_8)
    text should include("deployment=texera")
  }

  it should "NOT include high-cardinality labels (workflow.id / execution.id)" in {
    // Tripwire: a future contributor might be tempted to add
    // workflow.id as a static label. That blows up Parca storage.
    // This assertion makes the design intent enforceable. Comments
    // are skipped — they're allowed (and required) to explain the
    // rule.
    val configLines = new String(Files.readAllBytes(agentEnv), StandardCharsets.UTF_8)
      .linesIterator
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .toSeq
    configLines.foreach { line =>
      line should not include "workflow.id"
      line should not include "workflow_id"
      line should not include "execution.id"
      line should not include "execution_id"
    }
  }

  // ----- README ---------------------------------------------------------

  "README.md" should "exist and document the opt-out env var" in {
    Files.exists(readme) shouldBe true
    val text = new String(Files.readAllBytes(readme), StandardCharsets.UTF_8)
    text should include("TEXERA_OBSERVABILITY_PROFILES=disabled")
  }

  it should "document the Linux-only / privileged-container requirement" in {
    val text = new String(Files.readAllBytes(readme), StandardCharsets.UTF_8)
    text.toLowerCase should include("linux")
    text.toLowerCase should include("privileged")
  }
}
