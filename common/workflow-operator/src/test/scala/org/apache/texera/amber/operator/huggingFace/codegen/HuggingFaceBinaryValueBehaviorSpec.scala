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

package org.apache.texera.amber.operator.huggingFace.codegen

import org.apache.texera.amber.util.PythonTestSupport.{
  configuredPython,
  copyScriptResource,
  withProbe
}
import org.apache.texera.amber.operator.huggingFace.codegen.HuggingFaceCodegenFixtures.textContext
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import scala.jdk.CollectionConverters._

/**
  * Executes the *generated* `_read_binary_value` against column values at and
  * over the size limit (#8962).
  *
  * A column value used to be stripped, sniffed for HTML and decoded whatever its
  * size, while a remote fetch of the same payload stops at MAX_REMOTE_FETCH_BYTES.
  * The probe lifts the method out of the rendered operator, replaces the helpers
  * that parse a value with recorders, and reports whether each value was refused
  * and what touched it first.
  */
class HuggingFaceBinaryValueBehaviorSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  private object AnyCodegen extends TaskCodegen {
    override def task: String = "image-classification"
    override def payloadPython(ctx: CodegenContext): String = "            payload = {}"
    override def parsePython(ctx: CodegenContext): String = "            return ''"
  }

  private lazy val probe: Path =
    copyScriptResource(
      getClass.getResourceAsStream("/python/hf_binary_value_probe.py"),
      "hf_binary_value_probe"
    )

  /** Runs the probe once over the rendered operator; one entry per case. */
  private lazy val outcomes: JsonNode = {
    val exe =
      configuredPython().getOrElse(cancel("no python interpreter available to run the probe"))
    val ctx = textContext.copy(
      modelId = "google/vit-base-patch16-224",
      task = AnyCodegen.task,
      systemPrompt = ""
    )
    val source = HuggingFaceCodegenBase.render(ctx, AnyCodegen)

    withProbe(exe, probe, "hf_binary_value_probe", 120, source.getBytes(StandardCharsets.UTF_8)) {
      (exitCode, out, err) =>
        withClue(s"probe stderr: $err\nprobe stdout: $out\n") { exitCode shouldBe 0 }
        mapper.readTree(out)
    }
  }

  private def limit: Long = outcomes.get("limit").asLong()
  private def calls(node: JsonNode): List[String] =
    node.get("calls").elements().asScala.map(_.asText()).toList

  "The generated _read_binary_value" should "refuse a string value over the limit before parsing it" in {
    val over = outcomes.get("str_over_limit")
    withClue(s"$over: ") {
      over.path("raised").asText() shouldBe "ValueError"
      over.path("message").asText() shouldBe s"Column value exceeds the $limit character limit."
      calls(over) shouldBe empty
    }
  }

  it should "refuse a bytes value over the limit" in {
    val over = outcomes.get("bytes_over_limit")
    withClue(s"$over: ") {
      over.path("raised").asText() shouldBe "ValueError"
      over.path("message").asText() shouldBe s"Column value exceeds the $limit byte limit."
    }
  }

  it should "still parse a string value exactly at the limit" in {
    val at = outcomes.get("str_at_limit")
    withClue(s"$at: ") {
      at.has("raised") shouldBe false
      calls(at) should contain allOf ("_looks_like_html", "b64decode")
    }
  }

  it should "still return a bytes value exactly at the limit" in {
    val at = outcomes.get("bytes_at_limit")
    withClue(s"$at: ") {
      at.has("raised") shouldBe false
      at.get("returned").asLong() shouldBe limit
    }
  }
}
