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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.texera.amber.operator.huggingFace.codegen.HuggingFaceCodegenFixtures.textContext
import org.apache.texera.amber.util.PythonTestSupport.{
  configuredPython,
  copyScriptResource,
  withProbe
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Path
import scala.jdk.CollectionConverters._

/**
  * Executes the *generated* provider lookup and request loop to check where each
  * request goes when the Hub's provider mapping is or is not usable (#9723).
  *
  * The Hub now returns `"inferenceProviderMapping": {}` for models that are
  * still served, and hf-inference no longer serves text-generation models, so
  * falling back to hf-inference failed every text-generation row. The expected
  * URL is the router's OpenAI-compatible route named in the issue, which picks a
  * live provider itself and takes the Hub model ID. The probe stubs only
  * `requests`, so nothing reaches the network.
  */
class HuggingFaceProviderRouteBehaviorSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  private val model = "openai/gpt-oss-20b"

  private val routerChat = "https://router.huggingface.co/v1/chat/completions"

  private lazy val probe: Path =
    copyScriptResource(
      getClass.getResourceAsStream("/python/hf_provider_route_probe.py"),
      "hf_provider_route_probe"
    )

  /** Runs one row for `task` with the given Hub reply; returns the (url, model) posted. */
  private def posts(task: String, hub: String): Seq[(String, String)] = {
    val exe =
      configuredPython().getOrElse(cancel("no python interpreter available to run the probe"))
    val request = mapper.createObjectNode()
    request.put(
      "source",
      HuggingFaceCodegenBase.render(textContext.copy(modelId = model), TextGenCodegen)
    )
    request.put("model", model)
    val c = request.putArray("cases").addObject()
    c.put("task", task)
    c.set("hub", mapper.readTree(hub))

    withProbe(exe, probe, "hf_provider_route_probe", 60, mapper.writeValueAsBytes(request)) {
      (exitCode, out, err) =>
        withClue(s"probe stderr: $err\nprobe stdout: $out\n") { exitCode shouldBe 0 }
        mapper
          .readTree(out)
          .get("results")
          .get(0)
          .get("posts")
          .elements()
          .asScala
          .map(p => (p.get("url").asText(), p.get("model").asText()))
          .toSeq
    }
  }

  // The exact Hub reply quoted in the issue.
  private val emptyMapping = """{"status": 200, "body": {"inferenceProviderMapping": {}}}"""

  "The generated request loop" should "send text generation to the router's chat route when the Hub lists no provider" in {
    posts("text-generation", emptyMapping) shouldBe Seq((routerChat, model))
  }

  it should "send text generation to the router's chat route when the Hub lookup fails" in {
    posts("text-generation", """{"status": 500}""") shouldBe Seq((routerChat, model))
    posts("text-generation", """{"raise": true}""") shouldBe Seq((routerChat, model))
  }

  it should "keep a live provider from the Hub on its own route and model name" in {
    val live =
      """{"status": 200, "body": {"inferenceProviderMapping":
        |{"cerebras": {"status": "live", "providerId": "gpt-oss-20b"}}}}""".stripMargin
    posts("text-generation", live) shouldBe
      Seq(("https://router.huggingface.co/cerebras/v1/chat/completions", "gpt-oss-20b"))
  }

  it should "keep the hf-inference fallback for image-text-to-text" in {
    // It shares the chat branch with text generation, but the fix for #9723 only
    // moves text generation to the router's chat route.
    posts("image-text-to-text", emptyMapping) shouldBe
      Seq((s"https://router.huggingface.co/hf-inference/models/$model/v1/chat/completions", model))
  }

  it should "keep the hf-inference fallback for pipeline tasks" in {
    // Only text generation moves to the router's chat route; pipeline tasks have
    // no chat payload, so they still post to hf-inference with the model in the path.
    posts("question-answering", emptyMapping) shouldBe
      Seq((s"https://router.huggingface.co/hf-inference/models/$model", ""))
  }
}
