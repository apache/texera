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
import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Executes the *generated* response parser instead of matching its source text.
  *
  * Every other Hugging Face spec asserts on the emitted Python as a string, so a
  * parser that raises at runtime still passes them (review feedback on #8617).
  * This spec renders the operator, lifts `_parse_response` and its helpers out of
  * the result, and runs them under a real interpreter against the response shapes
  * providers actually return — valid, empty, missing, null and wrong-typed.
  *
  * The contract asserted here: `_parse_response` always returns a string and
  * never raises, because an uncaught exception is caught by the per-row handler
  * and replaces the cell with "Request failed" instead of the raw body.
  */
class HuggingFaceParseBehaviorSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  private def makeCtx(task: EncodableString): CodegenContext =
    CodegenContext(
      hfApiToken = "token",
      modelId = "Qwen/Qwen2.5-72B-Instruct",
      promptColumn = "prompt",
      resultColumn = "hf_response",
      task = task,
      systemPrompt = "You are a helpful assistant.",
      safeMaxTokens = 256,
      safeTemp = 0.7
    )

  /** Same resolution order as PythonCodeRawInvalidTextSpec: udf.conf, then PATH. */
  private lazy val pythonExe: Option[String] = {
    def fromConfig: Option[String] =
      Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
        .orElse(Try(ConfigFactory.load()).toOption)
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)

    def isRunnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(isRunnable)
  }

  private lazy val probe: Path = {
    val script =
      scala.io.Source.fromInputStream(
        getClass.getResourceAsStream("/python/hf_parse_probe.py"),
        "UTF-8"
      )
    val body =
      try script.mkString
      finally script.close()
    val file = Files.createTempFile("hf_parse_probe", ".py")
    Files.write(file, body.getBytes(StandardCharsets.UTF_8))
    file.toFile.deleteOnExit()
    file
  }

  /** Runs the generated parser for `task` over `bodies`; returns one outcome each. */
  private def parseAll(codegen: TaskCodegen, task: String, bodies: Seq[String]): Seq[String] = {
    val exe = pythonExe.getOrElse(cancel("no python interpreter available to run the probe"))
    val request = mapper.createObjectNode()
    request.put("source", HuggingFaceCodegenBase.render(makeCtx(task), codegen))
    request.put("task", task)
    val arr = request.putArray("bodies")
    bodies.foreach(b => arr.add(mapper.readTree(b)))

    val process = new ProcessBuilder(exe, probe.toString).redirectErrorStream(false).start()
    process.getOutputStream.write(mapper.writeValueAsBytes(request))
    process.getOutputStream.close()
    val out = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
    val err = new String(process.getErrorStream.readAllBytes(), StandardCharsets.UTF_8)
    if (!process.waitFor(60, TimeUnit.SECONDS)) {
      process.destroyForcibly(); fail("probe timed out")
    }
    withClue(s"probe stderr: $err\nprobe stdout: $out\n") { process.exitValue() shouldBe 0 }

    mapper
      .readTree(out)
      .get("results")
      .elements()
      .asScala
      .map(n =>
        if (n.has("raised")) s"RAISED:${n.get("raised").asText()}"
        else if (n.get("value").isNull) "RETURNED_NONE"
        else if (!n.get("value").isTextual) s"NON_STRING:${n.get("value").getNodeType}"
        else n.get("value").asText()
      )
      .toSeq
  }

  /** Shapes seen from providers: valid, empty, missing, null and wrong-typed. */
  private val malformed = Seq(
    """{"choices": []}""",
    """{"choices": [null]}""",
    """{"choices": [{"message": null}]}""",
    """{"choices": "bad"}""",
    """{"choices": {"a": 1}}""",
    """{"choices": [42]}""",
    """{"choices": [{"message": {}}]}""",
    """{"choices": [{"message": {"content": null}}]}""",
    """{"choices": [{"message": {"content": 42}}]}""",
    """{"choices": [{"no_message": true}]}""",
    """{}""",
    """[]""",
    """[{"generated_text": "x"}]"""
  )

  // Scoped to the codegens this PR touches. ImageTaskCodegen has the same chat
  // extractions (merged in #7920) and will be routed through the helper and added
  // here in a follow-up, so that change arrives with its own failing-first test.
  private val cases = Seq(
    (TextGenCodegen: TaskCodegen, "text-generation"),
    (QaRankingCodegen, "question-answering"),
    (QaRankingCodegen, "table-question-answering"),
    (QaRankingCodegen, "zero-shot-classification"),
    (QaRankingCodegen, "sentence-similarity"),
    (QaRankingCodegen, "text-ranking")
  )

  "The generated parser" should "return the raw body, never raise, on malformed chat responses" in {
    cases.foreach {
      case (codegen, task) =>
        val outcomes = parseAll(codegen, task, malformed)
        withClue(s"task=$task ") {
          outcomes.foreach { outcome =>
            outcome should not startWith "RAISED:"
            outcome should not be "RETURNED_NONE"
            outcome should not startWith "NON_STRING:"
          }
        }
    }
  }

  it should "return the assistant text for a well-formed chat response" in {
    val body = """{"choices": [{"message": {"content": "the answer"}}]}"""
    cases.foreach {
      case (codegen, task) =>
        withClue(s"task=$task ") {
          parseAll(codegen, task, Seq(body)) shouldBe Seq("the answer")
        }
    }
  }

  it should "join a content list returned as parts" in {
    val body =
      """{"choices": [{"message": {"content": [{"type": "text", "text": "a "},
        |{"type": "text", "text": "b"}]}}]}""".stripMargin.replace("\n", "")
    parseAll(TextGenCodegen, "text-generation", Seq(body)) shouldBe Seq("a b")
  }

  it should "keep the native hf-inference shapes working" in {
    parseAll(QaRankingCodegen, "question-answering", Seq("""{"answer": "Ada"}""")) shouldBe
      Seq("Ada")
    parseAll(QaRankingCodegen, "text-ranking", Seq("""[{"index": 0}]""")) shouldBe
      Seq("""[{"index": 0}]""")
  }
}
