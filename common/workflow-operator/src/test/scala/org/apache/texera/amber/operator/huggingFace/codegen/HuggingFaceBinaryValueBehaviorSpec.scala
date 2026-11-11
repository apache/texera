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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.Try

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
    val script = scala.io.Source.fromInputStream(
      getClass.getResourceAsStream("/python/hf_binary_value_probe.py"),
      "UTF-8"
    )
    val body =
      try script.mkString
      finally script.close()
    val file = Files.createTempFile("hf_binary_value_probe", ".py")
    Files.write(file, body.getBytes(StandardCharsets.UTF_8))
    file.toFile.deleteOnExit()
    file
  }

  /** Runs the probe once over the rendered operator; one entry per case. */
  private lazy val outcomes: JsonNode = {
    val exe = pythonExe.getOrElse(cancel("no python interpreter available to run the probe"))
    val ctx = CodegenContext(
      hfApiToken = "token",
      modelId = "google/vit-base-patch16-224",
      promptColumn = "prompt",
      resultColumn = "hf_response",
      task = AnyCodegen.task,
      systemPrompt = "",
      safeMaxTokens = 256,
      safeTemp = 0.7
    )
    val source = HuggingFaceCodegenBase.render(ctx, AnyCodegen)

    // Files rather than pipes for the child's output, so a hung probe still
    // reaches the timeout below (see HuggingFaceParseBehaviorSpec).
    val outFile = Files.createTempFile("hf_binary_value_probe_out", ".json")
    val errFile = Files.createTempFile("hf_binary_value_probe_err", ".txt")
    try {
      val process = new ProcessBuilder(exe, probe.toString)
        .redirectOutput(outFile.toFile)
        .redirectError(errFile.toFile)
        .start()
      process.getOutputStream.write(source.getBytes(StandardCharsets.UTF_8))
      process.getOutputStream.close()
      if (!process.waitFor(120, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        fail("probe timed out")
      }
      val out = new String(Files.readAllBytes(outFile), StandardCharsets.UTF_8)
      val err = new String(Files.readAllBytes(errFile), StandardCharsets.UTF_8)
      withClue(s"probe stderr: $err\nprobe stdout: $out\n") { process.exitValue() shouldBe 0 }
      mapper.readTree(out)
    } finally {
      Files.deleteIfExists(outFile)
      Files.deleteIfExists(errFile)
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
