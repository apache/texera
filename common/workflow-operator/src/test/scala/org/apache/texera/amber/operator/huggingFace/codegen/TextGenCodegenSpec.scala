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

import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TextGenCodegenSpec extends AnyFlatSpec with Matchers {

  private def makeCtx(
      hfApiToken: EncodableString = "token",
      modelId: EncodableString = "Qwen/Qwen2.5-72B-Instruct",
      promptColumn: EncodableString = "prompt",
      resultColumn: EncodableString = "hf_response",
      task: EncodableString = "text-generation",
      systemPrompt: EncodableString = "You are a helpful assistant.",
      safeMaxTokens: Int = 256,
      safeTemp: Double = 0.7
  ): CodegenContext =
    CodegenContext(
      hfApiToken = hfApiToken,
      modelId = modelId,
      promptColumn = promptColumn,
      resultColumn = resultColumn,
      task = task,
      systemPrompt = systemPrompt,
      safeMaxTokens = safeMaxTokens,
      safeTemp = safeTemp
    )

  "TextGenCodegen.task" should "be the canonical text-generation string" in {
    TextGenCodegen.task shouldBe "text-generation"
  }

  "TextGenCodegen.payloadPython" should "open with the text-generation branch" in {
    val out = TextGenCodegen.payloadPython(makeCtx())
    out should include("""if task == "text-generation":""")
  }

  it should "emit the OpenAI chat-completions payload shape (messages, max_tokens, temperature)" in {
    val out = TextGenCodegen.payloadPython(makeCtx())
    out should include("messages")
    out should include("max_tokens")
    out should include("temperature")
  }

  it should "include the else fallback that ships the raw prompt as inputs" in {
    val out = TextGenCodegen.payloadPython(makeCtx())
    out should include("""payload = {"inputs": prompt_value}""")
  }

  "TextGenCodegen.parsePython" should "pull text out of choices[0].message.content" in {
    val out = TextGenCodegen.parsePython(makeCtx())
    out should include("choices")
    out should include("message")
    out should include("content")
  }

  "TextGenCodegen snippets" should "never inline raw CodegenContext string values" in {
    // The snippets must reference self.* attributes — the base class decodes
    // user-supplied strings safely at runtime. Sentinel values chosen to be
    // distinctive and non-overlapping with the static template text.
    val ctx = makeCtx(
      hfApiToken = "MARKER_TOKEN_zXyq42",
      modelId = "MARKER_MODEL_zXyq42",
      promptColumn = "MARKER_PROMPT_zXyq42",
      resultColumn = "MARKER_RESULT_zXyq42",
      task = "MARKER_TASK_zXyq42",
      systemPrompt = "MARKER_SYSTEM_zXyq42"
    )
    val payload = TextGenCodegen.payloadPython(ctx)
    val parse = TextGenCodegen.parsePython(ctx)

    payload should not include "MARKER_TOKEN_zXyq42"
    payload should not include "MARKER_MODEL_zXyq42"
    payload should not include "MARKER_PROMPT_zXyq42"
    payload should not include "MARKER_RESULT_zXyq42"
    payload should not include "MARKER_TASK_zXyq42"
    payload should not include "MARKER_SYSTEM_zXyq42"
    parse should not include "MARKER_TOKEN_zXyq42"
    parse should not include "MARKER_MODEL_zXyq42"
    parse should not include "MARKER_PROMPT_zXyq42"
    parse should not include "MARKER_RESULT_zXyq42"
    parse should not include "MARKER_TASK_zXyq42"
    parse should not include "MARKER_SYSTEM_zXyq42"
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    // text-generation's payload/parse are static — they reference only
    // self.* attributes, never ctx fields. Two unrelated contexts must
    // serialise to byte-identical Python. A future refactor that
    // accidentally consumes a ctx field will regress here.
    val ctxA = makeCtx(
      hfApiToken = "token-A",
      modelId = "model-A",
      promptColumn = "col-A",
      resultColumn = "result-A",
      systemPrompt = "system-A",
      safeMaxTokens = 1,
      safeTemp = 0.0
    )
    val ctxB = makeCtx(
      hfApiToken = "token-B",
      modelId = "model-B",
      promptColumn = "col-B",
      resultColumn = "result-B",
      systemPrompt = "system-B",
      safeMaxTokens = 4096,
      safeTemp = 2.0
    )

    TextGenCodegen.payloadPython(ctxA) shouldBe TextGenCodegen.payloadPython(ctxB)
    TextGenCodegen.parsePython(ctxA) shouldBe TextGenCodegen.parsePython(ctxB)
  }
}
