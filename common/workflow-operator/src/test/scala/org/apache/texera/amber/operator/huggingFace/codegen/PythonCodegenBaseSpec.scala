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

class PythonCodegenBaseSpec extends AnyFlatSpec with Matchers {

  // A stand-in TaskCodegen whose payload/parse snippets are distinctive
  // sentinels. Because payloadPython/parsePython return plain Python source
  // fragments (not EncodableString user data), the base template must splice
  // them in verbatim, letting the spec prove the splice happened and that the
  // fragment is codegen-driven rather than hardcoded in the base.
  private object StubCodegen extends TaskCodegen {
    override def task: String = "text-generation"
    override def payloadPython(ctx: CodegenContext): String =
      "            payload = STUB_PAYLOAD_zX7q42"
    override def parsePython(ctx: CodegenContext): String =
      "            return STUB_PARSE_zX7q42"
  }

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

  "PythonCodegenBase.render" should "emit the ProcessTableOperator skeleton and shared helpers" in {
    val out = PythonCodegenBase.render(makeCtx(), StubCodegen)
    out should include("class ProcessTableOperator(UDFTableOperator):")
    out should include("def open(self):")
    out should include("def process_table(")
    out should include("def _parse_response(")
    out should include("def _resolve_providers(")
    out should include("def _post_with_fallback(")
    out should include("PROVIDER_COST_PRIORITY")
    out should include("CHAT_ROUTES")
    out should include("MAX_REMOTE_FETCH_BYTES")
  }

  it should "splice the per-task codegen's payload and parse snippets into the template" in {
    val out = PythonCodegenBase.render(makeCtx(), StubCodegen)
    out should include("STUB_PAYLOAD_zX7q42")
    out should include("STUB_PARSE_zX7q42")
  }

  it should "delegate payload/parse to the codegen rather than hardcoding a task's output" in {
    // The same context routed through two different codegens must yield the
    // spliced fragments of whichever codegen is passed, proving the base is a
    // pure host for the codegen's snippets.
    val stub = PythonCodegenBase.render(makeCtx(), StubCodegen)
    val real = PythonCodegenBase.render(makeCtx(), TextGenCodegen)
    stub should include("STUB_PAYLOAD_zX7q42")
    real should not include "STUB_PAYLOAD_zX7q42"
    // TextGenCodegen's real payload/parse markers appear only in the real run.
    // Use its system-role message, unique to TextGenCodegen: the shared base
    // template contains "messages" in other helper branches, so that alone
    // would not prove TextGenCodegen's payload was spliced in.
    real should include("""{"role": "system", "content": self.SYSTEM_PROMPT}""")
    real should include("max_tokens")
    real should include("choices")
  }

  it should "interpolate the numeric context fields as raw Python literals" in {
    val out = PythonCodegenBase.render(makeCtx(safeMaxTokens = 512, safeTemp = 0.9), StubCodegen)
    out should include("self.MAX_NEW_TOKENS = 512")
    out should include("self.TEMPERATURE = 0.9")
  }

  it should "assign user-provided strings via runtime base64 decode expressions, not raw literals" in {
    val out = PythonCodegenBase.render(makeCtx(), StubCodegen)
    // Every user-supplied string field open() assigns is set through the safe
    // decode helper. This covers all EncodableString context fields, including
    // the result/task and per-task (image/audio/context/labels/sentences)
    // fields, not just the text-generation ones.
    out should include("self.HF_API_TOKEN = self.decode_python_template(")
    out should include("self.MODEL_ID = self.decode_python_template(")
    out should include("self.PROMPT_COLUMN = self.decode_python_template(")
    out should include("self.RESULT_COLUMN = self.decode_python_template(")
    out should include("self.TASK = self.decode_python_template(")
    out should include("self.SYSTEM_PROMPT = self.decode_python_template(")
    out should include("self.IMAGE_INPUT = self.decode_python_template(")
    out should include("self.INPUT_IMAGE_COLUMN = self.decode_python_template(")
    out should include("self.AUDIO_INPUT = self.decode_python_template(")
    out should include("self.INPUT_AUDIO_COLUMN = self.decode_python_template(")
    out should include("self.CONTEXT_COLUMN = self.decode_python_template(")
    out should include("self.CANDIDATE_LABELS = self.decode_python_template(")
    out should include("self.SENTENCES_COLUMN = self.decode_python_template(")
  }

  it should "never leak raw user-provided string values into the generated source" in {
    // Sentinels contain underscores, which base64 output cannot contain, so a
    // literal match here can only mean the raw value leaked past the encoder.
    val out = PythonCodegenBase.render(
      makeCtx(
        hfApiToken = "MARKER_TOKEN_zXyq42",
        modelId = "MARKER_MODEL_zXyq42",
        promptColumn = "MARKER_PROMPT_zXyq42",
        resultColumn = "MARKER_RESULT_zXyq42",
        task = "MARKER_TASK_zXyq42",
        systemPrompt = "MARKER_SYSTEM_zXyq42"
      ),
      StubCodegen
    )
    out should not include "MARKER_TOKEN_zXyq42"
    out should not include "MARKER_MODEL_zXyq42"
    out should not include "MARKER_PROMPT_zXyq42"
    out should not include "MARKER_RESULT_zXyq42"
    out should not include "MARKER_TASK_zXyq42"
    out should not include "MARKER_SYSTEM_zXyq42"
  }
}
