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

class AudioTaskCodegenSpec extends AnyFlatSpec with Matchers {

  private def makeCtx(
      hfApiToken: EncodableString = "token",
      modelId: EncodableString = "openai/whisper-large-v3",
      promptColumn: EncodableString = "prompt",
      resultColumn: EncodableString = "hf_response",
      task: EncodableString = "automatic-speech-recognition",
      systemPrompt: EncodableString = "You are a helpful assistant.",
      safeMaxTokens: Int = 256,
      safeTemp: Double = 0.7,
      audioInput: EncodableString = "clip.wav",
      inputAudioColumn: EncodableString = "audio"
  ): CodegenContext =
    CodegenContext(
      hfApiToken = hfApiToken,
      modelId = modelId,
      promptColumn = promptColumn,
      resultColumn = resultColumn,
      task = task,
      systemPrompt = systemPrompt,
      safeMaxTokens = safeMaxTokens,
      safeTemp = safeTemp,
      audioInput = audioInput,
      inputAudioColumn = inputAudioColumn
    )

  "AudioTaskCodegen.task" should "be the canonical automatic-speech-recognition string" in {
    AudioTaskCodegen.task shouldBe "automatic-speech-recognition"
  }

  "AudioTaskCodegen.tasks" should "cover exactly the three audio task families" in {
    AudioTaskCodegen.tasks shouldBe Set(
      "automatic-speech-recognition",
      "audio-classification",
      "text-to-speech"
    )
  }

  it should "include its primary task among the handled tasks" in {
    AudioTaskCodegen.tasks should contain(AudioTaskCodegen.task)
  }

  "AudioTaskCodegen.payloadPython" should "send raw audio bytes as the binary body for audio-only tasks" in {
    val out = AudioTaskCodegen.payloadPython(makeCtx())
    out should include("if task in audio_only_tasks:")
    out should include("payload = current_audio_bytes")
    out should include("use_raw_binary_body = True")
    out should include("raw_binary_headers = audio_headers")
  }

  it should "send a JSON inputs payload for the prompt-driven text-to-speech task" in {
    val out = AudioTaskCodegen.payloadPython(makeCtx())
    out should include("""elif task == "text-to-speech":""")
    out should include("""payload = {"inputs": prompt_value}""")
  }

  "AudioTaskCodegen.parsePython" should "branch on all three audio tasks" in {
    val out = AudioTaskCodegen.parsePython(makeCtx())
    out should include("""if task == "text-to-speech":""")
    out should include("""elif task == "automatic-speech-recognition":""")
    out should include("""elif task == "audio-classification":""")
  }

  it should "normalise text-to-speech URL and base64 envelopes into an audio data URL" in {
    val out = AudioTaskCodegen.parsePython(makeCtx())
    out should include("self._url_to_data_url(")
    out should include("b64_json")
    out should include("data:audio/mpeg;base64,")
  }

  it should "pull recognised text from the ASR response before falling back to JSON" in {
    val out = AudioTaskCodegen.parsePython(makeCtx())
    out should include("""body["text"]""")
    out should include("""body["generated_text"]""")
    out should include("return json.dumps(body)")
  }

  "AudioTaskCodegen snippets" should "never inline raw CodegenContext string values" in {
    // The snippets reference only self.* attributes and shared local names; the
    // base class decodes user-supplied strings safely at runtime. Sentinel
    // values are distinctive and non-overlapping with the static template text.
    val ctx = makeCtx(
      hfApiToken = "MARKER_TOKEN_zXyq42",
      modelId = "MARKER_MODEL_zXyq42",
      promptColumn = "MARKER_PROMPT_zXyq42",
      resultColumn = "MARKER_RESULT_zXyq42",
      task = "MARKER_TASK_zXyq42",
      systemPrompt = "MARKER_SYSTEM_zXyq42",
      audioInput = "MARKER_AUDIO_zXyq42",
      inputAudioColumn = "MARKER_AUDIOCOL_zXyq42"
    )
    val payload = AudioTaskCodegen.payloadPython(ctx)
    val parse = AudioTaskCodegen.parsePython(ctx)

    for (
      marker <- Seq(
        "MARKER_TOKEN_zXyq42",
        "MARKER_MODEL_zXyq42",
        "MARKER_PROMPT_zXyq42",
        "MARKER_RESULT_zXyq42",
        "MARKER_TASK_zXyq42",
        "MARKER_SYSTEM_zXyq42",
        "MARKER_AUDIO_zXyq42",
        "MARKER_AUDIOCOL_zXyq42"
      )
    ) {
      payload should not include marker
      parse should not include marker
    }
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    // The payload/parse snippets are static: they reference only self.*
    // attributes and shared local names, never ctx fields. Two unrelated
    // contexts must serialise to byte-identical Python. A future refactor that
    // accidentally consumes a ctx field will regress here.
    val ctxA = makeCtx(
      hfApiToken = "token-A",
      modelId = "model-A",
      promptColumn = "col-A",
      resultColumn = "result-A",
      systemPrompt = "system-A",
      audioInput = "audio-A",
      inputAudioColumn = "audiocol-A",
      safeMaxTokens = 1,
      safeTemp = 0.0
    )
    val ctxB = makeCtx(
      hfApiToken = "token-B",
      modelId = "model-B",
      promptColumn = "col-B",
      resultColumn = "result-B",
      systemPrompt = "system-B",
      audioInput = "audio-B",
      inputAudioColumn = "audiocol-B",
      safeMaxTokens = 4096,
      safeTemp = 2.0
    )

    AudioTaskCodegen.payloadPython(ctxA) shouldBe AudioTaskCodegen.payloadPython(ctxB)
    AudioTaskCodegen.parsePython(ctxA) shouldBe AudioTaskCodegen.parsePython(ctxB)
  }
}
