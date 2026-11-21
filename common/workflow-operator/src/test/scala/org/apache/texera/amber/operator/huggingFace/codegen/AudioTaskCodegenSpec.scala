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

import org.apache.texera.amber.operator.huggingFace.codegen.HuggingFaceCodegenFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AudioTaskCodegenSpec extends AnyFlatSpec with Matchers {

  private val ctx = textContext.copy(
    task = "automatic-speech-recognition",
    modelId = "openai/whisper-large-v3",
    audioInput = "clip.wav",
    inputAudioColumn = "audio"
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
    val out = AudioTaskCodegen.payloadPython(ctx)
    out should include("if task in audio_only_tasks:")
    out should include("payload = current_audio_bytes")
    out should include("use_raw_binary_body = True")
    out should include("raw_binary_headers = audio_headers")
  }

  it should "send a JSON inputs payload for the prompt-driven text-to-speech task" in {
    val out = AudioTaskCodegen.payloadPython(ctx)
    out should include("""elif task == "text-to-speech":""")
    out should include("""payload = {"inputs": prompt_value}""")
  }

  "AudioTaskCodegen.parsePython" should "branch on all three audio tasks" in {
    val out = AudioTaskCodegen.parsePython(ctx)
    out should include("""if task == "text-to-speech":""")
    out should include("""elif task == "automatic-speech-recognition":""")
    out should include("""elif task == "audio-classification":""")
  }

  it should "normalise text-to-speech URL and base64 envelopes into an audio data URL" in {
    val out = AudioTaskCodegen.parsePython(ctx)
    out should include("self._url_to_data_url(")
    out should include("b64_json")
    out should include("data:audio/mpeg;base64,")
  }

  it should "pull recognised text from the ASR response before falling back to JSON" in {
    val out = AudioTaskCodegen.parsePython(ctx)
    out should include("""body.get("text")""")
    out should include("""body.get("generated_text")""")
    out should include("return json.dumps(body)")
  }

  // #8869: the parse trusted whatever the provider returned, so a malformed 200
  // produced a corrupt or non-string cell rather than falling back to the body —
  // e.g. a null b64_json became the literal "data:audio/mpeg;base64,None".
  it should "require b64_json to be a string before building a data URL" in {
    val out = AudioTaskCodegen.parsePython(ctx)
    out should include("""isinstance(b64, str)""")
    out should not include ("""f"data:audio/mpeg;base64,{audio['b64_json']}"""")
    out should not include ("""f"data:audio/mpeg;base64,{data[0]['b64_json']}"""")
  }

  it should "guard the url branches the way the output branch already does" in {
    // The "output" branch checked isinstance(url, str) while the sibling "audio"
    // and "data" branches handed whatever they found to _url_to_data_url.
    val out = AudioTaskCodegen.parsePython(ctx)
    out.split("""isinstance\(url, str\)""").length - 1 should be >= 3
    out should not include ("""return self._url_to_data_url(audio["url"])""")
    out should not include ("""return self._url_to_data_url(data[0]["url"])""")
  }

  it should "check data is a list before indexing it" in {
    val out = AudioTaskCodegen.parsePython(ctx)
    out should include("""isinstance(data, list)""")
  }

  it should "return recognised text only when it is a string" in {
    val out = AudioTaskCodegen.parsePython(ctx)
    out should include("""isinstance(text, str)""")
    out should not include ("""return body["text"]""")
    out should not include ("""return body["generated_text"]""")
  }

  "AudioTaskCodegen snippets" should "never inline raw CodegenContext string values" in {
    assertNoRawContextValues(AudioTaskCodegen)
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    assertStaticSnippets(AudioTaskCodegen, "automatic-speech-recognition")
  }
}
