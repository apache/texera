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

/**
  * Codegen for Hugging Face audio task families.
  *
  * ASR and audio-classification send audio bytes as the raw request body.
  * Text-to-speech is prompt-driven and sends a JSON payload; its providers
  * return either audio bytes directly or a JSON envelope pointing to audio.
  */
object AudioTaskCodegen extends TaskCodegen {

  override val task: String = "automatic-speech-recognition"

  override val tasks: Set[String] = Set(
    "automatic-speech-recognition",
    "audio-classification",
    "text-to-speech"
  )

  override def payloadPython(ctx: CodegenContext): String =
    """            if task in audio_only_tasks:
      |                payload = current_audio_bytes
      |                use_raw_binary_body = True
      |                raw_binary_headers = audio_headers
      |            elif task == "text-to-speech":
      |                payload = {"inputs": prompt_value}""".stripMargin

  override def parsePython(ctx: CodegenContext): String =
    """            if task == "text-to-speech":
      |                # Every value below comes from the provider, so each is type-checked
      |                # before use: an unchecked one lands in the result column as a data
      |                # URL nothing can play (e.g. "data:audio/mpeg;base64,None") or as a
      |                # non-string cell, instead of falling back to the body.
      |                if isinstance(body, dict):
      |                    if "output" in body:
      |                        out = body["output"]
      |                        url = out[0] if isinstance(out, list) else out
      |                        if isinstance(url, str) and url.startswith("http"):
      |                            return self._url_to_data_url(url)
      |                    if "audio" in body:
      |                        audio = body["audio"]
      |                        if isinstance(audio, dict):
      |                            url = audio.get("url")
      |                            if isinstance(url, str) and url.startswith("http"):
      |                                return self._url_to_data_url(url)
      |                            b64 = audio.get("b64_json")
      |                            if isinstance(b64, str) and b64:
      |                                return f"data:audio/mpeg;base64,{b64}"
      |                    if "data" in body:
      |                        data = body["data"]
      |                        if isinstance(data, list) and data and isinstance(data[0], dict):
      |                            url = data[0].get("url")
      |                            if isinstance(url, str) and url.startswith("http"):
      |                                return self._url_to_data_url(url)
      |                            b64 = data[0].get("b64_json")
      |                            if isinstance(b64, str) and b64:
      |                                return f"data:audio/mpeg;base64,{b64}"
      |                return json.dumps(body)
      |            elif task == "automatic-speech-recognition":
      |                if isinstance(body, dict):
      |                    text = body.get("text")
      |                    if isinstance(text, str):
      |                        return text
      |                    generated = body.get("generated_text")
      |                    if isinstance(generated, str):
      |                        return generated
      |                return json.dumps(body)
      |            elif task == "audio-classification":
      |                return json.dumps(body)""".stripMargin
}
