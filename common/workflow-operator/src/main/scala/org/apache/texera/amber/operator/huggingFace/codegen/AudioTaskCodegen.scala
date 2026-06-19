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
      |                if isinstance(body, dict):
      |                    if "output" in body:
      |                        out = body["output"]
      |                        url = out[0] if isinstance(out, list) else out
      |                        if isinstance(url, str) and url.startswith("http"):
      |                            return self._url_to_data_url(url)
      |                    if "audio" in body:
      |                        audio = body["audio"]
      |                        if isinstance(audio, dict):
      |                            if "url" in audio:
      |                                return self._url_to_data_url(audio["url"])
      |                            if "b64_json" in audio:
      |                                return f"data:audio/mpeg;base64,{audio['b64_json']}"
      |                    if "data" in body:
      |                        data = body["data"]
      |                        if data and isinstance(data[0], dict):
      |                            if "url" in data[0]:
      |                                return self._url_to_data_url(data[0]["url"])
      |                            if "b64_json" in data[0]:
      |                                return f"data:audio/mpeg;base64,{data[0]['b64_json']}"
      |                return json.dumps(body)
      |            elif task == "automatic-speech-recognition":
      |                if isinstance(body, dict):
      |                    if "text" in body:
      |                        return body["text"]
      |                    if "generated_text" in body:
      |                        return body["generated_text"]
      |                return json.dumps(body)
      |            elif task == "audio-classification":
      |                return json.dumps(body)""".stripMargin
}
