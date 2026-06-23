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
  * Codegen for prompt-driven media generation tasks.
  *
  * Providers return media in several shapes: raw bytes, OpenAI-style
  * b64_json, or URLs. URL responses are normalized to data URLs by the
  * shared `_url_to_data_url` helper so downstream result rendering receives
  * a stable string format.
  */
object MediaGenCodegen extends TaskCodegen {

  override val task: String = "text-to-image"

  override val tasks: Set[String] = Set(
    "text-to-image",
    "text-to-video"
  )

  override def payloadPython(ctx: CodegenContext): String =
    """            payload = {"inputs": prompt_value}""".stripMargin

  override def parsePython(ctx: CodegenContext): String =
    """            if task == "text-to-image":
      |                if isinstance(body, dict):
      |                    if "output" in body:
      |                        out = body["output"]
      |                        url = out[0] if isinstance(out, list) else out
      |                        if isinstance(url, str) and url.startswith("http"):
      |                            return self._url_to_data_url(url)
      |                    if "images" in body:
      |                        images = body["images"]
      |                        if images and isinstance(images[0], dict) and "url" in images[0]:
      |                            return self._url_to_data_url(images[0]["url"])
      |                    if "data" in body:
      |                        data = body["data"]
      |                        if isinstance(data, dict) and "outputs" in data:
      |                            outputs = data["outputs"]
      |                            if outputs and isinstance(outputs[0], str) and outputs[0].startswith("http"):
      |                                return self._url_to_data_url(outputs[0])
      |                        if isinstance(data, list) and data and isinstance(data[0], dict):
      |                            if "b64_json" in data[0]:
      |                                return f"data:image/png;base64,{data[0]['b64_json']}"
      |                            if "url" in data[0]:
      |                                return self._url_to_data_url(data[0]["url"])
      |                return json.dumps(body)
      |            elif task == "text-to-video":
      |                if isinstance(body, dict):
      |                    if "output" in body:
      |                        out = body["output"]
      |                        url = out[0] if isinstance(out, list) else out
      |                        if isinstance(url, str) and url.startswith("http"):
      |                            return self._url_to_data_url(url)
      |                    if "video" in body:
      |                        video = body["video"]
      |                        if isinstance(video, dict) and "url" in video:
      |                            return self._url_to_data_url(video["url"])
      |                return json.dumps(body)""".stripMargin
}
