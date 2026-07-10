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
  * Codegen for the Hugging Face image-pipeline task family.
  *
  * Splits into two sub-families:
  *  - "image-only" tasks send raw image bytes as the request body and don't
  *    consume the prompt column: image-classification, object-detection,
  *    image-segmentation, image-to-text.
  *  - "image + prompt" tasks bundle a base64 image and a text prompt in a
  *    JSON payload: visual-question-answering, document-question-answering,
  *    zero-shot-image-classification, image-text-to-text, image-to-image.
  *
  * Per-row `current_image_bytes` is resolved upstream in
  * [[PythonCodegenBase]]'s `process_table` (either from the operator's
  * uploaded image or from `INPUT_IMAGE_COLUMN`). The image helpers
  * (`_read_image_input`, `_compress_image_bytes`, `_image_input_as_base64`,
  * `_read_binary_value`, `_looks_like_html`, `_html_to_image_bytes`,
  * `_extract_json_arg`) live in PythonCodegenBase alongside the per-task
  * tuples (`image_only_tasks`, `image_prompt_tasks`, `image_tasks`).
  */
object ImageTaskCodegen extends TaskCodegen {

  /** Primary key for registration; the dispatcher maps every task in
    * [[tasks]] to this codegen.
    */
  override val task: String = "image-classification"

  /** All HF tasks routed through this codegen. */
  override val tasks: Set[String] = Set(
    // image-only
    "image-classification",
    "object-detection",
    "image-segmentation",
    "image-to-text",
    // image + prompt
    "visual-question-answering",
    "document-question-answering",
    "zero-shot-image-classification",
    "image-text-to-text",
    "image-to-image"
  )

  override def payloadPython(ctx: CodegenContext): String =
    """            if task in image_only_tasks:
      |                payload = current_image_bytes
      |                use_raw_binary_body = True
      |                raw_binary_headers = image_headers
      |            elif task in ("visual-question-answering", "document-question-answering"):
      |                payload = {
      |                    "inputs": {
      |                        "image": self._image_input_as_base64(current_image_bytes),
      |                        "question": prompt_value,
      |                    }
      |                }
      |            elif task == "image-text-to-text":
      |                img_b64 = self._image_input_as_base64(current_image_bytes)
      |                payload = {
      |                    "model": self.MODEL_ID,
      |                    "messages": [{
      |                        "role": "user",
      |                        "content": [
      |                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
      |                            {"type": "text", "text": prompt_value if prompt_value else "Describe this image."},
      |                        ],
      |                    }],
      |                    "max_tokens": self.MAX_NEW_TOKENS,
      |                }
      |            elif task == "image-to-image":
      |                payload = current_image_bytes
      |                use_raw_binary_body = True
      |                raw_binary_headers = image_headers
      |            elif task == "zero-shot-image-classification":
      |                # Prefer the dedicated candidateLabels property; fall back to
      |                # the prompt column for backward compatibility.
      |                label_source = (self.CANDIDATE_LABELS or "").strip() if self.CANDIDATE_LABELS else ""
      |                if not label_source and prompt_value:
      |                    label_source = prompt_value
      |                labels = [s.strip() for s in label_source.split(",") if s.strip()]
      |                if len(labels) < 2:
      |                    raise ValueError(
      |                        "zero-shot-image-classification requires at least 2 candidate "
      |                        "labels: provide a comma-separated list in the Candidate Labels field."
      |                    )
      |                payload = {
      |                    "inputs": self._image_input_as_base64(current_image_bytes),
      |                    "parameters": {"candidate_labels": labels},
      |                }
      |            else:
      |                payload = {"inputs": prompt_value}""".stripMargin

  override def parsePython(ctx: CodegenContext): String =
    """            if task == "image-to-text":
      |                if isinstance(body, dict):
      |                    if "md_results" in body:
      |                        return body["md_results"]
      |                    if "choices" in body:
      |                        return body["choices"][0]["message"]["content"]
      |                if isinstance(body, list) and body and isinstance(body[0], dict):
      |                    return body[0].get("generated_text", json.dumps(body))
      |                return json.dumps(body)
      |            elif task in ("visual-question-answering", "document-question-answering"):
      |                if isinstance(body, dict):
      |                    return body.get("answer", json.dumps(body))
      |                return json.dumps(body)
      |            elif task == "image-text-to-text":
      |                if isinstance(body, dict) and "choices" in body:
      |                    return body["choices"][0]["message"]["content"]
      |                if isinstance(body, list) and body and isinstance(body[0], dict):
      |                    return body[0].get("generated_text", json.dumps(body))
      |                return json.dumps(body)
      |            elif task == "image-to-image":
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
      |            elif task in ("image-classification", "object-detection", "image-segmentation", "zero-shot-image-classification"):
      |                return json.dumps(body)""".stripMargin
}
