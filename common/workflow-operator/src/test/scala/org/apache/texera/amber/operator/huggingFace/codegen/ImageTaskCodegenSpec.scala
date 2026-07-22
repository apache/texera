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

class ImageTaskCodegenSpec extends AnyFlatSpec with Matchers {

  private def makeCtx(
      hfApiToken: EncodableString = "token",
      modelId: EncodableString = "google/vit-base-patch16-224",
      promptColumn: EncodableString = "prompt",
      resultColumn: EncodableString = "hf_response",
      task: EncodableString = "image-classification",
      systemPrompt: EncodableString = "You are a helpful assistant.",
      safeMaxTokens: Int = 256,
      safeTemp: Double = 0.7,
      imageInput: EncodableString = "",
      inputImageColumn: EncodableString = "",
      candidateLabels: EncodableString = ""
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
      imageInput = imageInput,
      inputImageColumn = inputImageColumn,
      candidateLabels = candidateLabels
    )

  "ImageTaskCodegen.task" should "be the canonical image-classification string" in {
    ImageTaskCodegen.task shouldBe "image-classification"
  }

  "ImageTaskCodegen.tasks" should "cover exactly the nine image-pipeline tasks" in {
    ImageTaskCodegen.tasks shouldBe Set(
      "image-classification",
      "object-detection",
      "image-segmentation",
      "image-to-text",
      "visual-question-answering",
      "document-question-answering",
      "zero-shot-image-classification",
      "image-text-to-text",
      "image-to-image"
    )
    ImageTaskCodegen.tasks should have size 9
  }

  "ImageTaskCodegen.payloadPython" should "send raw image bytes for image-only tasks" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include("if task in image_only_tasks:")
    out should include("payload = current_image_bytes")
    out should include("use_raw_binary_body = True")
    out should include("raw_binary_headers = image_headers")
  }

  it should "bundle a base64 image and question for VQA / document-QA tasks" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include(
      """elif task in ("visual-question-answering", "document-question-answering"):"""
    )
    out should include("self._image_input_as_base64(current_image_bytes)")
    out should include(""""question": prompt_value""")
  }

  it should "validate that zero-shot classification supplies at least two candidate labels" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include("""elif task == "zero-shot-image-classification":""")
    out should include("if len(labels) < 2:")
    out should include("raise ValueError")
    out should include("candidate_labels")
  }

  "ImageTaskCodegen.parsePython" should "extract chat-style content for image-text-to-text" in {
    val out = ImageTaskCodegen.parsePython(makeCtx())
    out should include("choices")
    out should include("message")
    out should include("content")
  }

  it should "normalize image-to-image URL responses through _url_to_data_url" in {
    val out = ImageTaskCodegen.parsePython(makeCtx())
    out should include("self._url_to_data_url(")
    out should include("data:image/png;base64,")
  }

  it should "fall back to json.dumps(body) for structured tasks" in {
    val out = ImageTaskCodegen.parsePython(makeCtx())
    out should include("json.dumps(body)")
  }

  "ImageTaskCodegen snippets" should "never inline raw CodegenContext string values" in {
    // The snippets are static and reference only self.* attributes; the base
    // class decodes user-supplied strings safely at runtime. Sentinel values
    // are distinctive and non-overlapping with the static template text.
    val ctx = makeCtx(
      hfApiToken = "MARKER_TOKEN_zXyq42",
      modelId = "MARKER_MODEL_zXyq42",
      promptColumn = "MARKER_PROMPT_zXyq42",
      resultColumn = "MARKER_RESULT_zXyq42",
      task = "MARKER_TASK_zXyq42",
      systemPrompt = "MARKER_SYSTEM_zXyq42",
      imageInput = "MARKER_IMAGE_zXyq42",
      inputImageColumn = "MARKER_IMAGECOL_zXyq42",
      candidateLabels = "MARKER_LABELS_zXyq42"
    )
    val payload = ImageTaskCodegen.payloadPython(ctx)
    val parse = ImageTaskCodegen.parsePython(ctx)

    payload should not include "MARKER_TOKEN_zXyq42"
    payload should not include "MARKER_MODEL_zXyq42"
    payload should not include "MARKER_PROMPT_zXyq42"
    payload should not include "MARKER_RESULT_zXyq42"
    payload should not include "MARKER_TASK_zXyq42"
    payload should not include "MARKER_SYSTEM_zXyq42"
    payload should not include "MARKER_IMAGE_zXyq42"
    payload should not include "MARKER_IMAGECOL_zXyq42"
    payload should not include "MARKER_LABELS_zXyq42"
    parse should not include "MARKER_TOKEN_zXyq42"
    parse should not include "MARKER_MODEL_zXyq42"
    parse should not include "MARKER_PROMPT_zXyq42"
    parse should not include "MARKER_RESULT_zXyq42"
    parse should not include "MARKER_TASK_zXyq42"
    parse should not include "MARKER_SYSTEM_zXyq42"
    parse should not include "MARKER_IMAGE_zXyq42"
    parse should not include "MARKER_IMAGECOL_zXyq42"
    parse should not include "MARKER_LABELS_zXyq42"
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    // image-task payload/parse are static — they reference only self.*
    // attributes, never ctx fields. Two unrelated contexts must serialise to
    // byte-identical Python. A future refactor that accidentally consumes a
    // ctx field will regress here.
    val ctxA = makeCtx(
      hfApiToken = "token-A",
      modelId = "model-A",
      promptColumn = "col-A",
      resultColumn = "result-A",
      task = "image-classification",
      systemPrompt = "system-A",
      safeMaxTokens = 1,
      safeTemp = 0.0,
      imageInput = "image-A",
      inputImageColumn = "image-col-A",
      candidateLabels = "labels-A"
    )
    val ctxB = makeCtx(
      hfApiToken = "token-B",
      modelId = "model-B",
      promptColumn = "col-B",
      resultColumn = "result-B",
      task = "zero-shot-image-classification",
      systemPrompt = "system-B",
      safeMaxTokens = 4096,
      safeTemp = 2.0,
      imageInput = "image-B",
      inputImageColumn = "image-col-B",
      candidateLabels = "labels-B"
    )

    ImageTaskCodegen.payloadPython(ctxA) shouldBe ImageTaskCodegen.payloadPython(ctxB)
    ImageTaskCodegen.parsePython(ctxA) shouldBe ImageTaskCodegen.parsePython(ctxB)
  }
}
