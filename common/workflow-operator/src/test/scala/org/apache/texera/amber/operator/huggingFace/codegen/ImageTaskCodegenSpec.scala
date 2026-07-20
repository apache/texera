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
      modelId: EncodableString = "Salesforce/blip-vqa-base",
      promptColumn: EncodableString = "prompt",
      resultColumn: EncodableString = "hf_response",
      task: EncodableString = "image-classification",
      systemPrompt: EncodableString = "You are a helpful assistant.",
      safeMaxTokens: Int = 256,
      safeTemp: Double = 0.7,
      imageInput: EncodableString = "pic.png",
      inputImageColumn: EncodableString = "image",
      candidateLabels: EncodableString = "cat,dog"
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

  "ImageTaskCodegen.tasks" should "cover exactly the nine image task families" in {
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
  }

  it should "include its primary task among the handled tasks" in {
    ImageTaskCodegen.tasks should contain(ImageTaskCodegen.task)
  }

  "ImageTaskCodegen.payloadPython" should "send raw image bytes as the binary body for image-only tasks" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include("if task in image_only_tasks:")
    out should include("payload = current_image_bytes")
    out should include("use_raw_binary_body = True")
    out should include("raw_binary_headers = image_headers")
  }

  it should "bundle a base64 image and question for the visual/document QA tasks" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include(
      """elif task in ("visual-question-answering", "document-question-answering"):"""
    )
    out should include("self._image_input_as_base64(current_image_bytes)")
    out should include(""""question": prompt_value""")
  }

  it should "build an OpenAI chat payload with the model id and token cap for image-text-to-text" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include("""elif task == "image-text-to-text":""")
    out should include("self.MODEL_ID")
    out should include("self.MAX_NEW_TOKENS")
    out should include("image_url")
    out should include("messages")
  }

  it should "guard zero-shot-image-classification with a minimum of two candidate labels" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include("""elif task == "zero-shot-image-classification":""")
    out should include("self.CANDIDATE_LABELS")
    out should include("candidate_labels")
    out should include("if len(labels) < 2:")
    out should include("raise ValueError(")
  }

  it should "route image-to-image through the raw binary body path" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include("""elif task == "image-to-image":""")
  }

  it should "fall back to shipping the raw prompt as inputs" in {
    val out = ImageTaskCodegen.payloadPython(makeCtx())
    out should include("else:")
    out should include("""payload = {"inputs": prompt_value}""")
  }

  "ImageTaskCodegen.parsePython" should "branch across the response-bearing image tasks" in {
    val out = ImageTaskCodegen.parsePython(makeCtx())
    out should include("""if task == "image-to-text":""")
    out should include(
      """elif task in ("visual-question-answering", "document-question-answering"):"""
    )
    out should include("""elif task == "image-text-to-text":""")
    out should include("""elif task == "image-to-image":""")
  }

  it should "extract chat and generated-text shapes for the text-producing image tasks" in {
    val out = ImageTaskCodegen.parsePython(makeCtx())
    out should include("""body["choices"][0]["message"]["content"]""")
    out should include("""body.get("answer"""")
    out should include("generated_text")
  }

  it should "normalise image-to-image URL and base64 outputs into a data URL" in {
    val out = ImageTaskCodegen.parsePython(makeCtx())
    out should include("self._url_to_data_url(")
    out should include("b64_json")
    out should include("data:image/png;base64,")
  }

  it should "return the raw JSON body for the pure-classification tasks" in {
    val out = ImageTaskCodegen.parsePython(makeCtx())
    out should include(
      """elif task in ("image-classification", "object-detection", "image-segmentation", "zero-shot-image-classification"):"""
    )
    out should include("return json.dumps(body)")
  }

  "ImageTaskCodegen snippets" should "never inline raw CodegenContext string values" in {
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
      imageInput = "MARKER_IMAGE_zXyq42",
      inputImageColumn = "MARKER_IMAGECOL_zXyq42",
      candidateLabels = "MARKER_LABELS_zXyq42"
    )
    val payload = ImageTaskCodegen.payloadPython(ctx)
    val parse = ImageTaskCodegen.parsePython(ctx)

    for (
      marker <- Seq(
        "MARKER_TOKEN_zXyq42",
        "MARKER_MODEL_zXyq42",
        "MARKER_PROMPT_zXyq42",
        "MARKER_RESULT_zXyq42",
        "MARKER_TASK_zXyq42",
        "MARKER_SYSTEM_zXyq42",
        "MARKER_IMAGE_zXyq42",
        "MARKER_IMAGECOL_zXyq42",
        "MARKER_LABELS_zXyq42"
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
      imageInput = "image-A",
      inputImageColumn = "imagecol-A",
      candidateLabels = "labels-A",
      safeMaxTokens = 1,
      safeTemp = 0.0
    )
    val ctxB = makeCtx(
      hfApiToken = "token-B",
      modelId = "model-B",
      promptColumn = "col-B",
      resultColumn = "result-B",
      systemPrompt = "system-B",
      imageInput = "image-B",
      inputImageColumn = "imagecol-B",
      candidateLabels = "labels-B",
      safeMaxTokens = 4096,
      safeTemp = 2.0
    )

    ImageTaskCodegen.payloadPython(ctxA) shouldBe ImageTaskCodegen.payloadPython(ctxB)
    ImageTaskCodegen.parsePython(ctxA) shouldBe ImageTaskCodegen.parsePython(ctxB)
  }
}
