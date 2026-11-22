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

class ImageTaskCodegenSpec extends AnyFlatSpec with Matchers {

  private val ctx =
    textContext.copy(task = "image-classification", modelId = "google/vit-base-patch16-224")

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
    val out = ImageTaskCodegen.payloadPython(ctx)
    out should include("if task in image_only_tasks:")
    out should include("payload = current_image_bytes")
    out should include("use_raw_binary_body = True")
    out should include("raw_binary_headers = image_headers")
  }

  it should "bundle a base64 image and question for VQA / document-QA tasks" in {
    val out = ImageTaskCodegen.payloadPython(ctx)
    out should include(
      """elif task in ("visual-question-answering", "document-question-answering"):"""
    )
    out should include("self._image_input_as_base64(current_image_bytes)")
    out should include(""""question": prompt_value""")
  }

  it should "build the candidate-labels payload for zero-shot image classification (validation is pre-loop)" in {
    val out = ImageTaskCodegen.payloadPython(ctx)
    out should include("""elif task == "zero-shot-image-classification":""")
    out should include("candidate_labels")
    // #7199 Part B: the >= 2 labels check moved to the pre-loop validation in
    // HuggingFaceCodegenBase; the per-task payload build no longer raises.
    out should not include ("raise ValueError")
    out should not include ("if len(labels) < 2:")
  }

  "ImageTaskCodegen.parsePython" should "extract chat-style content for image-text-to-text" in {
    val out = ImageTaskCodegen.parsePython(ctx)
    out should include("choices")
    out should include("message")
    out should include("content")
  }

  it should "normalize image-to-image URL responses through _url_to_data_url" in {
    val out = ImageTaskCodegen.parsePython(ctx)
    out should include("self._url_to_data_url(")
    out should include("data:image/png;base64,")
  }

  // #8870: the image-to-image branch trusted whatever the provider returned, so a
  // malformed 200 produced a data URL nothing can render — a null b64_json became
  // the literal "data:image/png;base64,None".
  it should "require b64_json to be a string before building a data URL" in {
    val out = ImageTaskCodegen.parsePython(ctx)
    out should include("""isinstance(b64, str)""")
    out should not include ("""f"data:image/png;base64,{data[0]['b64_json']}"""")
  }

  it should "fall back to json.dumps(body) for structured tasks" in {
    val out = ImageTaskCodegen.parsePython(ctx)
    out should include("json.dumps(body)")
  }

  it should "read chat-provider responses for visual and document question-answering" in {
    // #7906 Part A: these tasks only understood hf-inference's native
    // {"answer": ...} shape, so a chat-completions reply was written to the
    // result column as a raw JSON envelope. The native shape stays primary.
    val out = ImageTaskCodegen.parsePython(ctx)
    val branch = out
      .split("""elif task """)
      .find(_.startsWith("""in ("visual-question-answering"""))
      .getOrElse(fail("visual/document question-answering branch is missing"))
    branch should include(
      """body["choices"][0].get("message", {}).get("content", json.dumps(body))"""
    )
    branch should include("""body.get("answer"""")
    branch.indexOf("choices") should be < branch.indexOf("""body.get("answer"""")
  }

  it should "read chat-provider responses for zero-shot-image-classification" in {
    // #7906 Part A: the task used to share the image-only branch, which always
    // dumps the body, so a chat reply was never extracted. It now has its own
    // branch, which must precede the image-only one because the generated
    // if/elif chain is first-match-wins.
    val out = ImageTaskCodegen.parsePython(ctx)
    out should include("""elif task == "zero-shot-image-classification":""")
    out should include(
      """elif task in ("image-classification", "object-detection", "image-segmentation"):"""
    )
    out.indexOf("""elif task == "zero-shot-image-classification":""") should be <
      out.indexOf("""elif task in ("image-classification",""")
  }

  it should "degrade instead of raising when a chat response is malformed" in {
    // Review feedback on #7920: parsePython runs per row, so indexing straight
    // into choices[0]["message"]["content"] turns one malformed provider
    // response into an aborted run — an empty "choices" list raises IndexError
    // and a choice without "message"/"content" raises KeyError. Every chat
    // extraction in this file now uses a truthiness guard plus .get chaining,
    // matching how the native shapes already degrade via .get(..., json.dumps(body)).
    val out = ImageTaskCodegen.parsePython(ctx)
    out should not include ("""["message"]["content"]""")
    out.split("""body\.get\("choices"\)""").length - 1 shouldBe 4
    out
      .split(
        """\.get\("message", \{\}\)\.get\("content", json\.dumps\(body\)\)"""
      )
      .length - 1 shouldBe 4
  }

  "ImageTaskCodegen snippets" should "never inline raw CodegenContext string values" in {
    assertNoRawContextValues(ImageTaskCodegen)
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    assertStaticSnippets(
      ImageTaskCodegen,
      "image-classification",
      Some("zero-shot-image-classification")
    )
  }
}
