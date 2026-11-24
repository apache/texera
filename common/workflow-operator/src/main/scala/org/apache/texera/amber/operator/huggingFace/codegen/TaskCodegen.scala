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

/**
  * Inputs the dispatcher passes through to each TaskCodegen.
  *
  * User-provided string fields are typed as [[EncodableString]] so the
  * `pyb"..."` macro in [[HuggingFaceCodegenBase]] emits them as base64-decoded
  * runtime expressions rather than raw Python string literals — required to
  * pass `PythonCodeRawInvalidTextSpec`'s leakage check.
  */
final case class CodegenContext(
    hfApiToken: EncodableString,
    modelId: EncodableString,
    promptColumn: EncodableString,
    resultColumn: EncodableString,
    task: EncodableString,
    systemPrompt: EncodableString,
    safeMaxTokens: Int,
    safeTemp: Double,
    imageInput: EncodableString = "",
    inputImageColumn: EncodableString = "",
    audioInput: EncodableString = "",
    inputAudioColumn: EncodableString = "",
    contextColumn: EncodableString = "",
    candidateLabels: EncodableString = "",
    sentencesColumn: EncodableString = ""
)

/**
  * A bundle of Python snippets that customize generated inference code for
  * one Hugging Face pipeline task family.
  *
  * Concrete implementations are `object`s registered in
  * `HuggingFaceInferenceOpDesc.registeredCodegens`. New task families
  * (image, audio, QA, etc.) land in subsequent PRs by introducing new
  * `*Codegen` objects and adding them to that map.
  *
  * Snippets returned by these methods are Python source spliced into the
  * shared template assembled by [[HuggingFaceCodegenBase.render]]. Snippets must
  * NOT directly inline user-provided strings — reference the per-instance
  * attributes `self.HF_API_TOKEN`, `self.MODEL_ID`, `self.PROMPT_COLUMN`,
  * etc. that the base class initializes from `CodegenContext` via the
  * `pyb` macro's safe encoding. The snippet author is responsible for the
  * correct indentation column (see existing implementations).
  */
trait TaskCodegen {

  /** Canonical Hugging Face pipeline task string used as the primary key for
    * registration, e.g. "text-generation". Codegens that handle multiple
    * task strings (image, audio, …) override [[tasks]] to enumerate all of
    * them — the operator's dispatcher registers an entry per task.
    */
  def task: String

  /** All Hugging Face pipeline task strings handled by this codegen.
    * Defaults to the singleton `Set(task)` for codegens that handle one
    * task; multi-task codegens override this.
    */
  def tasks: Set[String] = Set(task)

  /** Python text that assigns `payload = …` for one row inside
    * `process_table`'s per-row loop. The snippet supplies its own leading
    * `if`/`elif task == "...":` opener and any `else` fallback.
    */
  def payloadPython(ctx: CodegenContext): String

  /** Python text for the body of `_parse_response`'s task switch. The
    * snippet supplies its own leading `if`/`elif task == "...":` opener.
    * The base class wraps the result in the try/except matching the
    * source layout.
    */
  def parsePython(ctx: CodegenContext): String
}

private[codegen] object BinaryResponseFragments {
  val outputUrl: String =
    """                    if "output" in body:
      |                        out = body["output"]
      |                        url = out[0] if isinstance(out, list) else out
      |                        if isinstance(url, str) and url.startswith("http"):
      |                            return self._url_to_data_url(url)""".stripMargin

  // Each PNG handler continues this envelope with its own base64 validation.
  val imageEnvelope: String =
    s"""$outputUrl
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
      |                        if isinstance(data, list) and data and isinstance(data[0], dict):""".stripMargin
}

private[codegen] object TextResponsePython {
  private val chatContent: String =
    """                content = self._chat_message_content(body)
      |                if content is not None:
      |                    return content""".stripMargin

  val chatOrAnswer: String =
    s"""$chatContent
      |                if isinstance(body, dict):
      |                    return body.get("answer", json.dumps(body))
      |                return json.dumps(body)""".stripMargin

  val chatOrBody: String =
    s"""$chatContent
      |                return json.dumps(body)""".stripMargin

  def rankingPayload(promptKey: String, sentencesKey: String): String =
    s"""                sent_val = row[self.SENTENCES_COLUMN]
      |                sent_val = "" if pd.isna(sent_val) else str(sent_val)
      |                sentences_list = [s.strip() for s in sent_val.split(",") if s.strip()]
      |                payload = {
      |                    "inputs": {
      |                        "$promptKey": prompt_value,
      |                        "$sentencesKey": sentences_list,
      |                    }
      |                }""".stripMargin
}
