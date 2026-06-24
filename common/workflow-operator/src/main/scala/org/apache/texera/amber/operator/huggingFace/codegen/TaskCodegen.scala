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
  * `pyb"..."` macro in [[PythonCodegenBase]] emits them as base64-decoded
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
  * shared template assembled by [[PythonCodegenBase.render]]. Snippets must
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
