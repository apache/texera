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

package org.apache.texera.amber.operator.huggingFace

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.huggingFace.codegen.{
  AudioTaskCodegen,
  CodegenContext,
  ImageTaskCodegen,
  MediaGenCodegen,
  PythonCodegenBase,
  TaskCodegen,
  TextGenCodegen
}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString

/**
  * Generic Hugging Face inference operator.
  *
  * This is the first slice of a feature that will eventually cover ~20 HF
  * pipeline tasks. PR 2 ships text-generation only; image, audio,
  * media-generation, and QA task families land in subsequent PRs as new
  * `TaskCodegen` implementations registered in `registeredCodegens`.
  *
  * The Python script that runs at execution time is assembled by
  * `PythonCodegenBase.render(ctx, codegen)`, which composes the shared
  * provider-fallback / request-loop infrastructure with the per-task
  * payload + parse snippets supplied by the selected `TaskCodegen`.
  *
  * User-provided string fields are typed as [[EncodableString]] so the
  * `pyb"..."` macro inside `PythonCodegenBase` emits them as
  * base64-decoded expressions at runtime instead of raw Python literals —
  * this is what allows the operator to satisfy
  * `PythonCodeRawInvalidTextSpec`'s contract that arbitrary `@JsonProperty`
  * values must not leak into generated source.
  */
class HuggingFaceInferenceOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "hfApiToken", required = true)
  @JsonSchemaTitle("HF API Token")
  @JsonPropertyDescription(
    "Your Hugging Face API token (from https://huggingface.co/settings/tokens)"
  )
  var hfApiToken: EncodableString = ""

  @JsonProperty(value = "task", required = true, defaultValue = "text-generation")
  @JsonSchemaTitle("Task")
  @JsonPropertyDescription("The Hugging Face pipeline task type")
  var task: EncodableString = "text-generation"

  @JsonProperty(
    value = "modelId",
    required = true,
    defaultValue = "Qwen/Qwen2.5-72B-Instruct"
  )
  @JsonSchemaTitle("Model")
  @JsonPropertyDescription("Select a Hugging Face model")
  var modelId: EncodableString = "Qwen/Qwen2.5-72B-Instruct"

  @JsonProperty(value = "promptColumn", required = true)
  @JsonSchemaTitle("Prompt Column")
  @JsonPropertyDescription("Column in the input table to use as the user prompt")
  @AutofillAttributeName
  var promptColumn: EncodableString = ""

  @JsonProperty(value = "imageInput", required = false)
  @JsonSchemaTitle("Image Upload")
  @JsonPropertyDescription("Upload an image for Hugging Face image tasks")
  var imageInput: EncodableString = ""

  @JsonProperty(value = "inputImageColumn", required = false)
  @JsonSchemaTitle("Input Image Column")
  @JsonPropertyDescription("Column containing image data from the input table")
  @AutofillAttributeName
  var inputImageColumn: EncodableString = ""

  @JsonProperty(value = "audioInput", required = false)
  @JsonSchemaTitle("Audio Upload")
  @JsonPropertyDescription("Upload audio for Hugging Face audio tasks")
  var audioInput: EncodableString = ""

  @JsonProperty(value = "inputAudioColumn", required = false)
  @JsonSchemaTitle("Input Audio Column")
  @JsonPropertyDescription("Column containing audio data from the input table")
  @AutofillAttributeName
  var inputAudioColumn: EncodableString = ""

  @JsonProperty(
    value = "systemPrompt",
    required = false,
    defaultValue = "You are a helpful assistant."
  )
  @JsonSchemaTitle("System Prompt")
  @JsonPropertyDescription("Optional system message to set model behavior")
  var systemPrompt: EncodableString = "You are a helpful assistant."

  @JsonProperty(value = "maxNewTokens", required = false, defaultValue = "256")
  @JsonSchemaTitle("Max New Tokens")
  @JsonPropertyDescription("Maximum number of tokens to generate (1-4096)")
  var maxNewTokens: java.lang.Integer = 256

  @JsonProperty(value = "temperature", required = false)
  @JsonSchemaTitle("Temperature")
  @JsonPropertyDescription("Sampling temperature (0.0 = deterministic, up to 2.0)")
  var temperature: java.lang.Double = 0.7

  @JsonProperty(
    value = "resultColumn",
    required = false,
    defaultValue = "hf_response"
  )
  @JsonSchemaTitle("Result Column Name")
  @JsonPropertyDescription("Name of the new column added to the output table")
  var resultColumn: EncodableString = "hf_response"

  /**
    * Per-task code generators. New entries are added as task families land
    * in subsequent PRs (e.g. ImageTaskCodegen, AudioTaskCodegen, etc.).
    *
    * An unrecognized task string falls back to [[TextGenCodegen]]; the
    * generated Python's `else` branch then produces a generic `{"inputs":
    * prompt_value}` payload and the HF endpoint surfaces the real error at
    * runtime. This matches the original monolithic operator's behavior and
    * keeps `generatePythonCode` total (it never throws on arbitrary input,
    * which is required by `PythonCodeRawInvalidTextSpec`).
    */
  private val registeredCodegens: Map[String, TaskCodegen] = {
    val byTask = scala.collection.mutable.Map.empty[String, TaskCodegen]
    byTask += (TextGenCodegen.task -> TextGenCodegen)
    ImageTaskCodegen.tasks.foreach(t => byTask += (t -> ImageTaskCodegen))
    AudioTaskCodegen.tasks.foreach(t => byTask += (t -> AudioTaskCodegen))
    MediaGenCodegen.tasks.foreach(t => byTask += (t -> MediaGenCodegen))
    byTask.toMap
  }

  private def codegenForTask(t: String): TaskCodegen =
    registeredCodegens.getOrElse(t, TextGenCodegen)

  /**
    * The output column name to use in generated Python and in the output
    * schema. Falls back to the `"hf_response"` sentinel when the user
    * leaves the field null or blank.
    *
    * Shared between [[generatePythonCode]] and [[getOutputSchemas]] so the
    * two never drift apart (a divergence would cause the Python operator
    * to write to a column the schema didn't declare). Returns
    * [[EncodableString]] rather than `String` so the value flows into the
    * `pyb` template with the encoding annotation intact.
    */
  private def resolvedResultColumn: EncodableString =
    if (resultColumn == null || resultColumn.trim.isEmpty) "hf_response"
    else resultColumn

  override def generatePythonCode(): String = {
    val safeTask: EncodableString =
      if (task == null || task.trim.isEmpty) "text-generation" else task
    val safeModelId: EncodableString =
      if (modelId == null) "" else modelId.trim
    val safePromptCol: EncodableString =
      if (promptColumn == null) "" else promptColumn
    val safeResultCol: EncodableString = resolvedResultColumn
    val safeSystemPrompt: EncodableString =
      if (systemPrompt == null) "" else systemPrompt
    val safeToken: EncodableString =
      if (hfApiToken == null) "" else hfApiToken

    val safeMaxTokens =
      math.max(1, math.min(if (maxNewTokens != null) maxNewTokens.intValue else 256, 4096))
    val safeTemp =
      math.max(0.0, math.min(if (temperature != null) temperature.doubleValue else 0.7, 2.0))

    val safeImageInput: EncodableString =
      if (imageInput == null) "" else imageInput
    val safeInputImageColumn: EncodableString =
      if (inputImageColumn == null) "" else inputImageColumn
    val safeAudioInput: EncodableString =
      if (audioInput == null) "" else audioInput
    val safeInputAudioColumn: EncodableString =
      if (inputAudioColumn == null) "" else inputAudioColumn

    val ctx = CodegenContext(
      hfApiToken = safeToken,
      modelId = safeModelId,
      promptColumn = safePromptCol,
      resultColumn = safeResultCol,
      task = safeTask,
      systemPrompt = safeSystemPrompt,
      safeMaxTokens = safeMaxTokens,
      safeTemp = safeTemp,
      imageInput = safeImageInput,
      inputImageColumn = safeInputImageColumn,
      audioInput = safeAudioInput,
      inputAudioColumn = safeInputAudioColumn
    )

    PythonCodegenBase.render(ctx, codegenForTask(safeTask))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hugging Face",
      "Call a Hugging Face model via the Inference API",
      OperatorGroupConstants.HUGGINGFACE_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] =
    Map(
      operatorInfo.outputPorts.head.id -> inputSchemas.values.head
        .add(resolvedResultColumn, AttributeType.STRING)
    )
}
