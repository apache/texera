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

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.huggingFace.codegen.{CodegenContext, TextGenCodegen}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HuggingFaceInferenceOpDescSpec extends AnyFlatSpec with Matchers {

  private def makeDesc(
      token: EncodableString = "token",
      modelId: EncodableString = "Qwen/Qwen2.5-72B-Instruct",
      promptColumn: EncodableString = "prompt",
      task: EncodableString = "text-generation",
      systemPrompt: EncodableString = "You are a helpful assistant.",
      maxNewTokens: Int = 256,
      temperature: Double = 0.7,
      resultColumn: EncodableString = "hf_response"
  ): HuggingFaceInferenceOpDesc = {
    val desc = new HuggingFaceInferenceOpDesc()
    desc.hfApiToken = token
    desc.modelId = modelId
    desc.promptColumn = promptColumn
    desc.task = task
    desc.systemPrompt = systemPrompt
    desc.maxNewTokens = maxNewTokens
    desc.temperature = temperature
    desc.resultColumn = resultColumn
    desc
  }

  "HuggingFaceInferenceOpDesc.operatorInfo" should
    "advertise the user-friendly name, HuggingFace group, and one input/output port" in {
    val info = (new HuggingFaceInferenceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Hugging Face"
    info.operatorGroupName shouldBe OperatorGroupConstants.HUGGINGFACE_GROUP
    info.inputPorts.size shouldBe 1
    info.outputPorts.size shouldBe 1
  }

  "generatePythonCode" should
    "fall back to the text-gen codegen on an unrecognized task (HF reports the real error at runtime)" in {
    // generatePythonCode must be total — never throw on arbitrary @JsonProperty
    // values — per the PythonCodeRawInvalidTextSpec contract. An unknown task
    // routes through TextGenCodegen, whose payload `if/else` hits the generic
    // `{"inputs": prompt_value}` branch at runtime.
    val code = makeDesc(task = "not-a-real-task").generatePythonCode()
    code should include("""payload = {"inputs": prompt_value}""")
  }

  it should "emit a ProcessTableOperator that initializes config in open()" in {
    val code = makeDesc().generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator):")
    code should include("def open(self):")
    // User-input strings are decoded at runtime, not embedded as literals.
    code should include("self.HF_API_TOKEN = self.decode_python_template(")
    code should include("self.MODEL_ID = self.decode_python_template(")
    code should include("self.PROMPT_COLUMN = self.decode_python_template(")
    code should include("self.TASK = self.decode_python_template(")
    code should include("self.SYSTEM_PROMPT = self.decode_python_template(")
  }

  it should "wire the text-gen payload and response parse correctly" in {
    val code = makeDesc().generatePythonCode()
    // Payload — chat-completions shape against the configured model + system prompt.
    code should include("self.MODEL_ID")
    code should include("self.SYSTEM_PROMPT")
    code should include("self.MAX_NEW_TOKENS")
    code should include("self.TEMPERATURE")
    // Parse — text-gen pulls choices[0].message.content out of the response.
    code should include("""body["choices"][0]["message"]["content"]""")
  }

  it should
    "emit a runtime check that rejects malformed MODEL_ID values before any HF URL is built" in {
    val code = makeDesc().generatePythonCode()
    // Pattern that fences MODEL_ID to org/model-name (allowing org/model-name/revision).
    code should include("_HF_MODEL_ID_PATTERN = re.compile(")
    // Runtime fail-fast inside process_table — happens before _resolve_providers
    // composes the URL, so a malformed value never escapes into a request.
    code should include("if not _HF_MODEL_ID_PATTERN.match(")
    code should include("raise ValueError(")
    code should include("Invalid Hugging Face model ID")
  }

  it should "not leak raw user-input strings into the generated Python source" in {
    // Sentinel value chosen to be distinctive and non-overlapping with anything
    // else in the template. If our encoding regressed back to raw literals
    // (e.g. `MODEL_ID = "MARKER_zXyq42"`), this assertion would fail.
    val marker = "MARKER_zXyq42"
    val code =
      makeDesc(modelId = marker, promptColumn = marker, token = marker).generatePythonCode()
    code should not include marker
  }

  it should "clamp maxNewTokens into the 1-4096 range" in {
    makeDesc(maxNewTokens = -5).generatePythonCode() should include(
      "self.MAX_NEW_TOKENS = 1"
    )
    makeDesc(maxNewTokens = 99999).generatePythonCode() should include(
      "self.MAX_NEW_TOKENS = 4096"
    )
  }

  it should "clamp temperature into the 0.0-2.0 range" in {
    makeDesc(temperature = -1.0).generatePythonCode() should include(
      "self.TEMPERATURE = 0.0"
    )
    makeDesc(temperature = 5.0).generatePythonCode() should include(
      "self.TEMPERATURE = 2.0"
    )
  }

  it should "tolerate null @JsonProperty values and fall back to safe defaults" in {
    // Every user-input field can land as null when the JSON deserializer is
    // handed a workflow that omits the field. generatePythonCode must not
    // throw on any combination — and the generated Python must still parse.
    val desc = new HuggingFaceInferenceOpDesc()
    desc.hfApiToken = null
    desc.modelId = null
    desc.promptColumn = null
    desc.systemPrompt = null
    desc.resultColumn = null
    desc.task = null
    desc.maxNewTokens = null
    desc.temperature = null
    val code = desc.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator):")
    code should include("def open(self):")
    // System-prompt default is the empty-string sentinel (no fallback string
    // injected) but the operator class still initializes the constant.
    code should include("self.SYSTEM_PROMPT = ")
    // maxNewTokens null path defaults to 256.
    code should include("self.MAX_NEW_TOKENS = 256")
    // temperature null path defaults to 0.7.
    code should include("self.TEMPERATURE = 0.7")
  }

  "TextGenCodegen" should "advertise text-generation as its canonical task" in {
    TextGenCodegen.task shouldBe "text-generation"
  }

  it should
    "emit payload and parse snippets that don't depend on the CodegenContext" in {
    // For text-generation, the codegen's only inputs to Python are static
    // strings referencing self.* attributes — exercising both methods
    // confirms they don't accidentally consume ctx fields (a future
    // refactor regression would surface here).
    val ctx = CodegenContext(
      hfApiToken = "irrelevant",
      modelId = "irrelevant",
      promptColumn = "irrelevant",
      resultColumn = "irrelevant",
      task = "irrelevant",
      systemPrompt = "irrelevant",
      safeMaxTokens = 0,
      safeTemp = 0.0
    )
    TextGenCodegen.payloadPython(ctx) should include("self.MODEL_ID")
    TextGenCodegen.parsePython(ctx) should include("""body["choices"][0]["message"]["content"]""")
  }

  "getOutputSchemas" should "add the result column as a STRING to the inherited schema" in {
    val desc = makeDesc(resultColumn = "answer")
    val inputSchema = Schema().add("prompt", AttributeType.STRING)
    val out = desc.getOutputSchemas(Map(PortIdentity(0) -> inputSchema))
    val outSchema = out(desc.operatorInfo.outputPorts.head.id)
    outSchema.getAttributeNames.contains("prompt") shouldBe true
    outSchema.getAttributeNames.contains("answer") shouldBe true
    outSchema.getAttribute("answer").getType shouldBe AttributeType.STRING
  }

  it should "fall back to the default 'hf_response' name when resultColumn is empty" in {
    val desc = makeDesc(resultColumn = "")
    val inputSchema = Schema().add("prompt", AttributeType.STRING)
    val out = desc.getOutputSchemas(Map(PortIdentity(0) -> inputSchema))
    val outSchema = out(desc.operatorInfo.outputPorts.head.id)
    outSchema.getAttributeNames.contains("hf_response") shouldBe true
  }
}
