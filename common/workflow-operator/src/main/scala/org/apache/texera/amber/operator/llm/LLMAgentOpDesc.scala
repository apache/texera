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

package org.apache.texera.amber.operator.llm

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class LLMAgentOpDesc extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Provider")
  @JsonPropertyDescription("LLM provider to call")
  var provider: LLMProvider = LLMProvider.ANTHROPIC

  @JsonProperty
  @JsonSchemaTitle("API Key")
  @JsonPropertyDescription(
    "API key for the chosen provider. Leave blank to fall back to the ANTHROPIC_API_KEY or OPENAI_API_KEY environment variable on the worker JVM."
  )
  var apiKey: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Model")
  @JsonPropertyDescription(
    "Model name. Anthropic examples: claude-haiku-4-5-20251001, claude-sonnet-4-6, claude-opus-4-7. OpenAI examples: gpt-4o-mini, gpt-4o."
  )
  var model: String = "claude-haiku-4-5-20251001"

  @JsonProperty(required = true)
  @JsonSchemaTitle("System Prompt")
  @JsonPropertyDescription(
    "System / instructions message. Supports ${fieldName} placeholders that will be filled from the input tuple."
  )
  var systemPrompt: String =
    "You are a concise analyst. Respond with a short, factual summary."

  @JsonProperty(required = true)
  @JsonSchemaTitle("User Prompt Template")
  @JsonPropertyDescription(
    "User message sent to the model. Supports ${fieldName} placeholders that will be filled from the input tuple."
  )
  var userPromptTemplate: String = "${response_body}"

  @JsonProperty(required = true)
  @JsonSchemaTitle("Max tokens")
  @JsonPropertyDescription("Maximum tokens in the model's reply")
  var maxTokens: Int = 1024

  @JsonProperty(required = true)
  @JsonSchemaTitle("Temperature")
  @JsonPropertyDescription("Sampling temperature, typically 0.0 to 1.0")
  var temperature: Double = 1.0

  @JsonProperty(required = true)
  @JsonSchemaTitle("Output column name")
  @JsonPropertyDescription("Name of the new column that will hold the model's reply text")
  var outputColumnName: String = "llm_response"

  @JsonProperty(required = true)
  @JsonSchemaTitle("Timeout (seconds)")
  @JsonPropertyDescription("Per-request timeout in seconds")
  var timeoutSeconds: Int = 60

  @JsonProperty(required = true)
  @JsonSchemaTitle("Fail on error")
  @JsonPropertyDescription(
    "If true, a non-2xx response or transport error fails the workflow. If false (default), the error is recorded in `llm_error` and processing continues."
  )
  var failOnError: Boolean = false

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.llm.LLMAgentOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        val inputSchema = inputSchemas.values.head
        val outputColName =
          if (outputColumnName == null || outputColumnName.trim.isEmpty) "llm_response"
          else outputColumnName
        val outputSchema = inputSchema
          .add(outputColName, AttributeType.STRING)
          .add("llm_error", AttributeType.STRING)
        Map(operatorInfo.outputPorts.head.id -> outputSchema)
      }))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "LLM Agent",
      operatorDescription =
        "Call an LLM (Anthropic or OpenAI) on each input tuple using a templated prompt; append the reply as a new column.",
      operatorGroupName = OperatorGroupConstants.API_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
