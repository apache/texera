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

package org.apache.texera.amber.operator.aiagent

import com.github.fge.jsonschema.main.JsonSchemaFactory
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters.IteratorHasAsScala

class AIAgentOpDescSpec extends AnyFlatSpec {

  private val workflowId = WorkflowIdentity(0L)
  private val executionId = ExecutionIdentity(0L)
  private val inputSchema: Schema = Schema()
    .add(new Attribute("comment", AttributeType.STRING))
    .add(new Attribute("rating", AttributeType.INTEGER))

  private def validDesc(): AIAgentOpDesc = {
    val desc = new AIAgentOpDesc
    desc.apiKey = "openrouter-key"
    desc.inputColumn = List("comment")
    desc
  }

  "AIAgentOpDesc metadata" should "generate a numeric temperature default" in {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AIAgentOpDesc])
    val temperature = schema.get("properties").get("temperature")

    assert(temperature.get("default").isDouble)
    assert(temperature.get("default").asDouble() == 0.7)
  }

  it should "expose output modes and hide mode-specific fields" in {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AIAgentOpDesc])
    val properties = schema.get("properties")
    val outputMode = properties.get("outputMode")

    assert(outputMode.get("default").asText() == "text")
    assert(outputMode.get("enum").toString.contains("structured"))
    assert(!outputMode.get("enum").toString.contains("classification"))
    assert(
      properties
        .get("structuredOutputFields")
        .get("hideExpectedValue")
        .asText() == "text"
    )
    assert(
      properties
        .get("structuredOutputFields")
        .get("title")
        .asText() == "Structured Output Fields"
    )
    assert(
      properties
        .get("structuredOutputFields")
        .get("description")
        .asText()
        .contains("Define each output column and what the model should extract")
    )
    assert(
      properties.get("textClassificationLabels").get("hideExpectedValue").asText() == "structured"
    )
    assert(properties.get("outputColumn").get("hideExpectedValue").asText() == "structured")
  }

  it should "show mode-specific output fields immediately after output mode" in {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AIAgentOpDesc])
    val properties = schema.get("properties")
    val propertyNames = properties.fieldNames().asScala.toList
    val aiAgentPropertyNames = propertyNames.filterNot(_ == "dummyPropertyList")

    assert(aiAgentPropertyNames.head == "outputMode")
    assert(
      aiAgentPropertyNames.slice(1, 4) == List(
        "structuredOutputFields",
        "textClassificationLabels",
        "classificationLabels"
      )
    )
    assert(properties.get("inputColumn").get("title").asText() == "Columns Sent to AI")
    assert(properties.get("inputColumn").get("autofill").asText() == "attributeNameList")
    assert(!propertyNames.contains("userPromptTemplate"))
  }

  it should "deserialize a legacy single input column as a one-element list" in {
    val desc = objectMapper.readValue(
      """{"operatorType":"AIAgent","inputColumn":"text","apiKey":"openrouter-key"}""",
      classOf[AIAgentOpDesc]
    )

    assert(desc.inputColumn == List("text"))
  }

  it should "deserialize structured output fields with column instructions" in {
    val desc = objectMapper.readValue(
      """{
        |  "operatorType": "AIAgent",
        |  "inputColumn": "text",
        |  "apiKey": "openrouter-key",
        |  "outputMode": "structured",
        |  "structuredOutputFields": [
        |    {
        |      "fieldType": "classification",
        |      "columnName": "sentiment",
        |      "instructions": "positive, neutral, or negative",
        |      "classificationLabels": ["positive", "neutral", "negative"]
        |    }
        |  ]
        |}""".stripMargin,
      classOf[AIAgentOpDesc]
    )

    assert(desc.normalizedStructuredOutputColumns == List("sentiment"))
    assert(
      desc.normalizedStructuredOutputFields.head.normalizedFieldType == AIAgentStructuredFieldType.Classification
    )
    assert(
      desc.normalizedStructuredOutputFields.head.instructions == "positive, neutral, or negative"
    )
    assert(
      desc.normalizedStructuredOutputFields.head.normalizedClassificationLabels == List(
        "positive",
        "neutral",
        "negative"
      )
    )
  }

  it should "validate structured mode without hidden text output column" in {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AIAgentOpDesc])
    val properties =
      """
        |{
        |  "outputMode": "structured",
        |  "structuredOutputFields": [
        |    {
        |      "columnName": "sentiment",
        |      "instructions": "positive, neutral, or negative"
        |    }
        |  ],
        |  "systemPrompt": "",
        |  "inputColumn": ["comment"],
        |  "apiKey": "openrouter-key",
        |  "model": "openai/gpt-4o-mini",
        |  "temperature": 0.7,
        |  "timeoutSeconds": 60,
        |  "cacheEnabled": true,
        |  "emitCostColumn": true,
        |  "emitErrorColumn": true
        |}
        |""".stripMargin
    val report = JsonSchemaFactory
      .byDefault()
      .getJsonSchema(schema)
      .validate(objectMapper.readTree(properties))

    assert(report.isSuccess)
  }

  it should "propagate input schema while output column is blank" in {
    val desc = validDesc()
    desc.outputColumn = ""

    val op = desc.getPhysicalOp(workflowId, executionId)
    val inputPortId = op.inputPorts.keys.head
    val outputPortId = op.outputPorts.keys.head
    val updated = op.propagateSchema(Some(inputPortId -> inputSchema))

    val outputSchema = inputSchema
      .add(new Attribute("_cost_usd", AttributeType.DOUBLE))
      .add(new Attribute("_error", AttributeType.STRING))

    assert(updated.outputPorts(outputPortId)._3.toOption.contains(outputSchema))
  }

  it should "append the configured output column during schema propagation" in {
    val desc = validDesc()
    desc.outputColumn = "ai_response"

    val op = desc.getPhysicalOp(workflowId, executionId)
    val inputPortId = op.inputPorts.keys.head
    val outputPortId = op.outputPorts.keys.head
    val updated = op.propagateSchema(Some(inputPortId -> inputSchema))
    val outputSchema = inputSchema
      .add(new Attribute("ai_response", AttributeType.STRING))
      .add(new Attribute("_cost_usd", AttributeType.DOUBLE))
      .add(new Attribute("_error", AttributeType.STRING))

    assert(updated.outputPorts(outputPortId)._3.toOption.contains(outputSchema))
  }

  it should "append structured output columns during schema propagation" in {
    val desc = validDesc()
    desc.outputMode = AIAgentOutputMode.Structured
    val sentiment = new AIAgentStructuredOutputField
    sentiment.columnName = "sentiment"
    sentiment.instructions = "positive, neutral, or negative"
    val reason = new AIAgentStructuredOutputField
    reason.columnName = "reason"
    reason.instructions = "short explanation for the sentiment"
    val blank = new AIAgentStructuredOutputField
    blank.columnName = " "
    desc.structuredOutputFields = List(sentiment, reason, blank)

    val op = desc.getPhysicalOp(workflowId, executionId)
    val inputPortId = op.inputPorts.keys.head
    val outputPortId = op.outputPorts.keys.head
    val updated = op.propagateSchema(Some(inputPortId -> inputSchema))
    val outputSchema = inputSchema
      .add(new Attribute("sentiment", AttributeType.STRING))
      .add(new Attribute("reason", AttributeType.STRING))
      .add(new Attribute("_cost_usd", AttributeType.DOUBLE))
      .add(new Attribute("_error", AttributeType.STRING))

    assert(updated.outputPorts(outputPortId)._3.toOption.contains(outputSchema))
  }

  it should "append classification label and confidence columns during schema propagation" in {
    val desc = validDesc()
    desc.outputMode = AIAgentOutputMode.Classification
    desc.outputColumn = "category"

    val op = desc.getPhysicalOp(workflowId, executionId)
    val inputPortId = op.inputPorts.keys.head
    val outputPortId = op.outputPorts.keys.head
    val updated = op.propagateSchema(Some(inputPortId -> inputSchema))
    val outputSchema = inputSchema
      .add(new Attribute("category", AttributeType.STRING))
      .add(new Attribute("_cost_usd", AttributeType.DOUBLE))
      .add(new Attribute("_error", AttributeType.STRING))

    assert(updated.outputPorts(outputPortId)._3.toOption.contains(outputSchema))
  }

  it should "skip cost and error columns when structured fields already use those names" in {
    val desc = validDesc()
    desc.outputMode = AIAgentOutputMode.Structured
    val cost = new AIAgentStructuredOutputField
    cost.columnName = "_cost_usd"
    val error = new AIAgentStructuredOutputField
    error.columnName = "_error"
    desc.structuredOutputFields = List(cost, error)

    val op = desc.getPhysicalOp(workflowId, executionId)
    val inputPortId = op.inputPorts.keys.head
    val outputPortId = op.outputPorts.keys.head
    val updated = op.propagateSchema(Some(inputPortId -> inputSchema))
    val outputSchema = inputSchema
      .add(new Attribute("_cost_usd", AttributeType.STRING))
      .add(new Attribute("_error", AttributeType.STRING))

    assert(updated.outputPorts(outputPortId)._3.toOption.contains(outputSchema))
  }
}
