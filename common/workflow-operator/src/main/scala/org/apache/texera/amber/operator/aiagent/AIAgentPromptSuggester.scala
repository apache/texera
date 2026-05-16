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
 */

package org.apache.texera.amber.operator.aiagent

import com.fasterxml.jackson.databind.JsonNode
import dev.langchain4j.agent.tool.ToolSpecification
import dev.langchain4j.model.chat.request.json.JsonObjectSchema
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.jdk.CollectionConverters._

case class SuggestedPromptConfig(
    systemPrompt: String,
    outputMode: String,
    outputColumn: String,
    structuredOutputFields: List[AIAgentStructuredOutputField]
)

// Backend RPC for "suggest a prompt": given the input column schema and a one-line goal,
// asks the LLM to draft a system prompt + output schema. Not wired to any HTTP endpoint —
// the frontend should expose this via a new REST route (out of scope for this hack).
object AIAgentPromptSuggester {

  private val SystemPrompt =
    """You are a prompt designer for a per-row data-pipeline AI Agent.
      |Given a one-line user goal and the input row schema, produce:
      |  - a concise systemPrompt instructing the model what to do for each row,
      |  - an outputMode of either "text" or "structured",
      |  - if outputMode is "text", an outputColumn name,
      |  - if outputMode is "structured", one or more structured output fields
      |    (each with columnName, fieldType "text" or "classification",
      |    instructions, and optional classificationLabels).
      |Call submit_prompt_suggestion exactly once with the final JSON suggestion.""".stripMargin

  def suggest(
      apiKey: String,
      model: String,
      goal: String,
      inputColumns: List[(String, String)],
      timeoutSeconds: Int = 60,
      client: ChatCompletionClient = new OpenRouterClient
  ): SuggestedPromptConfig = {
    val columnsBlock =
      if (inputColumns.isEmpty) "(no input columns)"
      else inputColumns.map { case (n, t) => s"- $n: $t" }.mkString("\n")
    val userPrompt =
      s"""Goal: ${Option(goal).getOrElse("").trim}
         |
         |Input columns:
         |$columnsBlock""".stripMargin

    val result = client.completeWithRequiredTool(
      apiKey,
      model,
      SystemPrompt,
      userPrompt,
      0.2,
      timeoutSeconds,
      suggestionTool
    )
    parse(result.text)
  }

  private def suggestionTool: ToolSpecification = {
    val params = JsonObjectSchema
      .builder()
      .description("Final prompt suggestion")
      .addStringProperty("systemPrompt", "System prompt to use for each row")
      .addEnumProperty(
        "outputMode",
        List("text", "structured").asJava,
        "Either text (one column) or structured (one column per extracted field)"
      )
      .addStringProperty("outputColumn", "Output column name when outputMode is text")
      .addStringProperty(
        "structuredOutputFieldsJson",
        "JSON array of {columnName, fieldType, instructions, classificationLabels} objects when outputMode is structured. Use [] for text mode."
      )
    ToolSpecification
      .builder()
      .name("submit_prompt_suggestion")
      .description("Submit a drafted prompt configuration")
      .parameters(
        params
          .required("systemPrompt", "outputMode", "outputColumn", "structuredOutputFieldsJson")
          .additionalProperties(false)
          .build()
      )
      .build()
  }

  private def parse(json: String): SuggestedPromptConfig = {
    val root: JsonNode = objectMapper.readTree(Option(json).getOrElse("{}"))
    val systemPrompt = textOrEmpty(root.get("systemPrompt"))
    val outputMode = {
      val raw = textOrEmpty(root.get("outputMode")).trim.toLowerCase
      if (raw == "structured") "structured" else "text"
    }
    val outputColumn = {
      val raw = textOrEmpty(root.get("outputColumn")).trim
      if (raw.nonEmpty) raw else "ai_agent_response"
    }
    val fieldsJson = textOrEmpty(root.get("structuredOutputFieldsJson"))
    val fields: List[AIAgentStructuredOutputField] =
      if (fieldsJson.trim.isEmpty) List.empty
      else {
        val arr = objectMapper.readTree(fieldsJson)
        if (!arr.isArray) List.empty
        else
          arr.elements().asScala.toList.flatMap { node =>
            val name = textOrEmpty(node.get("columnName")).trim
            if (name.isEmpty) None
            else {
              val f = new AIAgentStructuredOutputField
              f.columnName = name
              f.fieldType = {
                val ft = textOrEmpty(node.get("fieldType")).trim.toLowerCase
                if (ft == "classification") "classification" else "text"
              }
              f.instructions = textOrEmpty(node.get("instructions"))
              val labelsNode = node.get("classificationLabels")
              f.classificationLabels =
                if (labelsNode == null || !labelsNode.isArray) List.empty
                else labelsNode.elements().asScala.toList.map(_.asText("")).filter(_.nonEmpty)
              Some(f)
            }
          }
      }
    SuggestedPromptConfig(systemPrompt, outputMode, outputColumn, fields)
  }

  private def textOrEmpty(node: JsonNode): String =
    if (node == null || node.isNull) ""
    else if (node.isTextual) node.asText()
    else node.toString
}
