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

import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.util.JSONUtils.objectMapper

object AIAgentOutputParser {

  def parseTextResult(response: String, labels: List[String]): String = {
    val root = parseJsonObject(response, "text output")
    val responseNode = root.get("response")
    if (responseNode == null || !responseNode.isTextual) {
      throw new IllegalArgumentException("Text output must contain a string response")
    }
    val value = responseNode.asText()
    val normalizedLabels = normalize(labels)
    if (normalizedLabels.nonEmpty && !normalizedLabels.contains(value)) {
      throw new IllegalArgumentException(
        s"""Text classification label "$value" is not one of: ${normalizedLabels.mkString(", ")}"""
      )
    }
    value
  }

  def parseStructured(response: String, columns: List[String]): Seq[String] = {
    val normalizedColumns = normalize(columns)
    require(
      normalizedColumns.nonEmpty,
      "Structured output mode requires at least one output column"
    )

    val root = parseJsonObject(response, "structured output")
    normalizedColumns.map(column => jsonValueToString(root.get(column)))
  }

  def parseStructuredFields(response: String, fields: List[AIAgentStructuredOutputField]): Seq[String] = {
    val normalizedFields = Option(fields)
      .getOrElse(List.empty)
      .filter(field => field != null && field.columnName != null && field.columnName.trim.nonEmpty)
    require(
      normalizedFields.nonEmpty,
      "Structured output mode requires at least one output column"
    )

    val root = parseJsonObject(response, "structured output")
    normalizedFields.map { field =>
      val value = jsonValueToString(root.get(field.columnName.trim))
      if (field.normalizedFieldType == AIAgentStructuredFieldType.Classification) {
        val labels = field.normalizedClassificationLabels
        if (labels.nonEmpty && !labels.contains(value)) {
          throw new IllegalArgumentException(
            s"""Structured classification field "${field.columnName.trim}" label "$value" is not one of: ${labels
              .mkString(", ")}"""
          )
        }
      }
      value
    }
  }

  def parseClassification(response: String, labels: List[String]): (String, java.lang.Double) = {
    val normalizedLabels = normalize(labels)
    require(normalizedLabels.nonEmpty, "Classification mode requires at least one label")

    val root = parseJsonObject(response, "classification output")
    val labelNode = root.get("label")
    if (labelNode == null || !labelNode.isTextual) {
      throw new IllegalArgumentException("Classification output must contain a string label")
    }

    val label = labelNode.asText()
    if (!normalizedLabels.contains(label)) {
      throw new IllegalArgumentException(
        s"""Classification label "$label" is not one of: ${normalizedLabels.mkString(", ")}"""
      )
    }

    val confidenceNode = root.get("confidence")
    val confidence: java.lang.Double =
      if (confidenceNode == null || confidenceNode.isNull) {
        null
      } else if (confidenceNode.isNumber) {
        confidenceNode.asDouble()
      } else {
        throw new IllegalArgumentException(
          "Classification confidence must be numeric when provided"
        )
      }

    (label, confidence)
  }

  private def parseJsonObject(response: String, outputName: String): JsonNode = {
    val root = objectMapper.readTree(Option(response).getOrElse(""))
    if (!root.isObject) {
      throw new IllegalArgumentException(s"AI Agent $outputName must be a JSON object")
    }
    root
  }

  private def jsonValueToString(node: JsonNode): String =
    if (node == null || node.isNull) {
      ""
    } else if (node.isTextual) {
      node.asText()
    } else {
      node.toString
    }

  private def normalize(values: List[String]): List[String] =
    Option(values).getOrElse(List.empty).map(_.trim).filter(_.nonEmpty)
}
