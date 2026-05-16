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

import dev.langchain4j.agent.tool.ToolSpecification
import dev.langchain4j.model.chat.request.json.JsonObjectSchema

import scala.jdk.CollectionConverters._

object AIAgentFinalAnswerTools {
  final val SubmitTextResult = "submit_text_result"
  final val SubmitStructuredResult = "submit_structured_result"

  def textResult(labels: List[String]): ToolSpecification = {
    val normalizedLabels = normalize(labels)
    val parameters = JsonObjectSchema
      .builder()
      .description("Final text response for the current row")
    if (normalizedLabels.isEmpty) {
      parameters.addStringProperty("response", "Final text response")
    } else {
      parameters.addEnumProperty(
        "response",
        normalizedLabels.asJava,
        "Final classification label. Must exactly match one allowed label."
      )
    }
    ToolSpecification
      .builder()
      .name(SubmitTextResult)
      .description("Submit the final AI Agent text result for the current row")
      .parameters(parameters.required("response").additionalProperties(false).build())
      .build()
  }

  def structuredResult(fields: List[AIAgentStructuredOutputField]): ToolSpecification = {
    val normalizedFields = Option(fields).getOrElse(List.empty).filter { field =>
      field != null && field.columnName != null && field.columnName.trim.nonEmpty
    }
    val parameters = JsonObjectSchema
      .builder()
      .description("Final structured response for the current row")
    normalizedFields.foreach { field =>
      val columnName = field.columnName.trim
      val instructions = Option(field.instructions).getOrElse("").trim
      val description =
        if (instructions.isEmpty) "Extract this value for the row" else instructions
      if (field.normalizedFieldType == AIAgentStructuredFieldType.Classification) {
        val labels = normalize(field.classificationLabels)
        if (labels.nonEmpty) {
          parameters.addEnumProperty(columnName, labels.asJava, description)
        } else {
          parameters.addStringProperty(columnName, description)
        }
      } else {
        parameters.addStringProperty(columnName, description)
      }
    }
    ToolSpecification
      .builder()
      .name(SubmitStructuredResult)
      .description("Submit the final AI Agent structured result for the current row")
      .parameters(
        parameters
          .required(normalizedFields.map(_.columnName.trim): _*)
          .additionalProperties(false)
          .build()
      )
      .build()
  }

  private def normalize(values: List[String]): List[String] =
    Option(values).getOrElse(List.empty).map(_.trim).filter(_.nonEmpty)

}
