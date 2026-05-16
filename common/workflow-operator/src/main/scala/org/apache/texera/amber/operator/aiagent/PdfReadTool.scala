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
import dev.langchain4j.agent.tool.ToolSpecification
import dev.langchain4j.model.chat.request.json.{JsonIntegerSchema, JsonObjectSchema}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class PdfReadTool(maxChars: Int) extends AIAgentTool {
  override val name: String = PdfReadTool.Name

  override val specification: ToolSpecification = ToolSpecification
    .builder()
    .name(name)
    .description(
      "Read text from a PDF document at a public http(s) URL. Optionally restrict to a page range (1-based, inclusive). Returns extracted text."
    )
    .parameters(
      JsonObjectSchema
        .builder()
        .addStringProperty("source", "Public http(s) URL of the PDF")
        .addProperty(
          "startPage",
          JsonIntegerSchema
            .builder()
            .description("First page to read (1-based, inclusive). Omit for page 1.")
            .build()
        )
        .addProperty(
          "endPage",
          JsonIntegerSchema
            .builder()
            .description("Last page to read (1-based, inclusive). Omit for the last page.")
            .build()
        )
        .required("source")
        .additionalProperties(false)
        .build()
    )
    .build()

  override def execute(argumentsJson: String): String = {
    try {
      val args: JsonNode = objectMapper.readTree(Option(argumentsJson).getOrElse("{}"))
      val source = Option(args.get("source")).map(_.asText("")).getOrElse("")
      if (source.trim.isEmpty) {
        AIAgentToolResult.error("Missing required argument: source")
      } else {
        val startPage = Option(args.get("startPage")).filter(_.isNumber).map(_.asInt())
        val endPage = Option(args.get("endPage")).filter(_.isNumber).map(_.asInt())
        AIAgentToolResult.ok(PdfReader.readText(source, startPage, endPage, maxChars))
      }
    } catch {
      case t: Throwable =>
        AIAgentToolResult.error(
          s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}"
        )
    }
  }
}

object PdfReadTool {
  final val Name = "read_pdf"
}
