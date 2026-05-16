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
import dev.langchain4j.model.chat.request.json.JsonObjectSchema
import org.apache.texera.amber.util.JSONUtils.objectMapper

class UrlFetchTool(maxChars: Int) extends AIAgentTool {
  override val name: String = UrlFetchTool.Name

  override val specification: ToolSpecification = ToolSpecification
    .builder()
    .name(name)
    .description(
      "Fetch a web page and return its main content as Markdown. Use when you need to read a URL that the user provided or referenced. Returns clean article text without nav/ads/footers."
    )
    .parameters(
      JsonObjectSchema
        .builder()
        .addStringProperty("url", "Absolute http(s) URL of the page to fetch")
        .required("url")
        .additionalProperties(false)
        .build()
    )
    .build()

  override def execute(argumentsJson: String): String = {
    try {
      val args: JsonNode = objectMapper.readTree(Option(argumentsJson).getOrElse("{}"))
      val url = Option(args.get("url")).map(_.asText("")).getOrElse("")
      if (url.trim.isEmpty) {
        AIAgentToolResult.error("Missing required argument: url")
      } else {
        AIAgentToolResult.ok(UrlFetcher.fetchAsMarkdown(url, maxChars))
      }
    } catch {
      case t: Throwable =>
        AIAgentToolResult.error(
          s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}"
        )
    }
  }
}

object UrlFetchTool {
  final val Name = "read_url"
}
