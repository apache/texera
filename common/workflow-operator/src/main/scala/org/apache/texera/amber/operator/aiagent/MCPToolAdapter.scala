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
import dev.langchain4j.model.chat.request.json.{
  JsonArraySchema,
  JsonBooleanSchema,
  JsonIntegerSchema,
  JsonNumberSchema,
  JsonObjectSchema,
  JsonSchemaElement,
  JsonStringSchema
}

import scala.jdk.CollectionConverters._

/**
  * Wraps a tool discovered from an MCP server as an [[AIAgentTool]] so it can
  * be dropped into the per-row tool-execution loop alongside built-in tools
  * like `read_url` / `read_pdf`.
  *
  * Tool names are namespaced by server (e.g. `notion__search`) so multiple
  * servers can expose tools with the same local name without colliding.
  *
  * Argument schemas come from the MCP server as JSON Schema and are converted
  * to LangChain4j's [[JsonObjectSchema]] so the model receives a well-typed
  * parameter spec.
  */
class MCPToolAdapter(
    val client: MCPClient,
    val toolInfo: McpToolInfo
) extends AIAgentTool {

  override val name: String = MCPToolAdapter.namespacedName(client.serverName, toolInfo.name)

  override val specification: ToolSpecification = ToolSpecification
    .builder()
    .name(name)
    .description(
      if (toolInfo.description.nonEmpty) toolInfo.description
      else s"MCP tool ${toolInfo.name} on server ${client.serverName}"
    )
    .parameters(MCPToolAdapter.toJsonObjectSchema(toolInfo.inputSchema))
    .build()

  override def execute(argumentsJson: String): String = {
    try AIAgentToolResult.ok(client.callTool(toolInfo.name, argumentsJson))
    catch {
      case t: Throwable =>
        AIAgentToolResult.error(
          s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}"
        )
    }
  }

  override def close(): Unit = client.close()
}

object MCPToolAdapter {
  def namespacedName(serverName: String, toolName: String): String = {
    val safeServer = sanitize(serverName)
    val safeTool = sanitize(toolName)
    if (safeServer.isEmpty) safeTool else s"${safeServer}__${safeTool}"
  }

  private def sanitize(s: String): String =
    Option(s).getOrElse("").trim.replaceAll("[^A-Za-z0-9_]", "_")

  /**
    * Convert an MCP-provided JSON Schema object into a LangChain4j
    * [[JsonObjectSchema]]. Supports top-level primitive properties, nested
    * objects, arrays, and `required[]`. Unknown / unsupported types degrade to
    * string so the model can still send something rather than failing.
    */
  def toJsonObjectSchema(schema: JsonNode): JsonObjectSchema = {
    val builder = JsonObjectSchema.builder()
    if (schema == null || !schema.isObject) {
      return builder.additionalProperties(false).build()
    }
    Option(schema.get("description"))
      .map(_.asText(""))
      .filter(_.nonEmpty)
      .foreach(builder.description)

    val propertiesNode = Option(schema.get("properties")).filter(_.isObject)
    propertiesNode.foreach { props =>
      val fields = props.fields()
      while (fields.hasNext) {
        val entry = fields.next()
        val propName = entry.getKey
        val propSchema = entry.getValue
        builder.addProperty(propName, toJsonSchemaElement(propSchema))
      }
    }

    Option(schema.get("required")).filter(_.isArray).foreach { req =>
      val names = req.elements().asScala.map(_.asText("")).filter(_.nonEmpty).toList
      if (names.nonEmpty) builder.required(names.asJava)
    }

    builder.additionalProperties(false).build()
  }

  private def toJsonSchemaElement(schema: JsonNode): JsonSchemaElement = {
    if (schema == null || !schema.isObject) return JsonStringSchema.builder().build()
    val description = Option(schema.get("description")).map(_.asText("")).getOrElse("")
    val typeStr = Option(schema.get("type")).map(_.asText("")).getOrElse("string")
    typeStr match {
      case "string" =>
        val b = JsonStringSchema.builder()
        if (description.nonEmpty) b.description(description)
        b.build()
      case "integer" =>
        val b = JsonIntegerSchema.builder()
        if (description.nonEmpty) b.description(description)
        b.build()
      case "number" =>
        val b = JsonNumberSchema.builder()
        if (description.nonEmpty) b.description(description)
        b.build()
      case "boolean" =>
        val b = JsonBooleanSchema.builder()
        if (description.nonEmpty) b.description(description)
        b.build()
      case "array" =>
        val b = JsonArraySchema.builder()
        if (description.nonEmpty) b.description(description)
        val itemsNode = Option(schema.get("items")).getOrElse(null)
        if (itemsNode != null) b.items(toJsonSchemaElement(itemsNode))
        else b.items(JsonStringSchema.builder().build())
        b.build()
      case "object" =>
        toJsonObjectSchema(schema)
      case _ =>
        val b = JsonStringSchema.builder()
        if (description.nonEmpty) b.description(description)
        b.build()
    }
  }
}
