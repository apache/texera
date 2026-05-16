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
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

case class McpToolInfo(name: String, description: String, inputSchema: JsonNode)

/**
  * Minimal Model Context Protocol client over Streamable HTTP.
  *
  * Supports the three calls we need to register and execute MCP-discovered
  * tools inside the AI Agent loop: `initialize`, `tools/list`, `tools/call`.
  * Bearer auth is sent as `Authorization: Bearer <token>` when a token is
  * configured. Session continuity is maintained via the `Mcp-Session-Id`
  * response header per the MCP spec.
  *
  * Response bodies may arrive as plain `application/json` or as a single SSE
  * event (`text/event-stream`); both shapes are parsed transparently.
  */
class MCPClient(
    val serverName: String,
    val url: String,
    val bearerToken: Option[String] = None,
    val timeoutSeconds: Int = 30,
    val protocolVersion: String = "2025-06-18"
) extends AutoCloseable
    with LazyLogging {

  private val httpClient: HttpClient = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofSeconds(timeoutSeconds.toLong))
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build()

  private val nextId = new AtomicLong(0)
  @volatile private var sessionId: Option[String] = None
  @volatile private var initialized: Boolean = false

  def initialize(): Unit = {
    val params = objectMapper.createObjectNode()
    params.put("protocolVersion", protocolVersion)
    params.set[ObjectNode]("capabilities", objectMapper.createObjectNode())
    val clientInfo = objectMapper.createObjectNode()
    clientInfo.put("name", "texera-aiagent")
    clientInfo.put("version", "1.0.0")
    params.set[ObjectNode]("clientInfo", clientInfo)

    sendRequest("initialize", Some(params))
    sendNotification("notifications/initialized")
    initialized = true
    logger.info(
      s"[MCP] initialized server=$serverName url=$url session=${sessionId.getOrElse("-")}"
    )
  }

  def listTools(): List[McpToolInfo] = {
    ensureInitialized()
    val result = sendRequest("tools/list", None)
    val toolsNode = Option(result.get("tools")).filter(_.isArray)
    toolsNode match {
      case Some(arr) =>
        val builder = List.newBuilder[McpToolInfo]
        val iter = arr.elements()
        while (iter.hasNext) {
          val t = iter.next()
          val name = Option(t.get("name")).map(_.asText("")).getOrElse("")
          val description = Option(t.get("description")).map(_.asText("")).getOrElse("")
          val schema = Option(t.get("inputSchema")).getOrElse(objectMapper.createObjectNode())
          if (name.nonEmpty) {
            builder += McpToolInfo(name, description, schema)
          }
        }
        builder.result()
      case None => List.empty
    }
  }

	  def callTool(toolName: String, argumentsJson: String): String = {
    ensureInitialized()
    val params = objectMapper.createObjectNode()
    params.put("name", toolName)
    val args =
      try objectMapper.readTree(Option(argumentsJson).filter(_.nonEmpty).getOrElse("{}"))
      catch { case _: Throwable => objectMapper.createObjectNode() }
    params.set[ObjectNode]("arguments", args)

    val result = sendRequest("tools/call", Some(params))
    val isError = Option(result.get("isError")).exists(_.asBoolean(false))
    val contentNode = Option(result.get("content")).filter(_.isArray)
    val text = contentNode match {
      case Some(arr) =>
        val sb = new StringBuilder
        val iter = arr.elements()
        while (iter.hasNext) {
          val item = iter.next()
          val itemType = Option(item.get("type")).map(_.asText("")).getOrElse("")
          if (itemType == "text") {
            sb.append(Option(item.get("text")).map(_.asText("")).getOrElse(""))
          }
        }
        sb.toString
      case None => ""
    }
    if (isError) AIAgentToolResult.error(if (text.nonEmpty) text else "MCP tool returned isError")
    else text
  }

  override def close(): Unit = {
    sessionId.foreach { _ =>
      try httpDelete()
      catch {
        case _: Throwable =>
      }
    }
    initialized = false
    sessionId = None
  }

  private def ensureInitialized(): Unit =
    if (!initialized) initialize()

  private def sendNotification(method: String): Unit = {
    val payload = objectMapper.createObjectNode()
    payload.put("jsonrpc", "2.0")
    payload.put("method", method)
    httpPost(payload.toString)
  }

  private def sendRequest(method: String, params: Option[JsonNode]): JsonNode = {
    val id = nextId.incrementAndGet()
    val payload = objectMapper.createObjectNode()
    payload.put("jsonrpc", "2.0")
    payload.put("id", id)
    payload.put("method", method)
    params.foreach(p => payload.set[ObjectNode]("params", p))

    val responseBody = httpPost(payload.toString)
    val root = parseRpcBody(responseBody)
    Option(root.get("error")).foreach { err =>
      val code = Option(err.get("code")).map(_.asInt(0)).getOrElse(0)
      val msg = Option(err.get("message")).map(_.asText("")).getOrElse("unknown")
      throw new RuntimeException(s"MCP $serverName.$method error $code: $msg")
    }
    Option(root.get("result")).getOrElse(objectMapper.createObjectNode())
  }

  private def httpPost(body: String): String = {
    val builder = HttpRequest
      .newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(timeoutSeconds.toLong))
      .header("Content-Type", "application/json")
      .header("Accept", "application/json, text/event-stream")
      .POST(BodyPublishers.ofString(body))
    bearerToken.foreach(t => builder.header("Authorization", s"Bearer ${t.trim}"))
    sessionId.foreach(sid => builder.header("Mcp-Session-Id", sid))
    val request = builder.build()

    val response: HttpResponse[String] = httpClient.send(request, BodyHandlers.ofString())
    val status = response.statusCode()
    if (status < 200 || status >= 300) {
      throw new RuntimeException(s"MCP $serverName HTTP $status: ${response.body()}")
    }
    Option(response.headers().firstValue("mcp-session-id").orElse(null))
      .filter(_.nonEmpty)
      .foreach(sid => sessionId = Some(sid))
    response.body()
  }

  private def httpDelete(): Unit = {
    val builder = HttpRequest
      .newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(timeoutSeconds.toLong))
      .DELETE()
    bearerToken.foreach(t => builder.header("Authorization", s"Bearer ${t.trim}"))
    sessionId.foreach(sid => builder.header("Mcp-Session-Id", sid))
    httpClient.send(builder.build(), BodyHandlers.discarding())
  }

  private def parseRpcBody(body: String): JsonNode = {
    val trimmed = Option(body).getOrElse("").trim
    if (trimmed.isEmpty) objectMapper.createObjectNode()
    else if (trimmed.startsWith("{")) objectMapper.readTree(trimmed)
    else {
      // SSE: take the first `data:` line that parses as JSON-RPC.
      val data = trimmed.linesIterator
        .map(_.trim)
        .filter(_.startsWith("data:"))
        .map(_.stripPrefix("data:").trim)
        .find(_.startsWith("{"))
      data
        .map(objectMapper.readTree)
        .getOrElse(objectMapper.createObjectNode())
    }
  }
}
