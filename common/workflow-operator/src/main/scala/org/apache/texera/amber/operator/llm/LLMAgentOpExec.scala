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

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.operator.http.util.{HttpClientFactory, TemplateInterpolator}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import java.net.http.{HttpRequest, HttpResponse}
import java.time.Duration
import scala.collection.mutable

class LLMAgentOpExec(descString: String) extends OperatorExecutor with LazyLogging {
  private val desc: LLMAgentOpDesc =
    objectMapper.readValue(descString, classOf[LLMAgentOpDesc])

  private val outputColName: String =
    if (desc.outputColumnName == null || desc.outputColumnName.trim.isEmpty) "llm_response"
    else desc.outputColumnName

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    val provider = if (desc.provider == null) LLMProvider.ANTHROPIC else desc.provider

    val (replyText, errorMessage) = try {
      val apiKey = resolveApiKey(provider)
      if (apiKey == null || apiKey.isEmpty) {
        throw new RuntimeException(
          s"No API key for $provider. Set the operator's API Key field or " +
            s"the ${envVarName(provider)} environment variable on the worker JVM."
        )
      }
      val systemResolved = TemplateInterpolator.interpolate(desc.systemPrompt, tuple)
      val userResolved = TemplateInterpolator.interpolate(desc.userPromptTemplate, tuple)

      val bodyJson = buildRequestBody(provider, systemResolved, userResolved)

      val request = buildRequest(provider, apiKey, bodyJson)
      val response = HttpClientFactory.sharedClient
        .send(request, HttpResponse.BodyHandlers.ofString())

      if (response.statusCode() >= 300) {
        ("", s"HTTP ${response.statusCode()}: ${response.body()}")
      } else {
        val parsed = extractReplyText(provider, response.body())
        (parsed, null.asInstanceOf[String])
      }
    } catch {
      case t: Throwable =>
        if (desc.failOnError) throw t
        ("", s"${t.getClass.getSimpleName}: ${t.getMessage}")
    }

    if (desc.failOnError && errorMessage != null) {
      throw new RuntimeException(s"LLM call failed: $errorMessage")
    }

    val fields = mutable.LinkedHashMap[String, Any]()
    tuple.schema.getAttributeNames.foreach { name =>
      fields(name) = tuple.getField[Any](name)
    }
    fields(outputColName) = replyText
    fields("llm_error") = errorMessage
    Iterator(TupleLike(fields.toSeq: _*))
  }

  private def resolveApiKey(provider: LLMProvider): String = {
    if (desc.apiKey != null && desc.apiKey.nonEmpty) desc.apiKey
    else {
      val v = System.getenv(envVarName(provider))
      if (v == null) "" else v
    }
  }

  private def envVarName(provider: LLMProvider): String = provider match {
    case LLMProvider.ANTHROPIC => "ANTHROPIC_API_KEY"
    case LLMProvider.OPENAI    => "OPENAI_API_KEY"
  }

  private def buildRequestBody(
      provider: LLMProvider,
      systemContent: String,
      userContent: String
  ): String = {
    val root: ObjectNode = objectMapper.createObjectNode()
    root.put("model", desc.model)
    root.put("max_tokens", desc.maxTokens)
    root.put("temperature", desc.temperature)

    provider match {
      case LLMProvider.ANTHROPIC =>
        root.put("system", systemContent)
        val messages = root.putArray("messages")
        val userMsg = messages.addObject()
        userMsg.put("role", "user")
        userMsg.put("content", userContent)
      case LLMProvider.OPENAI =>
        val messages = root.putArray("messages")
        val sysMsg = messages.addObject()
        sysMsg.put("role", "system")
        sysMsg.put("content", systemContent)
        val userMsg = messages.addObject()
        userMsg.put("role", "user")
        userMsg.put("content", userContent)
    }
    objectMapper.writeValueAsString(root)
  }

  private def buildRequest(
      provider: LLMProvider,
      apiKey: String,
      bodyJson: String
  ): HttpRequest = {
    val (url, headers) = provider match {
      case LLMProvider.ANTHROPIC =>
        (
          "https://api.anthropic.com/v1/messages",
          Seq(
            "x-api-key" -> apiKey,
            "anthropic-version" -> "2023-06-01",
            "content-type" -> "application/json"
          )
        )
      case LLMProvider.OPENAI =>
        (
          "https://api.openai.com/v1/chat/completions",
          Seq(
            "Authorization" -> s"Bearer $apiKey",
            "content-type" -> "application/json"
          )
        )
    }

    val builder = HttpRequest
      .newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(math.max(1, desc.timeoutSeconds).toLong))
      .POST(HttpRequest.BodyPublishers.ofString(bodyJson))

    headers.foreach { case (k, v) => builder.header(k, v) }
    builder.build()
  }

  private def extractReplyText(provider: LLMProvider, responseBody: String): String = {
    val root = objectMapper.readTree(responseBody)
    provider match {
      case LLMProvider.ANTHROPIC =>
        // { "content": [ { "type": "text", "text": "..." } ], ... }
        val contentArr = root.path("content")
        if (contentArr.isArray && contentArr.size() > 0) {
          contentArr.get(0).path("text").asText("")
        } else ""
      case LLMProvider.OPENAI =>
        // { "choices": [ { "message": { "content": "..." } } ], ... }
        val choices = root.path("choices")
        if (choices.isArray && choices.size() > 0) {
          choices.get(0).path("message").path("content").asText("")
        } else ""
    }
  }
}
