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
import com.typesafe.scalalogging.LazyLogging
import dev.langchain4j.agent.tool.ToolSpecification
import dev.langchain4j.data.message.{
  AiMessage,
  ChatMessage,
  SystemMessage,
  ToolExecutionResultMessage,
  UserMessage
}
import dev.langchain4j.model.chat.request.{ChatRequest, ToolChoice}
import dev.langchain4j.model.openai.OpenAiChatModel
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.time.Duration
import scala.jdk.CollectionConverters._

case class ChatCompletionResult(text: String, usdCost: Double)

trait ChatCompletionClient extends Serializable {
  def complete(
      apiKey: String,
      model: String,
      systemPrompt: String,
      userPrompt: String,
      temperature: Double,
      timeoutSeconds: Int
  ): ChatCompletionResult

  def completeWithRequiredTool(
      apiKey: String,
      model: String,
      systemPrompt: String,
      userPrompt: String,
      temperature: Double,
      timeoutSeconds: Int,
      toolSpecification: ToolSpecification
  ): ChatCompletionResult

  /**
    * Multi-turn tool-execution loop for the per-row AI Agent.
    *
    * Mirrors the loop shape used by the workflow-edit assistant in
    * `agent-service`, simplified for the per-row data-in/data-out case: no
    * persistent conversation, no streaming. The loop terminates when the model
    * calls `finalAnswerTool` (returning its arguments JSON), when the model
    * returns plain text with no tool call (returned verbatim) and no
    * finalAnswer is required, or when `maxIterations` is exhausted (the last
    * turn is forced to call finalAnswerTool if one is supplied).
    */
  def completeWithTools(
      apiKey: String,
      model: String,
      systemPrompt: String,
      userPrompt: String,
      temperature: Double,
      timeoutSeconds: Int,
      tools: List[AIAgentTool],
      finalAnswerTool: Option[ToolSpecification],
      maxIterations: Int,
      maxRowCostUsd: Option[Double] = None
  ): ChatCompletionResult
}

class OpenRouterClient extends ChatCompletionClient with LazyLogging {
  override def complete(
      apiKey: String,
      model: String,
      systemPrompt: String,
      userPrompt: String,
      temperature: Double,
      timeoutSeconds: Int
  ): ChatCompletionResult = {
    require(apiKey != null && apiKey.trim.nonEmpty, "OpenRouter API key is required")
    require(model != null && model.trim.nonEmpty, "OpenRouter model is required")
    require(timeoutSeconds > 0, "Timeout seconds must be positive")

    val chatModel = createChatModel(apiKey, model, temperature, timeoutSeconds)

    val response = chatModel.chat(buildMessages(systemPrompt, userPrompt).toList.asJava)
    val text = Option(response.aiMessage().text()).getOrElse("")
    val cost = OpenRouterPricing.costFor(model, Option(response.tokenUsage()))
    ChatCompletionResult(text, cost)
  }

  override def completeWithRequiredTool(
      apiKey: String,
      model: String,
      systemPrompt: String,
      userPrompt: String,
      temperature: Double,
      timeoutSeconds: Int,
      toolSpecification: ToolSpecification
  ): ChatCompletionResult =
    completeWithTools(
      apiKey,
      model,
      systemPrompt,
      userPrompt,
      temperature,
      timeoutSeconds,
      tools = List.empty,
      finalAnswerTool = Some(toolSpecification),
      maxIterations = 1
    )

  override def completeWithTools(
      apiKey: String,
      model: String,
      systemPrompt: String,
      userPrompt: String,
      temperature: Double,
      timeoutSeconds: Int,
      tools: List[AIAgentTool],
      finalAnswerTool: Option[ToolSpecification],
      maxIterations: Int,
      maxRowCostUsd: Option[Double] = None
  ): ChatCompletionResult = {
    require(maxIterations > 0, "maxIterations must be positive")
    val chatModel = createChatModel(apiKey, model, temperature, timeoutSeconds)
    val toolsByName: Map[String, AIAgentTool] = tools.map(t => t.name -> t).toMap
    val allSpecs: List[ToolSpecification] = tools.map(_.specification) ++ finalAnswerTool.toList

    val messages = scala.collection.mutable.ListBuffer.empty[ChatMessage]
    messages ++= buildMessages(systemPrompt, userPrompt)

    logger.info(
      s"[AIAgent] start model=$model tools=${tools.map(_.name).mkString(",")} " +
        s"finalAnswer=${finalAnswerTool.map(_.name()).getOrElse("none")} maxIter=$maxIterations"
    )

    var accumulatedCost: Double = 0.0
    var lastText: String = ""
    var iteration = 0
    while (iteration < maxIterations) {
      iteration += 1
      val isLastIteration = iteration == maxIterations
      val turnStart = System.currentTimeMillis()
      val (aiMessage, turnCost) = sendChatTurnWithCost(
        chatModel,
        model,
        messages.toList,
        allSpecs,
        forceFinalAnswer = isLastIteration && finalAnswerTool.isDefined,
        finalAnswerTool
      )
      accumulatedCost += turnCost
      val turnMs = System.currentTimeMillis() - turnStart
      lastText = Option(aiMessage.text()).getOrElse("")

      val requests = Option(aiMessage.toolExecutionRequests())
        .map(_.asScala.toList)
        .getOrElse(List.empty)

      logger.info(
        s"[AIAgent] turn=$iteration/$maxIterations chatMs=$turnMs " +
          s"toolCalls=${requests.map(_.name()).mkString(",")} " +
          s"textLen=${lastText.length} costUsd=$accumulatedCost"
      )

      maxRowCostUsd.foreach { cap =>
        if (accumulatedCost > cap) {
          throw new RuntimeException(
            f"Row cost cap exceeded: $$$accumulatedCost%.6f > $$$cap%.6f"
          )
        }
      }

      if (requests.isEmpty) {
        logger.info(s"[AIAgent] done turn=$iteration reason=plainText")
        return ChatCompletionResult(lastText, accumulatedCost)
      }

      finalAnswerTool
        .flatMap(spec => requests.find(_.name() == spec.name()))
        .foreach { req =>
          logger.info(s"[AIAgent] done turn=$iteration reason=finalAnswerTool")
          return ChatCompletionResult(req.arguments(), accumulatedCost)
        }

      messages += aiMessage
      requests.foreach { request =>
        val toolStart = System.currentTimeMillis()
        val result = toolsByName.get(request.name()) match {
          case Some(tool) =>
            try tool.execute(Option(request.arguments()).getOrElse("{}"))
            catch {
              case t: Throwable =>
                AIAgentToolResult.error(
                  s"Tool ${request.name()} threw ${t.getClass.getSimpleName}: ${Option(t.getMessage)
                    .getOrElse("")}"
                )
            }
          case None =>
            AIAgentToolResult.error(s"Unknown tool: ${request.name()}")
        }
        val toolMs = System.currentTimeMillis() - toolStart
        val isErr = AIAgentToolResult.isError(result)
        val argsSnippet =
          Option(request.arguments()).getOrElse("").replaceAll("\\s+", " ").take(300)
        val errSnippet = if (isErr) s" args=$argsSnippet error=${result.take(400)}" else ""
        logger.info(
          s"[AIAgent] tool=${request.name()} ms=$toolMs " +
            s"isError=$isErr resultLen=${result.length}$errSnippet"
        )
        messages += ToolExecutionResultMessage.from(request, result)
      }
    }
    logger.warn(s"[AIAgent] done turn=$iteration reason=maxIterations textLen=${lastText.length}")
    ChatCompletionResult(lastText, accumulatedCost)
  }

  private def sendChatTurnWithCost(
      chatModel: OpenAiChatModel,
      model: String,
      messages: List[ChatMessage],
      toolSpecifications: List[ToolSpecification],
      forceFinalAnswer: Boolean,
      finalAnswerTool: Option[ToolSpecification]
  ): (AiMessage, Double) = {
    val builder = ChatRequest.builder().messages(messages.asJava)
    if (forceFinalAnswer && finalAnswerTool.isDefined) {
      builder
        .toolSpecifications(finalAnswerTool.get)
        .toolChoice(ToolChoice.REQUIRED)
    } else if (toolSpecifications.nonEmpty) {
      builder.toolSpecifications(toolSpecifications.asJava)
      if (finalAnswerTool.isDefined && toolSpecifications.size == 1) {
        builder.toolChoice(ToolChoice.REQUIRED)
      }
    }
    val resp = chatModel.chat(builder.build())
    val cost = OpenRouterPricing.costFor(model, Option(resp.tokenUsage()))
    (resp.aiMessage(), cost)
  }

  private def createChatModel(
      apiKey: String,
      model: String,
      temperature: Double,
      timeoutSeconds: Int
  ): OpenAiChatModel = {
    require(apiKey != null && apiKey.trim.nonEmpty, "OpenRouter API key is required")
    require(model != null && model.trim.nonEmpty, "OpenRouter model is required")
    require(timeoutSeconds > 0, "Timeout seconds must be positive")

    OpenAiChatModel
      .builder()
      .baseUrl(OpenRouterClient.OpenRouterBaseUrl)
      .apiKey(apiKey.trim)
      .modelName(model)
      .temperature(temperature)
      .timeout(Duration.ofSeconds(timeoutSeconds.toLong))
      .build()
  }

  private def buildMessages(
      systemPrompt: String,
      userPrompt: String
  ): scala.collection.mutable.ListBuffer[dev.langchain4j.data.message.ChatMessage] = {
    val messages =
      scala.collection.mutable.ListBuffer.empty[dev.langchain4j.data.message.ChatMessage]
    if (systemPrompt != null && systemPrompt.nonEmpty) {
      messages += SystemMessage.from(systemPrompt)
    }
    messages += UserMessage.from(Option(userPrompt).getOrElse(""))
    messages
  }
}

object OpenRouterPricing extends com.typesafe.scalalogging.LazyLogging {
  // USD per 1M tokens (prompt, completion). Fallback used when the OpenRouter
  // models API is unreachable.
  private val fallbackTable: Map[String, (Double, Double)] = Map(
    "openai/gpt-4o-mini" -> (0.15, 0.60),
    "openai/gpt-4o" -> (2.50, 10.00),
    "openai/gpt-5" -> (1.25, 10.00),
    "anthropic/claude-3.5-sonnet" -> (3.00, 15.00),
    "anthropic/claude-3.5-haiku" -> (0.80, 4.00),
    "google/gemini-2.0-flash-001" -> (0.10, 0.40),
    "meta-llama/llama-3.3-70b-instruct" -> (0.12, 0.30)
  )

  // Per-token (not per-1M) pricing fetched from OpenRouter. Lazy + cached for
  // the JVM lifetime; one network hit per worker on first use.
  @volatile private var remoteTable: Option[Map[String, (Double, Double)]] = None
  private val lock = new Object

  private def loadRemoteTable(): Map[String, (Double, Double)] = {
    val client = java.net.http.HttpClient
      .newBuilder()
      .connectTimeout(java.time.Duration.ofSeconds(5))
      .build()
    val req = java.net.http.HttpRequest
      .newBuilder(java.net.URI.create("https://openrouter.ai/api/v1/models"))
      .timeout(java.time.Duration.ofSeconds(10))
      .GET()
      .build()
    val resp = client.send(req, java.net.http.HttpResponse.BodyHandlers.ofString())
    if (resp.statusCode() / 100 != 2) {
      throw new RuntimeException(s"OpenRouter /models HTTP ${resp.statusCode()}")
    }
    val root = objectMapper.readTree(resp.body())
    val data = root.get("data")
    if (data == null || !data.isArray) {
      throw new RuntimeException("OpenRouter /models response missing data[]")
    }
    val builder = Map.newBuilder[String, (Double, Double)]
    data.elements().forEachRemaining { node =>
      val id = Option(node.get("id")).map(_.asText("")).getOrElse("")
      val pricing = node.get("pricing")
      if (id.nonEmpty && pricing != null) {
        val pIn = Option(pricing.get("prompt")).flatMap(n => parseDouble(n.asText(""))).getOrElse(0.0)
        val pOut = Option(pricing.get("completion")).flatMap(n => parseDouble(n.asText(""))).getOrElse(0.0)
        builder += (id.toLowerCase -> (pIn, pOut))
      }
    }
    builder.result()
  }

  private def parseDouble(s: String): Option[Double] =
    try Some(s.toDouble) catch { case _: Throwable => None }

  private def perTokenPricing(model: String): (Double, Double) = {
    val key = Option(model).map(_.trim.toLowerCase).getOrElse("")
    if (remoteTable.isEmpty) {
      lock.synchronized {
        if (remoteTable.isEmpty) {
          try {
            val t = loadRemoteTable()
            logger.info(s"[AIAgent] loaded OpenRouter pricing for ${t.size} models")
            remoteTable = Some(t)
          } catch {
            case t: Throwable =>
              logger.warn(s"[AIAgent] OpenRouter pricing fetch failed: ${t.getMessage}; using fallback table")
              remoteTable = Some(Map.empty)
          }
        }
      }
    }
    remoteTable.get.get(key) match {
      case Some(p) => p
      case None =>
        val (pIn, pOut) = fallbackTable.getOrElse(key, (0.0, 0.0))
        (pIn / 1000000.0, pOut / 1000000.0)
    }
  }

  def costFor(model: String, usage: Option[dev.langchain4j.model.output.TokenUsage]): Double = {
    val (pIn, pOut) = perTokenPricing(model)
    usage match {
      case Some(u) =>
        val inTok = Option(u.inputTokenCount()).map(_.intValue).getOrElse(0)
        val outTok = Option(u.outputTokenCount()).map(_.intValue).getOrElse(0)
        inTok * pIn + outTok * pOut
      case None => 0.0
    }
  }
}

object OpenRouterClient {
  final val OpenRouterBaseUrl = "https://openrouter.ai/api/v1"
  final val OpenRouterChatCompletionsUrl = "https://openrouter.ai/api/v1/chat/completions"

  def parseChatCompletionContent(responseBody: String): String = {
    val root = objectMapper.readTree(responseBody)
    extractFirstChoiceContent(root).getOrElse {
      throw new RuntimeException("OpenRouter response does not contain choices[0].message.content")
    }
  }

  private def extractFirstChoiceContent(root: JsonNode): Option[String] =
    for {
      choices <- Option(root.get("choices"))
      if choices.isArray && choices.size() > 0
      choice <- Option(choices.get(0))
      message <- Option(choice.get("message"))
      content <- Option(message.get("content"))
      if content.isTextual
    } yield content.asText()
}
