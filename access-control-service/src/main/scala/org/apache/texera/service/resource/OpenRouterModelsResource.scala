// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.service.resource

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import jakarta.ws.rs.core.{MediaType, Response}
import jakarta.ws.rs.{GET, Path, Produces}
import org.apache.texera.config.{GuiConfig, LLMConfig}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.{Clock, Duration}
import scala.jdk.CollectionConverters.IteratorHasAsScala

case class OpenRouterModelSummary(
    id: String,
    name: String,
    contextLength: Option[Long],
    pricing: Map[String, String]
)

case class OpenRouterModelsResponse(
    data: Seq[OpenRouterModelSummary],
    cachedAtEpochMillis: Long,
    expiresAtEpochMillis: Long,
    stale: Boolean,
    error: Option[String] = None
)

case class OpenRouterModelsError(error: String)

object OpenRouterModelsResource {
  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val openRouterModelsUri =
    URI.create("https://openrouter.ai/api/v1/models?output_modalities=text")
  private val client = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofSeconds(3))
    .build()

  def buildOpenRouterModelsRequest(openRouterApiKey: Option[String]): HttpRequest = {
    val builder = HttpRequest
      .newBuilder(openRouterModelsUri)
      .timeout(Duration.ofSeconds(5))
      .header("Accept", "application/json")
      .header("User-Agent", "Apache-Texera")

    openRouterApiKey
      .map(_.trim)
      .filter(_.nonEmpty)
      .foreach(apiKey => builder.header("Authorization", s"Bearer $apiKey"))

    builder.GET().build()
  }

  def fetchOpenRouterModelsJson(
      openRouterApiKey: Option[String] = LLMConfig.openRouterApiKey
  ): String = {
    val request = buildOpenRouterModelsRequest(openRouterApiKey)

    val response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    if (response.statusCode() / 100 != 2) {
      throw new RuntimeException(s"OpenRouter returned HTTP ${response.statusCode()}")
    }
    response.body()
  }

  def summarizeModels(rawJson: String): Seq[OpenRouterModelSummary] = {
    val root = mapper.readTree(rawJson)
    val data = root.path("data")
    if (!data.isArray) {
      throw new IllegalArgumentException("OpenRouter response does not contain a data array")
    }

    data.elements().asScala.flatMap { model =>
      for {
        id <- nonEmptyText(model, "id")
        name <- nonEmptyText(model, "name")
      } yield OpenRouterModelSummary(
        id = id,
        name = name,
        contextLength = longValue(model, "context_length"),
        pricing = pricing(model.path("pricing"))
      )
    }.toSeq
  }

  private def nonEmptyText(node: JsonNode, fieldName: String): Option[String] =
    Option(node.get(fieldName))
      .filterNot(n => n.isNull || n.isMissingNode)
      .map(_.asText().trim)
      .filter(_.nonEmpty)

  private def longValue(node: JsonNode, fieldName: String): Option[Long] =
    Option(node.get(fieldName))
      .filter(n => n.isNumber)
      .map(_.asLong())

  private def pricing(node: JsonNode): Map[String, String] =
    if (node == null || !node.isObject) {
      Map.empty
    } else {
      node
        .fields()
        .asScala
        .filterNot(entry => entry.getValue.isNull || entry.getValue.isMissingNode)
        .map(entry => entry.getKey -> entry.getValue.asText())
        .toMap
    }
}

@Path("/models/openrouter")
@Produces(Array(MediaType.APPLICATION_JSON))
class OpenRouterModelsResource(
    fetchModelsJson: () => String = () => OpenRouterModelsResource.fetchOpenRouterModelsJson(),
    clock: Clock = Clock.systemUTC(),
    cacheTtl: Duration = Duration.ofHours(1),
    staleFailureRetryTtl: Duration = Duration.ofMinutes(5),
    isCopilotEnabled: () => Boolean = () => GuiConfig.guiWorkflowWorkspaceCopilotEnabled
) extends LazyLogging {

  private var cachedResponse: Option[OpenRouterModelsResponse] = None

  @GET
  def getOpenRouterModels: Response = synchronized {
    if (!isCopilotEnabled()) {
      return Response
        .status(Response.Status.FORBIDDEN)
        .entity("""{"error": "Copilot feature is disabled"}""")
        .build()
    }

    val now = clock.millis()
    cachedResponse.filter(_.expiresAtEpochMillis > now) match {
      case Some(cached) => Response.ok(cached).build()
      case None         => refresh(now)
    }
  }

  private def refresh(now: Long): Response =
    try {
      val models = OpenRouterModelsResource.summarizeModels(fetchModelsJson())
      val response = OpenRouterModelsResponse(
        data = models,
        cachedAtEpochMillis = now,
        expiresAtEpochMillis = now + cacheTtl.toMillis,
        stale = false
      )
      cachedResponse = Some(response)
      Response.ok(response).build()
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to fetch OpenRouter models: ${e.getMessage}", e)
        cachedResponse match {
          case Some(cached) =>
            val staleResponse = cached.copy(
              expiresAtEpochMillis = now + staleFailureRetryTtl.toMillis,
              stale = true,
              error = Some(e.getMessage)
            )
            cachedResponse = Some(staleResponse)
            Response.ok(staleResponse).build()
          case None =>
            Response
              .status(Response.Status.SERVICE_UNAVAILABLE)
              .entity(OpenRouterModelsError(s"Failed to fetch OpenRouter models: ${e.getMessage}"))
              .build()
        }
    }
}
