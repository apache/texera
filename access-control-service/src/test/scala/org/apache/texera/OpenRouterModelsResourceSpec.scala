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

package org.apache.texera

import jakarta.ws.rs.core.Response
import org.apache.texera.service.resource.{
  OpenRouterModelSummary,
  OpenRouterModelsResource,
  OpenRouterModelsResponse
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Clock, Duration, Instant, ZoneOffset}

class OpenRouterModelsResourceSpec extends AnyFlatSpec with Matchers {

  private val fixtureJson =
    """
      |{
      |  "data": [
      |    {
      |      "id": "openai/gpt-4",
      |      "name": "GPT-4",
      |      "context_length": 8192,
      |      "pricing": {
      |        "prompt": "0.00003",
      |        "completion": "0.00006"
      |      }
      |    },
      |    {
      |      "id": "meta-llama/llama-3.1-8b-instruct:free",
      |      "name": "Meta: Llama 3.1 8B Instruct (free)",
      |      "pricing": {
      |        "prompt": "0",
      |        "completion": "0"
      |      }
      |    }
      |  ]
      |}
      |""".stripMargin

  private val fixedClock: Clock =
    Clock.fixed(Instant.ofEpochMilli(1000), ZoneOffset.UTC)

  private def openRouterResource(
      fetchModelsJson: () => String,
      clock: Clock = fixedClock,
      cacheTtl: Duration = Duration.ofHours(1),
      staleFailureRetryTtl: Duration = Duration.ofMinutes(5)
  ): OpenRouterModelsResource =
    new OpenRouterModelsResource(
      fetchModelsJson,
      clock,
      cacheTtl,
      staleFailureRetryTtl,
      isCopilotEnabled = () => true
    )

  "OpenRouterModelsResource" should "summarize model ids, names, context, and pricing" in {
    val resource = openRouterResource(() => fixtureJson)

    val response = resource.getOpenRouterModels
    response.getStatus shouldBe Response.Status.OK.getStatusCode

    val entity = response.getEntity.asInstanceOf[OpenRouterModelsResponse]
    entity.stale shouldBe false
    entity.cachedAtEpochMillis shouldBe 1000
    entity.data should contain theSameElementsInOrderAs Seq(
      OpenRouterModelSummary(
        "openai/gpt-4",
        "GPT-4",
        Some(8192),
        Map("prompt" -> "0.00003", "completion" -> "0.00006")
      ),
      OpenRouterModelSummary(
        "meta-llama/llama-3.1-8b-instruct:free",
        "Meta: Llama 3.1 8B Instruct (free)",
        None,
        Map("prompt" -> "0", "completion" -> "0")
      )
    )
  }

  it should "serve cached models without fetching again before the TTL expires" in {
    var fetches = 0
    val resource = openRouterResource(
      () => {
        fetches += 1
        fixtureJson
      },
      fixedClock,
      Duration.ofHours(1)
    )

    resource.getOpenRouterModels.getStatus shouldBe Response.Status.OK.getStatusCode
    resource.getOpenRouterModels.getStatus shouldBe Response.Status.OK.getStatusCode

    fetches shouldBe 1
  }

  it should "return FORBIDDEN without fetching models when copilot is disabled" in {
    var fetches = 0
    val resource = new OpenRouterModelsResource(
      () => {
        fetches += 1
        fixtureJson
      },
      fixedClock,
      isCopilotEnabled = () => false
    )

    val response = resource.getOpenRouterModels

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
    response.getEntity shouldBe """{"error": "Copilot feature is disabled"}"""
    fetches shouldBe 0
  }

  it should "return stale cached models when refresh fails after the TTL expires" in {
    var fetches = 0
    val resource = openRouterResource(
      () => {
        fetches += 1
        if (fetches == 1) fixtureJson else throw new RuntimeException("upstream unavailable")
      },
      fixedClock,
      Duration.ZERO
    )

    resource.getOpenRouterModels.getStatus shouldBe Response.Status.OK.getStatusCode
    val staleResponse = resource.getOpenRouterModels

    staleResponse.getStatus shouldBe Response.Status.OK.getStatusCode
    val entity = staleResponse.getEntity.asInstanceOf[OpenRouterModelsResponse]
    entity.stale shouldBe true
    entity.error should contain("upstream unavailable")
  }

  it should "reuse stale cached models during the failure retry window" in {
    var fetches = 0
    val resource = openRouterResource(
      () => {
        fetches += 1
        if (fetches == 1) fixtureJson else throw new RuntimeException("upstream unavailable")
      },
      fixedClock,
      Duration.ZERO,
      staleFailureRetryTtl = Duration.ofMinutes(5)
    )

    resource.getOpenRouterModels.getStatus shouldBe Response.Status.OK.getStatusCode
    resource.getOpenRouterModels.getStatus shouldBe Response.Status.OK.getStatusCode
    val staleResponse = resource.getOpenRouterModels

    staleResponse.getStatus shouldBe Response.Status.OK.getStatusCode
    staleResponse.getEntity.asInstanceOf[OpenRouterModelsResponse].stale shouldBe true
    fetches shouldBe 2
  }

  it should "return SERVICE_UNAVAILABLE when the first upstream fetch fails" in {
    val resource = openRouterResource(
      () => throw new RuntimeException("upstream unavailable"),
      fixedClock
    )

    val response = resource.getOpenRouterModels

    response.getStatus shouldBe Response.Status.SERVICE_UNAVAILABLE.getStatusCode
  }

  it should "return SERVICE_UNAVAILABLE for malformed OpenRouter responses" in {
    val resource = openRouterResource(() => """{"data": {}}""")

    val response = resource.getOpenRouterModels

    response.getStatus shouldBe Response.Status.SERVICE_UNAVAILABLE.getStatusCode
  }

  it should "build OpenRouter requests with authorization when an API key is configured" in {
    val request = OpenRouterModelsResource.buildOpenRouterModelsRequest(Some(" openrouter-key "))

    request.headers().firstValue("Authorization").orElse("") shouldBe "Bearer openrouter-key"
  }

  it should "build OpenRouter requests without authorization when no API key is configured" in {
    val request = OpenRouterModelsResource.buildOpenRouterModelsRequest(None)

    request.headers().firstValue("Authorization").isPresent shouldBe false
  }
}
