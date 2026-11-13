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

package org.apache.texera.web.resource.aiassistant

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

class AiAssistantManagerSpec extends AnyFlatSpec with Matchers with PrivateMethodTester {

  private val initOpenAI = PrivateMethod[String](Symbol("initOpenAI"))

  private def initializeOpenAI(): String = AiAssistantManager invokePrivate initOpenAI()

  private def withAssistantConfiguration[T](
      accountKey: String,
      sharedUrl: String
  )(test: => T): T = {
    val originalAccountKey = AiAssistantManager.accountKey
    val originalSharedUrl = AiAssistantManager.sharedUrl
    AiAssistantManager.accountKey = accountKey
    AiAssistantManager.sharedUrl = sharedUrl
    try {
      test
    } finally {
      AiAssistantManager.accountKey = originalAccountKey
      AiAssistantManager.sharedUrl = originalSharedUrl
    }
  }

  private def withModelsServer(statusCode: Int)(
      test: (String, AtomicReference[String], AtomicReference[String]) => Unit
  ): Unit = {
    val requestMethod = new AtomicReference[String]()
    val authorization = new AtomicReference[String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/models",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          requestMethod.set(exchange.getRequestMethod)
          authorization.set(exchange.getRequestHeaders.getFirst("Authorization"))
          exchange.sendResponseHeaders(statusCode, -1)
          exchange.close()
        }
      }
    )
    server.start()
    try {
      val serverUrl = s"http://127.0.0.1:${server.getAddress.getPort}"
      test(serverUrl, requestMethod, authorization)
    } finally {
      server.stop(0)
    }
  }

  "initOpenAI" should "accept a successful models response with a normalized bearer key" in {
    withModelsServer(200) { (serverUrl, requestMethod, authorization) =>
      withAssistantConfiguration("  \"test-key\"  ", serverUrl) {
        initializeOpenAI() shouldBe "OpenAI"
      }

      requestMethod.get() shouldBe "GET"
      authorization.get() shouldBe "Bearer test-key"
    }
  }

  it should "reject a non-successful models response" in {
    withModelsServer(503) { (serverUrl, _, _) =>
      withAssistantConfiguration("test-key", serverUrl) {
        initializeOpenAI() shouldBe "NoAiAssistant"
      }
    }
  }

  it should "fall back when the configured service URL is malformed" in {
    withAssistantConfiguration("test-key", "not a valid URL") {
      initializeOpenAI() shouldBe "NoAiAssistant"
    }
  }
}
