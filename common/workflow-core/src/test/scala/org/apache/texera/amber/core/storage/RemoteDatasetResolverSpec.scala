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

package org.apache.texera.amber.core.storage

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

class RemoteDatasetResolverSpec extends AnyFlatSpec with Matchers {

  /** Starts a throwaway in-process HTTP server at /api/dataset/resolve and runs `test` with its URL. */
  private def withResolveServer(
      handler: HttpExchange => Unit
  )(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/api/dataset/resolve",
      new HttpHandler { override def handle(exchange: HttpExchange): Unit = handler(exchange) }
    )
    server.start()
    val endpoint = s"http://127.0.0.1:${server.getAddress.getPort}/api/dataset/resolve"
    try test(endpoint)
    finally server.stop(0)
  }

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.sendResponseHeaders(status, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }

  "RemoteDatasetResolver" should "GET the file-service with the bearer token and parse the returned URI" in {
    var seenAuth: String = null
    var seenQuery: String = null
    withResolveServer { exchange =>
      seenAuth = exchange.getRequestHeaders.getFirst("Authorization")
      seenQuery = exchange.getRequestURI.getQuery
      respond(exchange, 200, "dataset:///dataset-15/abc123/california/tw1.csv")
    } { endpoint =>
      val uri =
        RemoteDatasetResolver.resolve(
          endpoint,
          "tok-123",
          "/bob@texera.com/twitter/v1/california/tw1.csv"
        )

      uri.toString shouldBe "dataset:///dataset-15/abc123/california/tw1.csv"
      uri.getScheme shouldBe "dataset"
      seenAuth shouldBe "Bearer tok-123"
      // the dataset path is URL-encoded into the `path` query parameter
      seenQuery should include("path=")
      seenQuery should include("twitter")
    }
  }

  it should "fail when the file-service returns a non-200 status" in {
    withResolveServer { exchange =>
      respond(exchange, 404, "not found")
    } { endpoint =>
      val ex = the[RuntimeException] thrownBy
        RemoteDatasetResolver.resolve(endpoint, "tok", "/owner/ds/v1/missing.csv")
      ex.getMessage should include("404")
    }
  }
}
