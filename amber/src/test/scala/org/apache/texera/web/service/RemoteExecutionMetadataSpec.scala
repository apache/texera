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

package org.apache.texera.web.service

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

/**
  * Unit tests for the HTTP core of [[RemoteExecutionMetadata]] — the layer the computing unit uses
  * to read/write execution metadata over HTTP instead of JDBC (issue #5011). Every public method
  * routes through this `request` overload, so its method/auth/body forwarding and 2xx/404/error
  * handling are what matters.
  */
class RemoteExecutionMetadataSpec extends AnyFlatSpec with Matchers {

  private def withServer(handler: HttpExchange => Unit)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/",
      new HttpHandler { override def handle(exchange: HttpExchange): Unit = handler(exchange) }
    )
    server.start()
    val base = s"http://127.0.0.1:${server.getAddress.getPort}"
    try test(base)
    finally server.stop(0)
  }

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    if (bytes.isEmpty) exchange.sendResponseHeaders(status, -1)
    else {
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      exchange.getResponseBody.write(bytes)
    }
    exchange.close()
  }

  "RemoteExecutionMetadata.request" should "forward the method, bearer token and body, and return a 2xx body" in {
    var seenMethod: String = null
    var seenAuth: String = null
    var seenBody: String = null
    var seenPath: String = null
    withServer { exchange =>
      seenMethod = exchange.getRequestMethod
      seenAuth = exchange.getRequestHeaders.getFirst("Authorization")
      seenPath = exchange.getRequestURI.toString
      seenBody = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
      respond(exchange, 200, """{"eid":7}""")
    } { base =>
      val result = RemoteExecutionMetadata.request(
        base,
        "tok-1",
        "POST",
        "/create",
        Some("""{"workflowId":3}""")
      )
      result shouldBe Some("""{"eid":7}""")
      seenMethod shouldBe "POST"
      seenAuth shouldBe "Bearer tok-1"
      seenPath shouldBe "/create"
      seenBody should include("workflowId")
    }
  }

  it should "return None for a 404 (e.g. no result URI yet)" in {
    withServer { exchange => respond(exchange, 404, "") } { base =>
      RemoteExecutionMetadata.request(
        base,
        "tok",
        "GET",
        "/1/port-result?opId=a&portId=0&internal=false",
        None
      ) shouldBe None
    }
  }

  it should "throw for a non-2xx, non-404 status" in {
    withServer { exchange => respond(exchange, 500, "boom") } { base =>
      val ex = the[RuntimeException] thrownBy
        RemoteExecutionMetadata.request(base, "tok", "PUT", "/1/runtime-stats-uri", Some("{}"))
      ex.getMessage should include("500")
    }
  }
}
