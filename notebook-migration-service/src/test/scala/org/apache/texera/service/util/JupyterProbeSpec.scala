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

package org.apache.texera.service.util

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress

class JupyterProbeSpec extends AnyFlatSpec with Matchers {

  // Binds an ephemeral port, so this never collides with the fixed-port stub in
  // NotebookMigrationResourceSpec.
  private def withServer(status: Int)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext(
      "/api",
      (exchange: HttpExchange) => {
        exchange.getRequestBody.readAllBytes()
        val body = """{"version":"2.7.0"}""".getBytes("UTF-8")
        exchange.sendResponseHeaders(status, body.length)
        val os = exchange.getResponseBody
        os.write(body)
        os.close()
      }
    )
    server.start()
    try test(s"http://localhost:${server.getAddress.getPort}")
    finally server.stop(0)
  }

  // Serves /api/contents only to the expected token, mimicking Jupyter's own behaviour.
  private def withTokenServer(expected: String)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext(
      "/api/contents",
      (exchange: HttpExchange) => {
        exchange.getRequestBody.readAllBytes()
        val auth = Option(exchange.getRequestHeaders.getFirst("Authorization")).getOrElse("")
        val status = if (auth == s"token $expected") 200 else 403
        val body = "{}".getBytes("UTF-8")
        exchange.sendResponseHeaders(status, body.length)
        val os = exchange.getResponseBody
        os.write(body)
        os.close()
      }
    )
    server.start()
    try test(s"http://localhost:${server.getAddress.getPort}")
    finally server.stop(0)
  }

  "isAuthorized" should "accept a pod that takes the given token" in {
    withTokenServer("good")(JupyterProbe.isAuthorized(_, "good") shouldBe true)
  }

  it should "reject a pod holding a superseded token" in {
    // The rotation case: the pod is alive, so only an authenticated probe catches it.
    withTokenServer("old")(JupyterProbe.isAuthorized(_, "new") shouldBe false)
  }

  it should "report unauthorized when nothing is listening" in {
    JupyterProbe.isAuthorized("http://localhost:1", "any") shouldBe false
  }

  it should "report unauthorized for a malformed URL without opening a connection" in {
    // The URL itself throws, so the cleanup path runs with no connection to close.
    JupyterProbe.isAuthorized("notaprotocol://host", "any") shouldBe false
  }

  "isAvailable" should "treat 200 as reachable" in {
    withServer(200)(JupyterProbe.isAvailable(_) shouldBe true)
  }

  it should "treat 403 as reachable" in {
    // /api needs no token, so a refusal still proves the server is up.
    withServer(403)(JupyterProbe.isAvailable(_) shouldBe true)
  }

  it should "treat any other status as unavailable" in {
    withServer(500)(JupyterProbe.isAvailable(_) shouldBe false)
  }

  it should "report unavailable when nothing is listening" in {
    // Port 1 is reserved and unbound; the connect fails rather than hanging.
    JupyterProbe.isAvailable("http://localhost:1") shouldBe false
  }

  it should "report unavailable for a malformed URL without opening a connection" in {
    // The URL itself throws, so the cleanup path runs with no connection to close.
    JupyterProbe.isAvailable("notaprotocol://host") shouldBe false
  }
}
