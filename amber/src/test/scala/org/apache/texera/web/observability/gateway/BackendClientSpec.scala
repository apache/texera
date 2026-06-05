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

package org.apache.texera.web.observability.gateway

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.texera.web.observability.gateway.dtos._
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

/**
 * Tests for [[BackendClient]] using an in-process JDK HttpServer.
 *
 * We avoid mocking the HttpClient — that would only exercise our
 * adapter glue. A real socket on the loopback exercises:
 *  - URL composition (path concatenation with the base URL)
 *  - AccountID / ProjectID headers (multi-tenancy guards)
 *  - Status code propagation into [[BackendResponse.isOk]]
 *  - Body decoding under UTF-8
 *  - The 10 MiB response cap that protects against runaway backends
 */
class BackendClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with OptionValues {

  private var server: HttpServer = _
  private var baseUrl: String = _

  // Routing state captured by handlers so tests can assert on what
  // arrived at the backend (headers, path, body).
  private val lastHeaders = scala.collection.mutable.Map.empty[String, String]
  private var lastPath: String = _
  private var lastMethod: String = _
  private var lastBody: Array[Byte] = Array.emptyByteArray
  @volatile private var responseStatus: Int = 200
  @volatile private var responseBody: Array[Byte] = Array.emptyByteArray
  @volatile private var responseContentType: String = "application/json"

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", new HttpHandler {
      override def handle(ex: HttpExchange): Unit = {
        lastHeaders.clear()
        ex.getRequestHeaders.keySet().forEach { k =>
          lastHeaders.put(k, ex.getRequestHeaders.getFirst(k))
        }
        lastPath = ex.getRequestURI.toString
        lastMethod = ex.getRequestMethod
        lastBody = ex.getRequestBody.readAllBytes()
        ex.getResponseHeaders.set("Content-Type", responseContentType)
        ex.sendResponseHeaders(responseStatus, responseBody.length)
        ex.getResponseBody.write(responseBody)
        ex.getResponseBody.close()
      }
    })
    server.setExecutor(null)
    server.start()
    baseUrl = s"http://127.0.0.1:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
  }

  private def scope =
    GatewayScope(userId = 42L, allowedWorkflowIds = Set(7L), allowedProjectIds = Set(9L))

  // ----- happy-path GET ---------------------------------------------

  "BackendClient.get" should "send GET, propagate path, status, body" in {
    responseStatus = 200
    responseBody = """{"ok":true}""".getBytes(StandardCharsets.UTF_8)
    val client = new BackendClient(baseUrl)
    val r = client.get("/select/foo?bar=1", scope, "logs")
    val resp = r.toOption.value
    resp.status shouldBe 200
    resp.body shouldBe """{"ok":true}"""
    resp.isOk shouldBe true
    lastPath shouldBe "/select/foo?bar=1"
    lastMethod shouldBe "GET"
  }

  it should "send the ProjectID header but NOT AccountID (per single-tenant posture)" in {
    responseStatus = 200
    responseBody = "{}".getBytes(StandardCharsets.UTF_8)
    val client = new BackendClient(baseUrl)
    client.get("/x", scope, "logs")
    // JDK HttpServer canonicalises header keys to Pascal-case (case-insensitive).
    lastHeaders.find { case (k, _) => k.equalsIgnoreCase("ProjectID") }.value._2 shouldBe "9"
    // AccountID is deliberately NOT sent — see BackendClient class comment.
    // VictoriaLogs multi-tenancy would otherwise filter every query to an
    // empty tenant because OTel ingest does not set AccountID at write time.
    lastHeaders.keys.exists(_.equalsIgnoreCase("AccountID")) shouldBe false
  }

  it should "default ProjectID to '0' when the allow-set is empty" in {
    val emptyProject = GatewayScope(userId = 1L, allowedWorkflowIds = Set.empty, allowedProjectIds = Set.empty)
    responseStatus = 200; responseBody = "{}".getBytes
    val client = new BackendClient(baseUrl)
    client.get("/x", emptyProject, "logs")
    lastHeaders.find { case (k, _) => k.equalsIgnoreCase("ProjectID") }.value._2 shouldBe "0"
  }

  // ----- non-2xx ----------------------------------------------------

  it should "surface non-2xx in BackendResponse.status, with isOk=false" in {
    responseStatus = 422
    responseBody = "too many points".getBytes(StandardCharsets.UTF_8)
    val client = new BackendClient(baseUrl)
    val resp = client.get("/api/v1/query_range?query=x", scope, "metrics").toOption.value
    resp.status shouldBe 422
    resp.body shouldBe "too many points"
    resp.isOk shouldBe false
  }

  it should "surface 503/Connection-refused as BackendUnreachable" in {
    // Point at a port we know is closed. Pick a high ephemeral port
    // unlikely to be bound; localhost-only so no external impact.
    val client = new BackendClient("http://127.0.0.1:1")
    val r = client.get("/", scope, "logs")
    r.isLeft shouldBe true
    r.swap.toOption.value.code shouldBe "backend_unreachable"
  }

  // ----- body cap ---------------------------------------------------

  it should "surface ResponseTooLarge when the body exceeds MaxResponseBytes" in {
    // Build a body just over the cap. Reuse a small buffer to keep
    // the test's memory footprint tame.
    val len = (MaxResponseBytes + 1L).toInt
    val chunk = Array.fill[Byte](1024)('a'.toByte)
    val buf = new java.io.ByteArrayOutputStream(len)
    while (buf.size() < len) buf.write(chunk, 0, math.min(chunk.length, len - buf.size()))
    responseStatus = 200; responseBody = buf.toByteArray
    val client = new BackendClient(baseUrl)
    val r = client.get("/x", scope, "logs")
    r.isLeft shouldBe true
    r.swap.toOption.value.code shouldBe "response_too_large"
  }

  // ----- POST -------------------------------------------------------

  it should "POST a byte body with the supplied content-type" in {
    responseStatus = 200
    responseBody = "{\"echoed\":true}".getBytes(StandardCharsets.UTF_8)
    val client = new BackendClient(baseUrl)
    val sent = "logs-payload".getBytes(StandardCharsets.UTF_8)
    client.post("/insert", sent, "application/x-ndjson", scope, "logs")
    lastMethod shouldBe "POST"
    new String(lastBody, StandardCharsets.UTF_8) shouldBe "logs-payload"
    lastHeaders.find { case (k, _) => k.equalsIgnoreCase("Content-Type") }.value._2 shouldBe "application/x-ndjson"
  }
}
