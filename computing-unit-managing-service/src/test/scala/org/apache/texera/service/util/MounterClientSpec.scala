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

package org.apache.texera.service.util

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.collection.mutable

/**
  * Tests [[MounterClient]] against a stub node-mounter HTTP server, verifying it builds the
  * mount / unmount / list requests the mounter expects and parses the responses correctly.
  */
class MounterClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var server: HttpServer = _
  private var port: Int = _
  // records the last request the stub received, per path
  private val received = mutable.Map[String, (String, String, String)]() // path -> (method, query, body)

  private def body(ex: HttpExchange): String =
    new String(ex.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)

  private def reply(ex: HttpExchange, code: Int, json: String): Unit = {
    val bytes = json.getBytes(StandardCharsets.UTF_8)
    ex.sendResponseHeaders(code, bytes.length)
    ex.getResponseBody.write(bytes)
    ex.getResponseBody.close()
  }

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/mount",
      (ex: HttpExchange) => {
        received("/mount") = (ex.getRequestMethod, Option(ex.getRequestURI.getQuery).getOrElse(""), body(ex))
        if (ex.getRequestMethod == "POST")
          reply(ex, 200, """{"mountPath":"/var/lib/texera-mounts/7/dataset-1/abc123"}""")
        else reply(ex, 200, """{"status":"ok"}""") // DELETE
      }
    )
    server.createContext(
      "/mounts",
      (ex: HttpExchange) => {
        received("/mounts") = (ex.getRequestMethod, Option(ex.getRequestURI.getQuery).getOrElse(""), "")
        reply(
          ex,
          200,
          """{"mounts":[{"repositoryName":"dataset-1","commitHash":"abc123","mountPath":"/var/lib/texera-mounts/7/dataset-1/abc123"}]}"""
        )
      }
    )
    server.start()
    port = server.getAddress.getPort

    failing = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    failing.createContext("/", (ex: HttpExchange) => reply(ex, 500, """{"error":"boom"}"""))
    failing.start()
    failingPort = failing.getAddress.getPort
  }

  private var failing: HttpServer = _
  private var failingPort: Int = _

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
    if (failing != null) failing.stop(0)
  }

  "mount" should "POST the mount request and return the mounter's mount path" in {
    val path =
      MounterClient.mount("127.0.0.1", port, "7", "dataset-1", "abc123", "the-jwt", "http://file-service:9092")
    path shouldBe "/var/lib/texera-mounts/7/dataset-1/abc123"
    val (method, _, requestBody) = received("/mount")
    method shouldBe "POST"
    requestBody should include(""""cuid":"7"""")
    requestBody should include(""""repositoryName":"dataset-1"""")
    requestBody should include(""""commitHash":"abc123"""")
    requestBody should include(""""jwt":"the-jwt"""")
    requestBody should include(""""fileServiceBase":"http://file-service:9092"""")
  }

  "listMounts" should "GET with the cuid and parse the returned mounts" in {
    val mounts = MounterClient.listMounts("127.0.0.1", port, "7")
    mounts should have size 1
    mounts.head shouldBe MounterClient.MountEntry(
      "dataset-1",
      "abc123",
      "/var/lib/texera-mounts/7/dataset-1/abc123"
    )
    received("/mounts")._2 should include("cuid=7")
  }

  "unmount" should "DELETE with the identifiers in the query string (no body)" in {
    MounterClient.unmount("127.0.0.1", port, "7", "dataset-1", "abc123")
    val (method, query, _) = received("/mount")
    method shouldBe "DELETE"
    query should include("cuid=7")
    query should include("repositoryName=dataset-1")
    query should include("commitHash=abc123")
  }

  "a non-2xx mounter response" should "surface as a RuntimeException carrying the HTTP status" in {
    val ex = intercept[RuntimeException] {
      MounterClient.mount("127.0.0.1", failingPort, "7", "dataset-1", "abc123", "j", "http://f")
    }
    ex.getMessage should include("HTTP 500")
  }
}
