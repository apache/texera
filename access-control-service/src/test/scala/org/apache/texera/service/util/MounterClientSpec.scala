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
import java.nio.file.Files
import scala.collection.mutable

class MounterClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var server: HttpServer = _
  private var port: Int = _
  private var client: MounterClient = _

  private val received = mutable.Map[String, (String, String, String)]()
  private val authorization = mutable.Map[String, String]()
  private var replyWith: Option[(Int, String)] = None

  private def bodyOf(exchange: HttpExchange): String =
    new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)

  private def reply(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }

  private def record(exchange: HttpExchange, path: String): Unit = {
    authorization(path) = Option(exchange.getRequestHeaders.getFirst("Authorization")).getOrElse("")
    received(path) = (
      exchange.getRequestMethod,
      Option(exchange.getRequestURI.getQuery).getOrElse(""),
      bodyOf(exchange)
    )
  }

  override def beforeAll(): Unit = {
    val tokenFile = Files.createTempFile("mounter-token", "")
    Files.writeString(tokenFile, "the-service-account-token\n")
    tokenFile.toFile.deleteOnExit()
    client = new MounterClient(tokenFile.toString)

    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/mount",
      (exchange: HttpExchange) => {
        record(exchange, "/mount")
        replyWith match {
          case Some((status, body)) => reply(exchange, status, body)
          case None =>
            reply(exchange, 200, """{"mountPath":"/var/lib/texera-mounts/7/dataset-1/abc123"}""")
        }
      }
    )
    server.start()
    port = server.getAddress.getPort
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  private val nodeIp = "127.0.0.1"

  "MounterClient.mount" should "post the mount request and return the mounter's path" in {
    val path = client.mount(nodeIp, port, "7", "dataset-1", "abc123", "user-jwt", "http://fs:9092")

    path shouldBe "/var/lib/texera-mounts/7/dataset-1/abc123"
    val (method, _, requestBody) = received("/mount")
    method shouldBe "POST"
    requestBody should include(""""cuid":"7"""")
    requestBody should include(""""repositoryName":"dataset-1"""")
    requestBody should include(""""commitHash":"abc123"""")
    requestBody should include(""""jwt":"user-jwt"""")
    requestBody should include(""""fileServiceBase":"http://fs:9092"""")
  }

  it should "identify itself with the projected service-account token" in {
    client.mount(nodeIp, port, "7", "dataset-1", "abc123", "user-jwt", "http://fs:9092")
    authorization("/mount") shouldBe "Bearer the-service-account-token"
  }

  it should "carry the mounter's status back on a refusal" in {
    replyWith = Some((400, """{"error":"nope"}"""))
    try {
      val failure = the[MounterClient.MounterRequestException] thrownBy
        client.mount(nodeIp, port, "7", "dataset-1", "abc123", "user-jwt", "http://fs:9092")
      failure.status shouldBe 400
    } finally replyWith = None
  }

  it should "fail rather than report a mount when the mounter's success names no path" in {
    Seq("{}", """{"mountPath":null}""", """{"mountPath":""}""").foreach { body =>
      replyWith = Some((200, body))
      try {
        an[IllegalStateException] should be thrownBy
          client.mount(nodeIp, port, "7", "dataset-1", "abc123", "user-jwt", "http://fs:9092")
      } finally replyWith = None
    }
  }

  // The escapes reported on the infrastructure PR: each would otherwise be joined into the
  // mount path, and the directory created before the mounter's own validation could matter.
  it should "refuse a cuid that is not a single numeric segment, without calling the mounter" in {
    received.remove("/mount")
    Seq("5/../8", "../..", "/absolute", "", "7x").foreach { cuid =>
      an[IllegalArgumentException] should be thrownBy
        client.mount(nodeIp, port, cuid, "dataset-1", "abc123", "jwt", "http://fs:9092")
    }
    received should not contain key("/mount")
  }

  it should "refuse a repository or commit that is not a single safe segment" in {
    received.remove("/mount")
    Seq("../evil", "a/b", "-o", "", ".hidden/../x").foreach { bad =>
      an[IllegalArgumentException] should be thrownBy
        client.mount(nodeIp, port, "7", bad, "abc123", "jwt", "http://fs:9092")
      an[IllegalArgumentException] should be thrownBy
        client.mount(nodeIp, port, "7", "dataset-1", bad, "jwt", "http://fs:9092")
    }
    received should not contain key("/mount")
  }

  it should "fail loudly when the service-account token is missing" in {
    val withoutToken = new MounterClient("/nonexistent/mounter/token")
    val failure = the[IllegalStateException] thrownBy
      withoutToken.mount(nodeIp, port, "7", "dataset-1", "abc123", "jwt", "http://fs:9092")
    failure.getMessage should include("/nonexistent/mounter/token")
  }
}
