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

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable

/**
  * Exercises [[LakekeeperClient]] end-to-end against an in-process HTTP stub standing in
  * for Lakekeeper (same approach as AsterixDBConnUtilSpec). No network dependency; the
  * stub binds port 0 to pick a free ephemeral port.
  */
class LakekeeperClientSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val mapper = new ObjectMapper()
  private val warehouseId = UUID.randomUUID()

  // Every request in arrival order, as "METHOD path?query" plus the body for POSTs.
  private val requests = mutable.Buffer[String]()
  @volatile private var lastCreateBody: String = ""

  private val server: HttpServer = HttpServer.create(new InetSocketAddress(0), 0)

  private def record(exchange: HttpExchange): String = {
    val query = Option(exchange.getRequestURI.getQuery).map("?" + _).getOrElse("")
    val line = s"${exchange.getRequestMethod} ${exchange.getRequestURI.getPath}$query"
    requests.synchronized { requests += line }
    line
  }

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }

  server.createContext(
    "/management/v1/warehouse",
    (exchange: HttpExchange) => {
      record(exchange)
      if (exchange.getRequestMethod == "POST") {
        lastCreateBody = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
        respond(exchange, 201, s"""{"warehouse-id": "$warehouseId"}""")
      } else {
        respond(exchange, 200, "{}")
      }
    }
  )
  server.createContext(
    s"/catalog/v1/$warehouseId/namespaces",
    (exchange: HttpExchange) => {
      val line = record(exchange)
      (exchange.getRequestMethod, exchange.getRequestURI.getPath) match {
        case ("GET", path) if path.endsWith("/namespaces") =>
          respond(exchange, 200, """{"namespaces": [["operator-port-result"]]}""")
        case ("GET", path) if path.endsWith("/tables") =>
          respond(
            exchange,
            200,
            """{"identifiers": [{"namespace": ["operator-port-result"], "name": "wid_1_eid_2_result"}]}"""
          )
        case ("DELETE", _) =>
          respond(exchange, 204, "")
        case _ =>
          respond(exchange, 500, s"""{"error": "unexpected $line"}""")
      }
    }
  )
  server.start()

  private val client = new LakekeeperClient(
    s"http://localhost:${server.getAddress.getPort}/catalog"
  )

  override protected def beforeEach(): Unit = {
    requests.synchronized { requests.clear() }
    lastCreateBody = ""
  }

  override protected def afterAll(): Unit = server.stop(0)

  "createWarehouse" should "post the Local storage profile and return the assigned id" in {
    client.createWarehouse("user-7-mybucket") shouldBe warehouseId

    val payload = mapper.readTree(lastCreateBody)
    payload.get("warehouse-name").asText() shouldBe "user-7-mybucket"
    val profile = payload.get("storage-profile")
    profile.get("type").asText() shouldBe "s3"
    profile.get("sts-enabled").asBoolean() shouldBe false
    profile.get("path-style-access").asBoolean() shouldBe true
    // Each warehouse owns its own key prefix inside the shared bucket.
    profile.get("key-prefix").asText() shouldBe "user-7-mybucket"
    payload.get("storage-credential").get("credential-type").asText() shouldBe "access-key"
  }

  "deleteWarehouseEmptyFirst" should "purge tables, then namespaces, then the warehouse" in {
    client.deleteWarehouseEmptyFirst(warehouseId)

    val deletes = requests.synchronized { requests.filter(_.startsWith("DELETE")).toList }
    deletes shouldBe List(
      s"DELETE /catalog/v1/$warehouseId/namespaces/operator-port-result/tables/wid_1_eid_2_result?purgeRequested=true",
      s"DELETE /catalog/v1/$warehouseId/namespaces/operator-port-result",
      s"DELETE /management/v1/warehouse/$warehouseId"
    )
  }

  it should "surface a Lakekeeper failure with its status and body" in {
    val badId = UUID.randomUUID()
    // No stub context matches this warehouse's namespace listing → 404 from the server.
    val error = intercept[RuntimeException] {
      client.deleteWarehouseEmptyFirst(badId)
    }
    error.getMessage should include("Lakekeeper")
  }
}
