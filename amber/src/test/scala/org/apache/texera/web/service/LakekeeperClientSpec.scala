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
import org.apache.texera.web.service.LakekeeperClient.PurgeWaitPolicy
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

  // How many warehouse deletes have reached the stub for this id, including the one
  // being served: `record` logs every request before the handler dispatches on it.
  private def deleteAttempts(id: UUID): Int =
    requests.synchronized { requests.count(_ == s"DELETE /management/v1/warehouse/$id") }

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }

  // Lakekeeper purges dropped tables asynchronously (queue `tabular_purge`), and
  // answers a warehouse delete with 409 WarehouseHasUnfinishedTasks while any
  // purge task is pending (#7742). These stub warehouses model that queue:
  // `racing` drains after two attempts, `alwaysBusy` never drains, and
  // `otherConflict` 409s for an unrelated reason (which must NOT be retried).
  private val racingWarehouseId = UUID.randomUUID()
  private val alwaysBusyWarehouseId = UUID.randomUUID()
  private val otherConflictWarehouseId = UUID.randomUUID()
  private val malformedConflictWarehouseId = UUID.randomUUID()
  private val unfinishedTasksBody =
    """{"error":{"message":"Warehouse has unfinished tasks. Cannot delete warehouse until all tasks are finished.","type":"WarehouseHasUnfinishedTasks","code":409}}"""

  server.createContext(
    "/management/v1/warehouse",
    (exchange: HttpExchange) => {
      record(exchange)
      if (exchange.getRequestMethod == "POST") {
        lastCreateBody = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
        respond(exchange, 201, s"""{"warehouse-id": "$warehouseId"}""")
      } else if (exchange.getRequestMethod == "DELETE") {
        val path = exchange.getRequestURI.getPath
        if (path.endsWith(racingWarehouseId.toString)) {
          if (deleteAttempts(racingWarehouseId) <= 2) respond(exchange, 409, unfinishedTasksBody)
          else respond(exchange, 204, "")
        } else if (path.endsWith(alwaysBusyWarehouseId.toString)) {
          respond(exchange, 409, unfinishedTasksBody)
        } else if (path.endsWith(malformedConflictWarehouseId.toString)) {
          // A 409 whose body isn't the JSON envelope the type check reads.
          respond(exchange, 409, "<html>gateway conflict</html>")
        } else if (path.endsWith(otherConflictWarehouseId.toString)) {
          respond(
            exchange,
            409,
            """{"error":{"message":"warehouse is in use","type":"Conflict","code":409}}"""
          )
        } else {
          respond(exchange, 200, "{}")
        }
      } else {
        respond(exchange, 200, "{}")
      }
    }
  )
  server.createContext(
    s"/catalog/v1/$warehouseId/namespaces",
    (exchange: HttpExchange) => {
      val line = record(exchange)
      val query = Option(exchange.getRequestURI.getQuery).getOrElse("")
      (exchange.getRequestMethod, exchange.getRequestURI.getPath) match {
        case ("GET", path) if path.endsWith("/namespaces") =>
          respond(exchange, 200, """{"namespaces": [["operator-port-result"]]}""")
        // The tables listing is served in TWO pages so the client's next-page-token
        // loop is pinned: missing the second page would leave the warehouse non-empty.
        case ("GET", path) if path.endsWith("/tables") && !query.contains("pageToken") =>
          respond(
            exchange,
            200,
            """{"identifiers": [{"namespace": ["operator-port-result"], "name": "wid_1_eid_2_result"}],
               |"next-page-token": "page-2"}""".stripMargin
          )
        case ("GET", path) if path.endsWith("/tables") =>
          respond(
            exchange,
            200,
            """{"identifiers": [{"namespace": ["operator-port-result"], "name": "wid_1_eid_3_result"}]}"""
          )
        case ("DELETE", _) =>
          respond(exchange, 204, "")
        case _ =>
          respond(exchange, 500, s"""{"error": "unexpected $line"}""")
      }
    }
  )
  private val erroringWarehouseId = UUID.randomUUID()
  server.createContext(
    s"/catalog/v1/$erroringWarehouseId/namespaces",
    (exchange: HttpExchange) => {
      record(exchange)
      respond(exchange, 500, """{"error": "internal"}""")
    }
  )
  server.start()

  private val client = new LakekeeperClient(
    s"http://localhost:${server.getAddress.getPort}/catalog"
  )

  // Zero retry delay keeps the spec free of real sleeps (deterministic); 3
  // retries keeps the exhaustion case cheap to assert.
  private val retryClient = new LakekeeperClient(
    s"http://localhost:${server.getAddress.getPort}/catalog",
    PurgeWaitPolicy(retries = 3, initialDelayMillis = 0)
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

  "deleteWarehouseEmptyFirst" should "purge every page of tables, then namespaces, then the warehouse" in {
    client.deleteWarehouseEmptyFirst(warehouseId)

    val deletes = requests.synchronized { requests.filter(_.startsWith("DELETE")).toList }
    deletes shouldBe List(
      s"DELETE /catalog/v1/$warehouseId/namespaces/operator-port-result/tables/wid_1_eid_2_result?purgeRequested=true",
      s"DELETE /catalog/v1/$warehouseId/namespaces/operator-port-result/tables/wid_1_eid_3_result?purgeRequested=true",
      s"DELETE /catalog/v1/$warehouseId/namespaces/operator-port-result",
      s"DELETE /management/v1/warehouse/$warehouseId"
    )
  }

  it should "treat an already-gone warehouse as deleted (404s tolerated, idempotent)" in {
    val goneId = UUID.randomUUID()
    // No stub context matches this warehouse → every request 404s. A retry after a
    // partial failure must heal rather than wedge on \"not found\".
    noException should be thrownBy client.deleteWarehouseEmptyFirst(goneId)
  }

  it should "surface a Lakekeeper failure with its status and body" in {
    val error = intercept[RuntimeException] {
      client.deleteWarehouseEmptyFirst(erroringWarehouseId)
    }
    error.getMessage should include("Lakekeeper")
    error.getMessage should include("500")
  }

  it should "wait out 409 WarehouseHasUnfinishedTasks from the asynchronous purge (#7742)" in {
    // Lakekeeper purges dropped tables asynchronously; the stub answers the
    // warehouse delete with 409 WarehouseHasUnfinishedTasks twice before the
    // queue "drains" and it returns 204. The delete must ride that out.
    noException should be thrownBy retryClient.deleteWarehouseEmptyFirst(racingWarehouseId)
    deleteAttempts(racingWarehouseId) shouldBe 3
  }

  it should "give up once the purge-wait retries are exhausted" in {
    val error = intercept[RuntimeException] {
      retryClient.deleteWarehouseEmptyFirst(alwaysBusyWarehouseId)
    }
    error.getMessage should include("409")
    error.getMessage should include("WarehouseHasUnfinishedTasks")
    // 1 initial attempt + 3 retries, then fail -- the wait is bounded.
    deleteAttempts(alwaysBusyWarehouseId) shouldBe 4
  }

  it should "fail immediately on a 409 whose body is not the expected JSON envelope" in {
    // The type check parses the body; a malformed one must read as "not the
    // purge conflict" and fail rather than be retried as if it were transient.
    val error = intercept[RuntimeException] {
      retryClient.deleteWarehouseEmptyFirst(malformedConflictWarehouseId)
    }
    error.getMessage should include("409")
    deleteAttempts(malformedConflictWarehouseId) shouldBe 1
  }

  it should "fail immediately on a 409 that is not WarehouseHasUnfinishedTasks" in {
    val error = intercept[RuntimeException] {
      retryClient.deleteWarehouseEmptyFirst(otherConflictWarehouseId)
    }
    error.getMessage should include("409")
    deleteAttempts(otherConflictWarehouseId) shouldBe 1
  }
}
