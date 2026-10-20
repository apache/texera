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

package org.apache.texera.web.model.websocket.event

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.web.model.websocket.request.ResultPaginationRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
  * Pins the request -> event projection that answers a pagination frame.
  *
  * `ExecutionResultService.handleResultPagination` builds its reply through
  * `PaginatedResultEvent.apply(request, mappedResults, attributes)` on both branches
  * (the storage-backed one and the empty-storage fallback), so this companion `apply`
  * is the only place where a `ResultPaginationRequest` is turned into the frame the
  * result panel receives.
  *
  * Two things can silently break here and neither is a compile error:
  *   1. `requestID` and `operatorID` are both `String` and adjacent in both the request
  *      and the event, so swapping them type-checks. The frontend correlates the reply
  *      to the pending request by `requestID`; a swap would make every page load hang.
  *      The fixtures below use distinct values so a transposition fails.
  *   2. The request carries four Ints (`pageIndex`, `pageSize`, `columnOffset`,
  *      `columnLimit`) and only `pageIndex` is projected. Passing `pageSize` instead
  *      type-checks too, so the fixture keeps `pageIndex` (3) far from `pageSize` (25).
  *
  * The emitted field set is pinned as well: the Angular `PaginatedResultEvent` declares
  * exactly `requestID`, `operatorID`, `pageIndex`, `table` and `schema`, so none of the
  * request's column/pagination bookkeeping may leak onto the wire.
  */
class PaginatedResultEventSpec extends AnyFlatSpec with Matchers {

  // pageIndex(3) != pageSize(25), requestID != operatorID, and the column fields carry
  // values that must NOT show up anywhere in the projected event.
  private val request = ResultPaginationRequest(
    requestID = "req-alpha",
    operatorID = "op-beta",
    pageIndex = 3,
    pageSize = 25,
    columnOffset = 4,
    columnLimit = 6,
    columnSearch = Some("city")
  )

  private val table = List(
    objectMapper.createObjectNode().put("city", "Irvine"),
    objectMapper.createObjectNode().put("city", "Anaheim")
  )

  private val schema = List(
    new Attribute("city", AttributeType.STRING),
    new Attribute("population", AttributeType.INTEGER)
  )

  "PaginatedResultEvent.apply" should "project requestID, operatorID and pageIndex" in {
    val event = PaginatedResultEvent(request, table, schema)
    event.requestID shouldBe "req-alpha"
    event.operatorID shouldBe "op-beta"
    event.pageIndex shouldBe 3
  }

  it should "pass the table and schema through untouched" in {
    val event = PaginatedResultEvent(request, table, schema)
    event.table should contain theSameElementsInOrderAs table
    event.schema should contain theSameElementsInOrderAs schema
  }

  "the projected event" should "serialize to exactly the fields the Angular client declares" in {
    val json = objectMapper.readTree(
      objectMapper.writeValueAsString(PaginatedResultEvent(request, table, schema))
    )
    json.fieldNames().asScala.toSet shouldBe
      Set("type", "requestID", "operatorID", "pageIndex", "table", "schema")
    json.get("type").asText() shouldBe "PaginatedResultEvent"
    json.get("requestID").asText() shouldBe "req-alpha"
    json.get("operatorID").asText() shouldBe "op-beta"
    json.get("pageIndex").asInt() shouldBe 3
    json.get("table").get(0).get("city").asText() shouldBe "Irvine"
    json.get("schema").get(1).get("attributeName").asText() shouldBe "population"
    json.get("schema").get(1).get("attributeType").asText() shouldBe "integer"
  }
}
