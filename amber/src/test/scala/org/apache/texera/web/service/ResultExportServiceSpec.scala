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

import com.fasterxml.jackson.core.JsonProcessingException
import jakarta.ws.rs.core.Response
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.web.model.http.request.result.{OperatorExportInfo, ResultExportRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Unit tests for the ResultExportService request helpers parseOperators and
// validateExportRequest. Constructing the service is cheap (it only stores the
// identity and computing-unit id), so no engine/DB dependency is needed here.
class ResultExportServiceSpec extends AnyFlatSpec with Matchers {

  private val service = new ResultExportService(WorkflowIdentity(1L), computingUnitId = 0)

  private def requestWith(operators: List[OperatorExportInfo]): ResultExportRequest =
    ResultExportRequest(
      exportType = "csv",
      workflowId = 1,
      workflowName = "wf",
      operators = operators,
      datasetIds = List.empty,
      rowIndex = 0,
      columnIndex = 0,
      filename = "",
      computingUnitId = 0
    )

  // -- parseOperators ---------------------------------------------------------

  "parseOperators" should "deserialize a JSON array into OperatorExportInfo objects" in {
    val json =
      """[{"id":"op-1","outputType":"csv"},{"id":"op-2","outputType":"arrow"}]"""
    val parsed = service.parseOperators(json)

    parsed shouldBe List(
      OperatorExportInfo("op-1", "csv"),
      OperatorExportInfo("op-2", "arrow")
    )
  }

  it should "round-trip an empty JSON array to an empty list" in {
    service.parseOperators("[]") shouldBe List.empty[OperatorExportInfo]
  }

  it should "throw when given a malformed JSON string" in {
    a[JsonProcessingException] should be thrownBy service.parseOperators("not json")
  }

  // -- validateExportRequest --------------------------------------------------

  "validateExportRequest" should "return a 400 response when no operators are selected" in {
    val result = service.validateExportRequest(requestWith(List.empty))

    val response = result.getOrElse(fail("expected a validation error response"))
    response.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
    response.getEntity match {
      case m: java.util.Map[_, _] => m.get("error") shouldBe "No operator selected"
      case other                  => fail(s"unexpected entity: $other")
    }
  }

  it should "return None when at least one operator is selected" in {
    val result =
      service.validateExportRequest(requestWith(List(OperatorExportInfo("op-1", "csv"))))
    result shouldBe None
  }
}
