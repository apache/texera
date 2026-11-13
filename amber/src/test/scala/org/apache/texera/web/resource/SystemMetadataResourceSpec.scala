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

package org.apache.texera.web.resource

import org.apache.texera.amber.operator.metadata.{
  AllOperatorMetadata,
  OperatorGroupConstants,
  OperatorMetadataGenerator
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * `GET /api/resources/operator-metadata` is the first call the workspace makes: the palette, the
  * property editor and the link validator are all built from this one payload, so an endpoint that
  * answered with a trimmed or rebuilt catalogue would leave the editor with operators it cannot
  * place or forms it cannot render.
  *
  * The resource only delegates, and the delegation is what these tests pin — that a caller receives
  * the generated catalogue whole, operators and palette groups alike. How the catalogue itself is
  * derived (JSON-schema generation, port declarations) belongs to `OperatorMetadataGenerator` and is
  * not re-asserted here.
  *
  * `TexeraWebApplicationSpec` separately asserts that this resource is registered on Jersey, but
  * nothing there calls it, so until now nothing has run the method behind the endpoint.
  */
class SystemMetadataResourceSpec extends AnyFlatSpec with Matchers {

  private val response: AllOperatorMetadata = new SystemMetadataResource().getOperatorMetadata

  behavior of "SystemMetadataResource"

  it should "hand back an entry for every operator the workspace can place" in {
    // Every `LogicalOp` subtype registered for JSON polymorphism is an operator a saved workflow may
    // reference, so one missing entry is an operator the editor cannot render.
    response.operators.map(_.operatorType) should contain theSameElementsAs
      OperatorMetadataGenerator.operatorTypeMap.values.toList
    // The comparison above says the two agree, not that either holds anything: these two operators
    // are always registered, so they also rule out an agreeing-but-empty catalogue.
    response.operators.map(_.operatorType) should contain allOf ("CSVFileScan", "Filter")
  }

  it should "hand back the palette's group order" in {
    // The palette renders its sections in this order. The list is declared, not derived from the
    // operators, so the endpoint has to pass it through rather than reconstruct it.
    response.groups shouldBe OperatorGroupConstants.OperatorGroupOrderList
    response.groups should not be empty
  }
}
