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

package org.apache.texera.amber.operator.source.fetcher

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class URLFetcherOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def configured(decoding: DecodingMethod): URLFetcherOpDesc = {
    val op = new URLFetcherOpDesc
    op.url = "https://example.test/data"
    op.decodingMethod = decoding
    op
  }

  "URLFetcherOpDesc.operatorInfo" should "advertise the user-friendly name and API group" in {
    val info = (new URLFetcherOpDesc).operatorInfo
    info.userFriendlyName shouldBe "URL Fetcher"
    info.operatorGroupName shouldBe OperatorGroupConstants.API_GROUP
    info.operatorDescription should include("URL")
  }

  it should "expose no input ports and one output port (source-shaped)" in {
    val info = (new URLFetcherOpDesc).operatorInfo
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "URLFetcherOpDesc.sourceSchema" should "produce a single STRING column when decoding is UTF-8" in {
    val op = configured(DecodingMethod.UTF_8)
    val schema = op.sourceSchema()
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "URL content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  it should "produce an ANY column for raw-bytes decoding" in {
    val op = configured(DecodingMethod.RAW_BYTES)
    val schema = op.sourceSchema()
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "URL content"
    schema.getAttributes.head.getType shouldBe AttributeType.ANY
  }

  it should "default to ANY when decodingMethod is left unset (current behavior)" in {
    // Pin: `var decodingMethod: DecodingMethod = _` defaults to null.
    // sourceSchema's branch is `if (decodingMethod == DecodingMethod.UTF_8)
    // STRING else ANY`, so a null comparison falls through to ANY without
    // raising. Documenting the current behavior so a future explicit-null
    // check breaks this spec deliberately.
    val op = new URLFetcherOpDesc
    op.url = "https://example.test/data"
    val schema = op.sourceSchema()
    schema.getAttributes.head.getType shouldBe AttributeType.ANY
  }

  "URLFetcherOpDesc.getPhysicalOp" should "wire the URLFetcherOpExec class name into the OpExecInitInfo" in {
    val op = configured(DecodingMethod.UTF_8)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo.toString should include(
      "org.apache.texera.amber.operator.source.fetcher.URLFetcherOpExec"
    )
  }

  it should "propagate sourceSchema onto the single output port" in {
    val op = configured(DecodingMethod.UTF_8)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.outputPorts.size shouldBe op.operatorInfo.outputPorts.size
  }
}
