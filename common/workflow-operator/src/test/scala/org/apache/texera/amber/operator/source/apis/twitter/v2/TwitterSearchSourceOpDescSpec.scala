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

package org.apache.texera.amber.operator.source.apis.twitter.v2

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

// The Twitter source operators are @deprecated (no longer executable) but retained so
// legacy workflows still deserialize; the coverage below pins that backward-compatible contract.
@nowarn("cat=deprecation")
class TwitterSearchSourceOpDescSpec extends AnyFlatSpec with Matchers {

  "TwitterSearchSourceOpDesc.operatorInfo" should
    "advertise the Twitter Search API source in the External API group" in {
    val info = (new TwitterSearchSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Twitter Search API"
    info.operatorDescription shouldBe "Retrieve data from Twitter Search API"
    info.operatorGroupName shouldBe OperatorGroupConstants.API_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "TwitterSearchSourceOpDesc" should "default its query/credential fields (limit is 0, not 100)" in {
    val d = new TwitterSearchSourceOpDesc
    d.searchQuery shouldBe null
    d.limit shouldBe 0
    d.apiKey shouldBe null
    d.apiSecretKey shouldBe null
    d.stopWhenRateLimited shouldBe false
    d.APIName shouldBe Some("Search")
  }

  "TwitterSearchSourceOpDesc.sourceSchema" should
    "describe the fixed 33-column tweet schema" in {
    val schema = (new TwitterSearchSourceOpDesc).sourceSchema()
    schema.getAttributes should have length 33
    schema.getAttribute("id").getType shouldBe AttributeType.STRING
    schema.getAttribute("created_at").getType shouldBe AttributeType.TIMESTAMP
    schema.getAttribute("like_count").getType shouldBe AttributeType.LONG
    schema.getAttribute("user_protected").getType shouldBe AttributeType.BOOLEAN
  }

  "TwitterSearchSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new TwitterSearchSourceOpDesc
    d.searchQuery = "texera"
    d.limit = 50
    d.apiKey = "k"
    d.apiSecretKey = "s"
    d.stopWhenRateLimited = true
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"TwitterSearch\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[TwitterSearchSourceOpDesc]
    val r = restored.asInstanceOf[TwitterSearchSourceOpDesc]
    r.searchQuery shouldBe "texera"
    r.limit shouldBe 50
    r.apiKey shouldBe "k"
    r.apiSecretKey shouldBe "s"
    r.stopWhenRateLimited shouldBe true
  }
}
