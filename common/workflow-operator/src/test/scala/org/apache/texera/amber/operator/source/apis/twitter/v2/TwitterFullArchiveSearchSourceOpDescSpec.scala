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

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

// The Twitter source operators are @deprecated (no longer executable) but retained so
// legacy workflows still deserialize; the coverage below pins that backward-compatible contract.
@nowarn("cat=deprecation")
class TwitterFullArchiveSearchSourceOpDescSpec extends AnyFlatSpec with Matchers {

  "TwitterFullArchiveSearchSourceOpDesc.operatorInfo" should
    "advertise the Twitter Full Archive Search API source in the External API group" in {
    val info = (new TwitterFullArchiveSearchSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Twitter Full Archive Search API"
    info.operatorDescription shouldBe "Retrieve data from Twitter Full Archive Search API"
    info.operatorGroupName shouldBe OperatorGroupConstants.API_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "TwitterFullArchiveSearchSourceOpDesc" should
    "default its query/date/credential fields (limit is 0, dates null)" in {
    val d = new TwitterFullArchiveSearchSourceOpDesc
    d.searchQuery shouldBe null
    d.fromDateTime shouldBe null
    d.toDateTime shouldBe null
    d.limit shouldBe 0
    d.apiKey shouldBe null
    d.apiSecretKey shouldBe null
    d.stopWhenRateLimited shouldBe false
    d.APIName shouldBe Some("Full Archive Search")
  }

  "TwitterFullArchiveSearchSourceOpDesc.sourceSchema" should
    "describe the fixed 33-column tweet schema" in {
    val schema = (new TwitterFullArchiveSearchSourceOpDesc).sourceSchema()
    schema.getAttributes should have length 33
    schema.getAttribute("id").getType shouldBe AttributeType.STRING
    schema.getAttribute("created_at").getType shouldBe AttributeType.TIMESTAMP
    schema.getAttribute("retweet_count").getType shouldBe AttributeType.LONG
    schema.getAttribute("user_verified").getType shouldBe AttributeType.BOOLEAN
  }

  "TwitterFullArchiveSearchSourceOpDesc" should
    "round-trip its config fields through the polymorphic base" in {
    val d = new TwitterFullArchiveSearchSourceOpDesc
    d.searchQuery = "texera"
    d.fromDateTime = "2021-04-01T00:00:00Z"
    d.toDateTime = "2021-05-01T00:00:00Z"
    d.limit = 50
    d.apiKey = "k"
    d.apiSecretKey = "s"
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"TwitterFullArchiveSearch\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[TwitterFullArchiveSearchSourceOpDesc]
    val r = restored.asInstanceOf[TwitterFullArchiveSearchSourceOpDesc]
    r.searchQuery shouldBe "texera"
    r.fromDateTime shouldBe "2021-04-01T00:00:00Z"
    r.toDateTime shouldBe "2021-05-01T00:00:00Z"
    r.limit shouldBe 50
    r.apiKey shouldBe "k"
    r.apiSecretKey shouldBe "s"
  }

  "TwitterFullArchiveSearchSourceOpDesc.getPhysicalOp" should
    "wire the TwitterFullArchiveSearch executor as a source op that propagates the tweet schema" in {
    val d = new TwitterFullArchiveSearchSourceOpDesc
    d.searchQuery = "texera"
    d.limit = 50
    val p = d.getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))

    p.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe
          "org.apache.texera.amber.operator.source.apis.twitter.v2.TwitterFullArchiveSearchSourceOpExec"
        descString should include("texera")
      case other => fail(s"expected OpExecWithClassName, got $other")
    }

    p.inputPorts shouldBe empty
    p.outputPorts should have size 1

    // Invoke the schema-propagation closure so its body (not just the wiring) is exercised.
    val propagated = p.propagateSchema.func(Map.empty)
    propagated.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }
}
