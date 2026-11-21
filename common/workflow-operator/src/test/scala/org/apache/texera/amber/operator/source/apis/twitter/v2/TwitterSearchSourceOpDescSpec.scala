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
import org.apache.texera.amber.operator.source.apis.twitter.TwitterSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

// The Twitter source operators are @deprecated (no longer executable) but retained so
// legacy workflows still deserialize; the coverage below pins that backward-compatible contract.
@nowarn("cat=deprecation")
class TwitterSearchSourceOpDescSpec extends AnyFlatSpec with TwitterSourceOpDescSpecSupport {

  "TwitterSearchSourceOpDesc.operatorInfo" should
    "advertise the Twitter Search API source in the External API group" in {
    checkOperatorInfo(new TwitterSearchSourceOpDesc, "Twitter Search API")
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
    checkTweetSchema(new TwitterSearchSourceOpDesc)
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

  "TwitterSearchSourceOpDesc.getPhysicalOp" should
    "wire the TwitterSearch executor as a source op that propagates the tweet schema" in {
    val d = new TwitterSearchSourceOpDesc
    d.searchQuery = "texera"
    d.limit = 50
    checkSourceWiring(d, "TwitterSearchSourceOpExec")
  }
}

@nowarn("cat=deprecation")
private[v2] trait TwitterSourceOpDescSpecSupport extends Matchers {

  protected def checkOperatorInfo(desc: TwitterSourceOpDesc, title: String): Unit = {
    val info = desc.operatorInfo
    info.userFriendlyName shouldBe title
    info.operatorDescription shouldBe s"Retrieve data from $title"
    info.operatorGroupName shouldBe OperatorGroupConstants.API_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  protected def checkTweetSchema(desc: TwitterSourceOpDesc): Unit = {
    val schema = desc.sourceSchema()
    schema.getAttributes should have length 33
    schema.getAttribute("id").getType shouldBe AttributeType.STRING
    schema.getAttribute("created_at").getType shouldBe AttributeType.TIMESTAMP
    schema.getAttribute("retweet_count").getType shouldBe AttributeType.LONG
    schema.getAttribute("user_verified").getType shouldBe AttributeType.BOOLEAN
    schema.getAttribute("like_count").getType shouldBe AttributeType.LONG
    schema.getAttribute("user_protected").getType shouldBe AttributeType.BOOLEAN
  }

  protected def checkSourceWiring(desc: TwitterSourceOpDesc, executorName: String): Unit = {
    val physical = desc.getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe s"org.apache.texera.amber.operator.source.apis.twitter.v2.$executorName"
        descString should include("texera")
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts shouldBe empty
    physical.outputPorts should have size 1

    val propagated = physical.propagateSchema.func(Map.empty)
    propagated.keySet shouldBe desc.operatorInfo.outputPorts.map(_.id).toSet
  }
}
