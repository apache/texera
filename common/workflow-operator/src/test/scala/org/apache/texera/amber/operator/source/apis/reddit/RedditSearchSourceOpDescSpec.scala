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

package org.apache.texera.amber.operator.source.apis.reddit

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RedditSearchSourceOpDescSpec extends AnyFlatSpec with Matchers {

  "RedditSearchSourceOpDesc.operatorInfo" should
    "advertise the Reddit Search source in the External API group" in {
    val info = (new RedditSearchSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Reddit Search"
    info.operatorDescription shouldBe "Search for recent posts with python-wrapped Reddit API, PRAW"
    info.operatorGroupName shouldBe OperatorGroupConstants.API_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "RedditSearchSourceOpDesc" should "be a source and default its fields (limit is 100)" in {
    val d = new RedditSearchSourceOpDesc
    d.asSource() shouldBe true
    d.limit.intValue shouldBe 100
    d.clientId shouldBe null
    d.clientSecret shouldBe null
    d.query shouldBe null
    d.sorting shouldBe null
  }

  "RedditSearchSourceOpDesc.sourceSchema" should "describe the fixed 17-column post schema" in {
    val schema = (new RedditSearchSourceOpDesc).sourceSchema()
    schema.getAttributes should have length 17
    schema.getAttribute("id").getType shouldBe AttributeType.STRING
    schema.getAttribute("created_utc").getType shouldBe AttributeType.TIMESTAMP
    schema.getAttribute("is_self").getType shouldBe AttributeType.BOOLEAN
    schema.getAttribute("score").getType shouldBe AttributeType.INTEGER
    schema.getAttribute("upvote_ratio").getType shouldBe AttributeType.DOUBLE
  }

  "RedditSearchSourceOpDesc.getOutputSchemas" should
    "expose the source schema keyed by the declared output port" in {
    val d = new RedditSearchSourceOpDesc
    val out = d.getOutputSchemas(Map.empty)
    out(d.operatorInfo.outputPorts.head.id).getAttributes should have length 17
  }

  "RedditSearchSourceOpDesc.generatePythonCode" should
    "emit the PRAW source operator honoring the sorting method" in {
    val d = new RedditSearchSourceOpDesc
    d.clientId = "id"
    d.clientSecret = "secret"
    d.query = "texera"
    d.sorting = RedditSourceOperatorFunction.Hot
    val code = d.generatePythonCode()
    code should include("import praw")
    code should include("class ProcessTupleOperator(UDFSourceOperator)")
    code should include("sorting = 'hot'")
    code should include("subreddit('all').search")
  }

  it should "embed runtime ValueError guards for the required fields" in {
    val d = new RedditSearchSourceOpDesc
    d.clientId = "id"
    d.clientSecret = "secret"
    d.query = "texera"
    d.sorting = RedditSourceOperatorFunction.Hot
    val code = d.generatePythonCode()
    code should include("raise ValueError('Client Id cannot be None.')")
    code should include("raise ValueError('Client Secret cannot be None.')")
    code should include("raise ValueError('Query cannot be None.')")
  }

  "RedditSearchSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new RedditSearchSourceOpDesc
    d.clientId = "id"
    d.clientSecret = "secret"
    d.query = "texera"
    d.limit = 50
    d.sorting = RedditSourceOperatorFunction.New
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"RedditSearch\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[RedditSearchSourceOpDesc]
    val r = restored.asInstanceOf[RedditSearchSourceOpDesc]
    r.clientId shouldBe "id"
    r.clientSecret shouldBe "secret"
    r.query shouldBe "texera"
    r.limit.intValue shouldBe 50
    r.sorting shouldBe RedditSourceOperatorFunction.New
  }
}
