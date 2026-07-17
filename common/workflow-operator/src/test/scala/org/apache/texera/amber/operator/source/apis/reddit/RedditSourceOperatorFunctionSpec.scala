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

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RedditSourceOperatorFunctionSpec extends AnyFlatSpec with Matchers {

  "RedditSourceOperatorFunction" should "map each constant to its wire value" in {
    RedditSourceOperatorFunction.None.getName shouldBe "none"
    RedditSourceOperatorFunction.Controversial.getName shouldBe "controversial"
    RedditSourceOperatorFunction.Gilded.getName shouldBe "gilded"
    RedditSourceOperatorFunction.Hot.getName shouldBe "hot"
    RedditSourceOperatorFunction.New.getName shouldBe "new"
    RedditSourceOperatorFunction.Rising.getName shouldBe "rising"
    RedditSourceOperatorFunction.Top.getName shouldBe "top"
    RedditSourceOperatorFunction.values() should have length 7
  }

  "RedditSourceOperatorFunction" should "round-trip through Jackson using its wire value" in {
    objectMapper.writeValueAsString(RedditSourceOperatorFunction.Hot) shouldBe "\"hot\""
    objectMapper.readValue(
      "\"hot\"",
      classOf[RedditSourceOperatorFunction]
    ) shouldBe RedditSourceOperatorFunction.Hot
  }
}
