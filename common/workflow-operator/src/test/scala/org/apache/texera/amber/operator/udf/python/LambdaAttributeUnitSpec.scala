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

package org.apache.texera.amber.operator.udf.python

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LambdaAttributeUnitSpec extends AnyFlatSpec with Matchers {

  "LambdaAttributeUnit" should "expose its constructor-supplied fields" in {
    val u = new LambdaAttributeUnit("score", "1 + 1", "scoreOut", AttributeType.INTEGER)
    u.attributeName shouldBe "score"
    u.expression shouldBe "1 + 1"
    u.newAttributeName shouldBe "scoreOut"
    u.attributeType shouldBe AttributeType.INTEGER
  }

  "LambdaAttributeUnit" should "honor equals/hashCode by all four fields" in {
    val a = new LambdaAttributeUnit("score", "1 + 1", "scoreOut", AttributeType.INTEGER)
    val b = new LambdaAttributeUnit("score", "1 + 1", "scoreOut", AttributeType.INTEGER)
    a shouldBe b
    a.hashCode shouldBe b.hashCode
    a should not be new LambdaAttributeUnit("other", "1 + 1", "scoreOut", AttributeType.INTEGER)
    a should not be new LambdaAttributeUnit("score", "1 + 2", "scoreOut", AttributeType.INTEGER)
    a should not be new LambdaAttributeUnit("score", "1 + 1", "renamed", AttributeType.INTEGER)
    a should not be new LambdaAttributeUnit("score", "1 + 1", "scoreOut", AttributeType.DOUBLE)
  }

  "LambdaAttributeUnit" should "round-trip through Jackson" in {
    val u = new LambdaAttributeUnit("score", "1 + 1", "scoreOut", AttributeType.INTEGER)
    val json = objectMapper.writeValueAsString(u)
    val node = objectMapper.readTree(json)
    node.get("attributeName").asText shouldBe "score"
    node.get("expression").asText shouldBe "1 + 1"
    node.get("newAttributeName").asText shouldBe "scoreOut"
    node.get("attributeType").asText shouldBe "integer"
    objectMapper.readValue(json, classOf[LambdaAttributeUnit]) shouldBe u
  }
}
