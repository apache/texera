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

package org.apache.texera.amber.operator.projection

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AttributeUnitSpec extends AnyFlatSpec with Matchers {

  "AttributeUnit.getAlias" should "return the alias when one is provided" in {
    new AttributeUnit("col", "renamed").getAlias shouldBe "renamed"
    new AttributeUnit("col", "renamed").getOriginalAttribute shouldBe "col"
  }

  it should "fall back to the original attribute when the alias is blank" in {
    new AttributeUnit("col", "").getAlias shouldBe "col"
    new AttributeUnit("col", "   ").getAlias shouldBe "col"
    new AttributeUnit("col", null).getAlias shouldBe "col"
  }

  "AttributeUnit" should "honor equals/hashCode by original attribute and alias" in {
    val a = new AttributeUnit("col", "x")
    val b = new AttributeUnit("col", "x")
    a shouldBe b
    a.hashCode shouldBe b.hashCode
    a should not be new AttributeUnit("col", "y") // differs by alias
    a should not be new AttributeUnit("other", "x") // differs by original attribute
  }

  "AttributeUnit" should "round-trip through Jackson" in {
    val u = new AttributeUnit("col", "renamed")
    val node = objectMapper.readTree(objectMapper.writeValueAsString(u))
    node.get("originalAttribute").asText shouldBe "col"
    node.get("alias").asText shouldBe "renamed"
    objectMapper.readValue(objectMapper.writeValueAsString(u), classOf[AttributeUnit]) shouldBe u
  }
}
