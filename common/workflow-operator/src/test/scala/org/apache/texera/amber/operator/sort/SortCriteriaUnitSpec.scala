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

package org.apache.texera.amber.operator.sort

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SortCriteriaUnitSpec extends AnyFlatSpec with Matchers {

  "SortCriteriaUnit" should "deserialize an ASC criterion (wire-key 'attribute')" in {
    val u = objectMapper.readValue(
      """{"attribute":"col","sortPreference":"ASC"}""",
      classOf[SortCriteriaUnit]
    )
    u.attributeName shouldBe "col"
    u.sortPreference shouldBe SortPreference.ASC
  }

  it should "deserialize a DESC criterion" in {
    val u = objectMapper.readValue(
      """{"attribute":"age","sortPreference":"DESC"}""",
      classOf[SortCriteriaUnit]
    )
    u.attributeName shouldBe "age"
    u.sortPreference shouldBe SortPreference.DESC
  }

  "SortCriteriaUnit JSON" should
    "serialize attributeName under the wire-key 'attribute' (not 'attributeName')" in {
    val u = new SortCriteriaUnit
    u.attributeName = "city"
    u.sortPreference = SortPreference.ASC
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(u))
    tree.has("attribute") shouldBe true
    tree.get("attribute").asText shouldBe "city"
    tree.has("attributeName") shouldBe false
    tree.get("sortPreference").asText shouldBe "ASC"
  }

  it should "round-trip both fields" in {
    val u = new SortCriteriaUnit
    u.attributeName = "score"
    u.sortPreference = SortPreference.DESC
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(u), classOf[SortCriteriaUnit])
    restored.attributeName shouldBe "score"
    restored.sortPreference shouldBe SortPreference.DESC
  }

  "SortCriteriaUnit#attributeName" should "carry @JsonProperty(\"attribute\", required = true)" in {
    val jp = classOf[SortCriteriaUnit]
      .getDeclaredField("attributeName")
      .getAnnotation(classOf[JsonProperty])
    jp should not be null
    jp.value shouldBe "attribute"
    jp.required shouldBe true
  }

  "SortCriteriaUnit#sortPreference" should
    "carry @JsonProperty(\"sortPreference\", required = true)" in {
    val jp = classOf[SortCriteriaUnit]
      .getDeclaredField("sortPreference")
      .getAnnotation(classOf[JsonProperty])
    jp should not be null
    jp.value shouldBe "sortPreference"
    jp.required shouldBe true
  }
}
