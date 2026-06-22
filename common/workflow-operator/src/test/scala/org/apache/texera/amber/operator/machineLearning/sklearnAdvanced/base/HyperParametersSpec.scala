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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HyperParametersSpec extends AnyFlatSpec with Matchers {

  "HyperParameters" should
    "default parameter/attribute/value to null and parametersSource to false" in {
    val h = new HyperParameters[String]
    h.parameter shouldBe null
    h.attribute shouldBe null
    h.value shouldBe null
    h.parametersSource shouldBe false
  }

  it should "allow all fields to be assigned post-construction" in {
    val h = new HyperParameters[String]
    h.parameter = "alpha"
    h.attribute = "colA"
    h.value = "0.5"
    h.parametersSource = true
    h.parameter shouldBe "alpha"
    h.attribute shouldBe "colA"
    h.value shouldBe "0.5"
    h.parametersSource shouldBe true
  }

  "HyperParameters" should "serialize attribute and value under their wire-keys" in {
    val h = new HyperParameters[String]
    h.attribute = "colA"
    h.value = "0.5"
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(h))
    tree.get("attribute").asText shouldBe "colA"
    tree.get("value").asText shouldBe "0.5"
  }

  "HyperParameters JSON" should "omit null fields (Include.NON_NULL) for a fresh instance" in {
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(new HyperParameters[String]))
    tree.has("parameter") shouldBe false
    tree.has("attribute") shouldBe false
    tree.has("value") shouldBe false
    tree.has("parametersSource") shouldBe true
  }

  it should "round-trip populated fields" in {
    val h = new HyperParameters[String]
    h.parameter = "alpha"
    h.attribute = "colA"
    h.value = "0.5"
    h.parametersSource = true
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(h), classOf[HyperParameters[String]])
    restored.parameter shouldBe "alpha"
    restored.attribute shouldBe "colA"
    restored.value shouldBe "0.5"
    restored.parametersSource shouldBe true
  }
}
