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

package org.apache.texera.amber.operator.visualization.bulletChart

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class BulletChartStepDefinitionSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Construction — @JsonCreator constructor accepts both fields
  // ---------------------------------------------------------------------------

  "BulletChartStepDefinition" should "store both constructor arguments" in {
    val d = new BulletChartStepDefinition("10", "90")
    assert(d.start == "10")
    assert(d.end == "90")
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow both fields to be reassigned post-construction" in {
    val d = new BulletChartStepDefinition("0", "1")
    d.start = "low"
    d.end = "high"
    assert(d.start == "low")
    assert(d.end == "high")
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip — wire keys are `start` / `end`
  // ---------------------------------------------------------------------------

  "BulletChartStepDefinition JSON round-trip" should
    "serialize start and end under the canonical wire keys" in {
    val d = new BulletChartStepDefinition("alpha", "omega")
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(d))
    assert(tree.has("start"))
    assert(tree.get("start").asText() == "alpha")
    assert(tree.has("end"))
    assert(tree.get("end").asText() == "omega")
  }

  it should "round-trip both fields cleanly" in {
    val d = new BulletChartStepDefinition("33", "66")
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(d),
      classOf[BulletChartStepDefinition]
    )
    assert(restored.start == "33")
    assert(restored.end == "66")
  }

  // ---------------------------------------------------------------------------
  // Annotations — on the @JsonCreator constructor parameters (Scala places
  // annotations on `var` ctor params on the parameter, not the synthesized
  // field, unless `@(JsonProperty @meta.field)` is used).
  // ---------------------------------------------------------------------------

  // Select the @JsonCreator-annotated constructor by its annotation rather than
  // by reflection order (`getDeclaredConstructors.head`), so the test stays
  // deterministic if an auxiliary constructor is ever added.
  private val jsonCreatorCtor =
    classOf[BulletChartStepDefinition].getDeclaredConstructors
      .find(_.isAnnotationPresent(classOf[JsonCreator]))
      .getOrElse(
        fail("expected a @JsonCreator constructor on BulletChartStepDefinition")
      )

  private def ctorParamJsonProperty(paramIndex: Int): JsonProperty = {
    val annotations = jsonCreatorCtor.getParameterAnnotations()(paramIndex)
    annotations.collectFirst { case jp: JsonProperty => jp }.orNull
  }

  "BulletChartStepDefinition ctor param[0] (start)" should "carry @JsonProperty(\"start\")" in {
    val jp = ctorParamJsonProperty(0)
    assert(jp != null)
    assert(jp.value == "start")
  }

  "BulletChartStepDefinition ctor param[1] (end)" should "carry @JsonProperty(\"end\")" in {
    val jp = ctorParamJsonProperty(1)
    assert(jp != null)
    assert(jp.value == "end")
  }

  // ---------------------------------------------------------------------------
  // Instance independence
  // ---------------------------------------------------------------------------

  it should "construct two independent instances (no static state shared)" in {
    val a = new BulletChartStepDefinition("a-start", "a-end")
    val b = new BulletChartStepDefinition("b-start", "b-end")
    a.start = "mutated"
    assert(b.start == "b-start")
  }
}
