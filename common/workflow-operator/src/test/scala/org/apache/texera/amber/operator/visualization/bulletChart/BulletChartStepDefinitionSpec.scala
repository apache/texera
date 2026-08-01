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
    val d = new BulletChartStepDefinition(Some(10), Some(90))
    assert(d.start.contains(10))
    assert(d.end.contains(90))
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow both fields to be reassigned post-construction" in {
    val d = new BulletChartStepDefinition(Some(0), Some(1))
    d.start = Some(2.5)
    d.end = Some(7.5)
    assert(d.start.contains(2.5))
    assert(d.end.contains(7.5))
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip — wire keys are `start` / `end`
  // ---------------------------------------------------------------------------

  "BulletChartStepDefinition JSON round-trip" should
    "serialize start and end as numbers under the canonical wire keys" in {
    val d = new BulletChartStepDefinition(Some(1.5), Some(9.5))
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(d))
    assert(tree.has("start"))
    assert(tree.get("start").isNumber)
    assert(tree.get("start").asDouble() == 1.5)
    assert(tree.has("end"))
    assert(tree.get("end").isNumber)
    assert(tree.get("end").asDouble() == 9.5)
  }

  it should "round-trip both fields cleanly" in {
    val d = new BulletChartStepDefinition(Some(33), Some(66))
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(d),
      classOf[BulletChartStepDefinition]
    )
    assert(restored.start.contains(33))
    assert(restored.end.contains(66))
  }

  /** `Option[Double]` erases its element type, so Jackson needs
    * `@JsonDeserialize(contentAs = ...)` on each constructor parameter to know what
    * to build. Without it a JSON string is left inside the Option unconverted and
    * the first arithmetic use throws ClassCastException — which a round trip cannot
    * catch, since it writes a number back.
    */
  private def read(json: String): BulletChartStepDefinition =
    objectMapper.readValue(json, classOf[BulletChartStepDefinition])

  "BulletChartStepDefinition bounds" should "deserialize JSON numbers" in {
    val d = read("""{"start":1.5,"end":9.5}""")
    assert(d.start.contains(1.5))
    assert(d.end.contains(9.5))
  }

  it should "deserialize the numeric strings a workflow saved before the bounds were numeric" in {
    val d = read("""{"start":"1.5","end":"9.5"}""")
    assert(d.start.contains(1.5))
    assert(d.end.contains(9.5))
  }

  it should "read absent, null and blank bounds as unset rather than as zero" in {
    assert(read("""{}""").start.isEmpty)
    assert(read("""{"start":null}""").start.isEmpty)
    assert(read("""{"start":""}""").start.isEmpty)
  }

  it should "hold a Double, not the raw JSON value" in {
    assert(read("""{"start":"1.5"}""").start.map(_ * 2).contains(3.0))
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
    val a = new BulletChartStepDefinition(Some(1), Some(2))
    val b = new BulletChartStepDefinition(Some(3), Some(4))
    a.start = Some(99)
    assert(b.start.contains(3))
  }
}
