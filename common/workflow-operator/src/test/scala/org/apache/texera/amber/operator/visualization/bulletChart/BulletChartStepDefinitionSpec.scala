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
import org.apache.texera.amber.operator.visualization.ChartConfigSpecSupport
import org.scalatest.flatspec.AnyFlatSpec

class BulletChartStepDefinitionSpec extends AnyFlatSpec with ChartConfigSpecSupport {

  "BulletChartStepDefinition" should "store both constructor arguments" in {
    val d = new BulletChartStepDefinition(Some(10), Some(90))
    assert(d.start.contains(10) && d.end.contains(90))
  }

  it should "allow both fields to be reassigned post-construction" in {
    val d = new BulletChartStepDefinition(Some(0), Some(1))
    d.start = Some(2.5)
    d.end = Some(7.5)
    assert(d.start.contains(2.5) && d.end.contains(7.5))
  }

  "BulletChartStepDefinition JSON round-trip" should
    "serialize start and end as numbers under the canonical wire keys" in {
    assertStepWireKeys(new BulletChartStepDefinition(Some(1.5), Some(9.5)), 1.5, 9.5)
  }

  it should "round-trip both fields cleanly" in {
    val restored = checkRoundTrip(
      new BulletChartStepDefinition(Some(33), Some(66)),
      classOf[BulletChartStepDefinition]
    )
    assert(restored.start.contains(33) && restored.end.contains(66))
  }

  "BulletChartStepDefinition bounds" should "deserialize JSON numbers" in {
    assertBoundsPair[BulletChartStepDefinition](
      classOf[BulletChartStepDefinition],
      """{"start":1.5,"end":9.5}""",
      d => (d.start, d.end),
      1.5,
      9.5
    )
  }

  it should "deserialize the numeric strings a workflow saved before the bounds were numeric" in {
    assertBoundsPair[BulletChartStepDefinition](
      classOf[BulletChartStepDefinition],
      """{"start":"1.5","end":"9.5"}""",
      d => (d.start, d.end),
      1.5,
      9.5
    )
  }

  it should "read absent, null and blank bounds as unset rather than as zero" in {
    assertUnsetBounds[BulletChartStepDefinition](classOf[BulletChartStepDefinition], _.start)
  }

  it should "hold a Double, not the raw JSON value" in {
    assertDoubleBoundsConversion[BulletChartStepDefinition](
      classOf[BulletChartStepDefinition],
      _.start
    )
  }

  private val jsonCreatorCtor =
    classOf[BulletChartStepDefinition].getDeclaredConstructors
      .find(_.isAnnotationPresent(classOf[JsonCreator]))
      .getOrElse(fail("expected a @JsonCreator constructor on BulletChartStepDefinition"))

  private def ctorParamJsonProperty(paramIndex: Int): JsonProperty = {
    jsonCreatorCtor
      .getParameterAnnotations()(paramIndex)
      .collectFirst { case jp: JsonProperty => jp }
      .orNull
  }

  "BulletChartStepDefinition ctor param[0] (start)" should "carry @JsonProperty(\"start\")" in {
    val jp = ctorParamJsonProperty(0)
    assert(jp != null && jp.value == "start")
  }

  "BulletChartStepDefinition ctor param[1] (end)" should "carry @JsonProperty(\"end\")" in {
    val jp = ctorParamJsonProperty(1)
    assert(jp != null && jp.value == "end")
  }

  it should "construct two independent instances (no static state shared)" in {
    val a = new BulletChartStepDefinition(Some(1), Some(2))
    val b = new BulletChartStepDefinition(Some(3), Some(4))
    a.start = Some(99)
    assert(b.start.contains(3))
  }
}
