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

package org.apache.texera.amber.operator.visualization.gaugeChart

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class GaugeChartStepsSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Defaults
  // ---------------------------------------------------------------------------

  "GaugeChartSteps" should "default start and end to unset" in {
    val s = new GaugeChartSteps
    assert(s.start.isEmpty)
    assert(s.end.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow start and end to be assigned post-construction" in {
    val s = new GaugeChartSteps
    s.start = Some(10)
    s.end = Some(90)
    assert(s.start.contains(10))
    assert(s.end.contains(90))
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip — wire keys are `start` / `end`
  // ---------------------------------------------------------------------------

  "GaugeChartSteps JSON round-trip" should
    "serialize start and end as numbers under the canonical wire keys" in {
    val s = new GaugeChartSteps
    s.start = Some(1.5)
    s.end = Some(9.5)
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(s))
    assert(tree.has("start"))
    assert(tree.get("start").isNumber)
    assert(tree.get("start").asDouble() == 1.5)
    assert(tree.has("end"))
    assert(tree.get("end").isNumber)
    assert(tree.get("end").asDouble() == 9.5)
  }

  it should "round-trip both fields cleanly" in {
    val s = new GaugeChartSteps
    s.start = Some(0)
    s.end = Some(100)
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(s),
      classOf[GaugeChartSteps]
    )
    assert(restored.start.contains(0))
    assert(restored.end.contains(100))
  }

  /** `Option[Double]` erases its element type, so Jackson needs
    * `@JsonDeserialize(contentAs = ...)` to know what to build. Without it a JSON
    * string is left inside the Option unconverted and the first arithmetic use
    * throws ClassCastException — which a round trip cannot catch, since it writes a
    * number back. These read the shapes a stored workflow can actually hold.
    */
  private def read(json: String): GaugeChartSteps =
    objectMapper.readValue(json, classOf[GaugeChartSteps])

  "GaugeChartSteps bounds" should "deserialize JSON numbers" in {
    val s = read("""{"start":1.5,"end":9.5}""")
    assert(s.start.contains(1.5))
    assert(s.end.contains(9.5))
  }

  it should "deserialize the numeric strings a workflow saved before the bounds were numeric" in {
    val s = read("""{"start":"1.5","end":"9.5"}""")
    assert(s.start.contains(1.5))
    assert(s.end.contains(9.5))
  }

  it should "read absent, null and blank bounds as unset rather than as zero" in {
    assert(read("""{}""").start.isEmpty)
    assert(read("""{"start":null}""").start.isEmpty)
    assert(read("""{"start":""}""").start.isEmpty)
  }

  it should "hold a Double, not the raw JSON value" in {
    // The ClassCastException surfaces here, at the first use, not at read time.
    assert(read("""{"start":"1.5"}""").start.map(_ * 2).contains(3.0))
  }

  // ---------------------------------------------------------------------------
  // Annotations
  // ---------------------------------------------------------------------------

  "GaugeChartSteps#start" should "carry @JsonProperty(\"start\")" in {
    val jp = classOf[GaugeChartSteps]
      .getDeclaredField("start")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null)
    assert(jp.value == "start")
  }

  "GaugeChartSteps#end" should "carry @JsonProperty(\"end\")" in {
    val jp = classOf[GaugeChartSteps]
      .getDeclaredField("end")
      .getAnnotation(classOf[JsonProperty])
    assert(jp != null)
    assert(jp.value == "end")
  }

  // ---------------------------------------------------------------------------
  // Instance independence
  // ---------------------------------------------------------------------------

  it should "construct two independent instances (no static state shared)" in {
    val a = new GaugeChartSteps
    val b = new GaugeChartSteps
    a.start = Some(1)
    assert(b.start.isEmpty)
  }
}
