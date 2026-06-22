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

  "GaugeChartSteps" should "default start and end to the empty string" in {
    val s = new GaugeChartSteps
    assert(s.start == "")
    assert(s.end == "")
  }

  // ---------------------------------------------------------------------------
  // Mutability
  // ---------------------------------------------------------------------------

  it should "allow start and end to be assigned post-construction" in {
    val s = new GaugeChartSteps
    s.start = "10"
    s.end = "90"
    assert(s.start == "10")
    assert(s.end == "90")
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip — wire keys are `start` / `end`
  // ---------------------------------------------------------------------------

  "GaugeChartSteps JSON round-trip" should
    "serialize start and end under the canonical wire keys" in {
    val s = new GaugeChartSteps
    s.start = "low"
    s.end = "high"
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(s))
    assert(tree.has("start"))
    assert(tree.get("start").asText() == "low")
    assert(tree.has("end"))
    assert(tree.get("end").asText() == "high")
  }

  it should "round-trip both fields cleanly" in {
    val s = new GaugeChartSteps
    s.start = "0"
    s.end = "100"
    val restored = objectMapper.readValue(
      objectMapper.writeValueAsString(s),
      classOf[GaugeChartSteps]
    )
    assert(restored.start == "0")
    assert(restored.end == "100")
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
    a.start = "1"
    assert(b.start == "")
  }
}
