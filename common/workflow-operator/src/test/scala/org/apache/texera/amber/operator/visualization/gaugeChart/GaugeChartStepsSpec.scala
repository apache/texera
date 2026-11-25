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
import org.apache.texera.amber.operator.visualization.ChartConfigSpecSupport
import org.scalatest.flatspec.AnyFlatSpec

class GaugeChartStepsSpec extends AnyFlatSpec with ChartConfigSpecSupport {

  "GaugeChartSteps" should "default start and end to unset" in {
    val s = new GaugeChartSteps
    assert(s.start.isEmpty && s.end.isEmpty)
  }

  it should "allow start and end to be assigned post-construction" in {
    val s = new GaugeChartSteps
    s.start = Some(10)
    s.end = Some(90)
    assert(s.start.contains(10) && s.end.contains(90))
  }

  "GaugeChartSteps JSON round-trip" should
    "serialize start and end as numbers under the canonical wire keys" in {
    val s = new GaugeChartSteps
    s.start = Some(1.5)
    s.end = Some(9.5)
    assertStepWireKeys(s, 1.5, 9.5)
  }

  it should "round-trip both fields cleanly" in {
    val s = new GaugeChartSteps
    s.start = Some(0)
    s.end = Some(100)
    val restored = checkRoundTrip(s, classOf[GaugeChartSteps])
    assert(restored.start.contains(0) && restored.end.contains(100))
  }

  "GaugeChartSteps bounds" should "deserialize JSON numbers" in {
    assertBoundsPair[GaugeChartSteps](
      classOf[GaugeChartSteps],
      """{"start":1.5,"end":9.5}""",
      s => (s.start, s.end),
      1.5,
      9.5
    )
  }

  it should "deserialize the numeric strings a workflow saved before the bounds were numeric" in {
    assertBoundsPair[GaugeChartSteps](
      classOf[GaugeChartSteps],
      """{"start":"1.5","end":"9.5"}""",
      s => (s.start, s.end),
      1.5,
      9.5
    )
  }

  it should "read absent, null and blank bounds as unset rather than as zero" in {
    assertUnsetBounds[GaugeChartSteps](classOf[GaugeChartSteps], _.start)
  }

  it should "hold a Double, not the raw JSON value" in {
    assertDoubleBoundsConversion[GaugeChartSteps](classOf[GaugeChartSteps], _.start)
  }

  "GaugeChartSteps#start" should "carry @JsonProperty(\"start\")" in {
    val jp = classOf[GaugeChartSteps].getDeclaredField("start").getAnnotation(classOf[JsonProperty])
    assert(jp != null && jp.value == "start")
  }

  "GaugeChartSteps#end" should "carry @JsonProperty(\"end\")" in {
    val jp = classOf[GaugeChartSteps].getDeclaredField("end").getAnnotation(classOf[JsonProperty])
    assert(jp != null && jp.value == "end")
  }

  it should "construct two independent instances (no static state shared)" in {
    val a = new GaugeChartSteps
    val b = new GaugeChartSteps
    a.start = Some(1)
    assert(b.start.isEmpty)
  }
}
