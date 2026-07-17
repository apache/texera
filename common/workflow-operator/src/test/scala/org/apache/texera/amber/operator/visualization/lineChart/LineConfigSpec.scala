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

package org.apache.texera.amber.operator.visualization.lineChart

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LineConfigSpec extends AnyFlatSpec with Matchers {

  "LineConfig" should "default string fields to empty and mode to LINE_WITH_DOTS" in {
    val c = new LineConfig
    c.yValue shouldBe ""
    c.xValue shouldBe ""
    c.name shouldBe ""
    c.color shouldBe ""
    c.mode shouldBe LineMode.LINE_WITH_DOTS
  }

  "LineConfig JSON" should "serialize fields under their wire-keys y / x / mode / name / color" in {
    val c = new LineConfig
    c.yValue = "sales"
    c.xValue = "month"
    c.name = "trend"
    c.color = "#fff"
    c.mode = LineMode.LINE
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(c))
    tree.get("y").asText shouldBe "sales"
    tree.get("x").asText shouldBe "month"
    tree.get("name").asText shouldBe "trend"
    tree.get("color").asText shouldBe "#fff"
    // mode is a Java enum with @JsonValue -> its lowercase phrase
    tree.get("mode").asText shouldBe "line"
  }

  it should "serialize LINE_WITH_DOTS via its @JsonValue string" in {
    val c = new LineConfig
    c.mode = LineMode.LINE_WITH_DOTS
    objectMapper
      .readTree(objectMapper.writeValueAsString(c))
      .get("mode")
      .asText shouldBe "line with dots"
  }

  it should "round-trip all five fields" in {
    val c = new LineConfig
    c.yValue = "y1"
    c.xValue = "x1"
    c.name = "n"
    c.color = "red"
    c.mode = LineMode.DOTS
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(c), classOf[LineConfig])
    restored.yValue shouldBe "y1"
    restored.xValue shouldBe "x1"
    restored.name shouldBe "n"
    restored.color shouldBe "red"
    restored.mode shouldBe LineMode.DOTS
  }

  "LineMode.fromString" should "map mode strings case-insensitively" in {
    LineMode.fromString("line") shouldBe LineMode.LINE
    LineMode.fromString("dots") shouldBe LineMode.DOTS
    LineMode.fromString("LINE WITH DOTS") shouldBe LineMode.LINE_WITH_DOTS
  }

  it should "throw IllegalArgumentException for an unknown mode" in {
    an[IllegalArgumentException] should be thrownBy LineMode.fromString("wiggly")
  }

  "LineMode.getModeInPlotly" should "map each mode to its plotly trace mode" in {
    LineMode.LINE.getModeInPlotly shouldBe "lines"
    LineMode.DOTS.getModeInPlotly shouldBe "markers"
    LineMode.LINE_WITH_DOTS.getModeInPlotly shouldBe "lines+markers"
  }
}
