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

class LineModeSpec extends AnyFlatSpec with Matchers {

  "LineMode" should "map each constant to its wire mode" in {
    LineMode.LINE.getMode shouldBe "line"
    LineMode.DOTS.getMode shouldBe "dots"
    LineMode.LINE_WITH_DOTS.getMode shouldBe "line with dots"
    LineMode.values() should have length 3
  }

  "LineMode.getModeInPlotly" should "translate to the Plotly mode string" in {
    LineMode.LINE.getModeInPlotly shouldBe "lines"
    LineMode.DOTS.getModeInPlotly shouldBe "markers"
    LineMode.LINE_WITH_DOTS.getModeInPlotly shouldBe "lines+markers"
  }

  "LineMode.fromString" should "resolve wire modes case-insensitively and reject unknowns" in {
    LineMode.fromString("line with dots") shouldBe LineMode.LINE_WITH_DOTS
    LineMode.fromString("DOTS") shouldBe LineMode.DOTS
    intercept[IllegalArgumentException](LineMode.fromString("zigzag"))
  }

  "LineMode" should "round-trip through Jackson using its wire mode" in {
    objectMapper.writeValueAsString(LineMode.LINE_WITH_DOTS) shouldBe "\"line with dots\""
    objectMapper.readValue("\"dots\"", classOf[LineMode]) shouldBe LineMode.DOTS
  }
}
