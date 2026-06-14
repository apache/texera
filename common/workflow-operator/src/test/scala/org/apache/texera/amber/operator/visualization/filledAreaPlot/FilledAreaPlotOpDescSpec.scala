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

package org.apache.texera.amber.operator.visualization.filledAreaPlot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FilledAreaPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: FilledAreaPlotOpDesc = _

  before {
    opDesc = new FilledAreaPlotOpDesc()
  }

  // The part of an assert message that names the offending field.
  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw error if X is empty" in {
    val y = "test1"
    val group = "test2"
    opDesc.y = y
    opDesc.lineGroup = group

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw error if Y is empty" in {
    val x = "test1"
    val group = "test2"
    opDesc.x = x
    opDesc.lineGroup = group

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw error if LineGroup is not indicated facet column is checked" in {
    val x = "test1"
    val y = "test2"
    opDesc.x = x
    opDesc.y = y
    opDesc.facetColumn = true
    opDesc.color = "color"

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw AssertionError naming the X-axis Attribute when only x is missing" in {
    opDesc.y = "area_y"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("x")
  }

  it should "throw AssertionError naming the Y-axis Attribute when only y is missing" in {
    opDesc.x = "area_x"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("y")
  }

  it should "throw AssertionError naming the Line Group when facetColumn is enabled without a line group" in {
    opDesc.x = "area_x"
    opDesc.y = "area_y"
    opDesc.facetColumn = true
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("line")
  }

  it should "render the configured x and y attributes when set" in {
    opDesc.x = "area_x"
    opDesc.y = "area_y"
    val plain = opDesc.createPlotlyFigure().plain
    plain should include("area_x")
    plain should include("area_y")
    plain should include("px.area")
  }
}
