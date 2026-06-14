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

package org.apache.texera.amber.operator.visualization.bubbleChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BubbleChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {
  var opDesc: BubbleChartOpDesc = _

  before {
    opDesc = new BubbleChartOpDesc()
  }

  // The part of an assert message that names the offending field.
  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "generate a plotly python figure with 3 columns" in {
    opDesc.xValue = "column1"
    opDesc.yValue = "column2"
    opDesc.zValue = "column3"
    opDesc.enableColor = false

    assert(
      opDesc
        .createPlotlyFigure()
        .plain
        .contains(
          "fig = go.Figure(px.scatter(table, x=column1, y=column2, size=column3, size_max=100))"
        )
    )
  }

  it should "throw assertion error if variable xValue is empty" in {
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw AssertionError naming the X-Column when only xValue is missing" in {
    opDesc.yValue = "column2"
    opDesc.zValue = "column3"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("x")
  }

  it should "throw AssertionError naming the Y-Column when only yValue is missing" in {
    opDesc.xValue = "column1"
    opDesc.zValue = "column3"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("y")
  }

  it should "throw AssertionError naming the Z-Column when only zValue is missing" in {
    opDesc.xValue = "column1"
    opDesc.yValue = "column2"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("z")
  }

  it should "throw AssertionError with a per-field message in manipulateTable when fields are missing" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("x")
  }

  it should "render all three columns in manipulateTable when configured" in {
    opDesc.xValue = "column1"
    opDesc.yValue = "column2"
    opDesc.zValue = "column3"
    val plain = opDesc.manipulateTable().plain
    plain should include("column1")
    plain should include("column2")
    plain should include("column3")
  }
}
