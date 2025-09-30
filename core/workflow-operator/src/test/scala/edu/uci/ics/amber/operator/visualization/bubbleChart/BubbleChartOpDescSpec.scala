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

package edu.uci.ics.amber.operator.visualization.bubbleChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class BubbleChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: BubbleChartOpDesc = _

  before {
    opDesc = new BubbleChartOpDesc()
  }

  it should "generate a plotly python figure with 3 columns" in {
    opDesc.xValue = "column1"
    opDesc.yValue = "column2"
    opDesc.zValue = "column3"
    opDesc.enableColor = false

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "fig = go.Figure(px.scatter(table, x='column1', y='column2', size='column3', size_max=100))"
        )
    )
  }

  it should "throw assertion error if variable xValue is empty" in {
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }
}
