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

package edu.uci.ics.amber.operator.visualization.ganttChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class GanttChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: GanttChartOpDesc = _
  before {
    opDesc = new GanttChartOpDesc()
  }
  it should "generate a plotly python figure with 3 columns and no color" in {
    opDesc.start = "start"
    opDesc.finish = "finish"
    opDesc.task = "task"
    opDesc.color = ""

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "fig = px.timeline(table, x_start='start', x_end='finish', y='task'  )"
        )
    )
  }
  it should "generate a plotly python figure with 3 columns and color" in {
    opDesc.start = "start"
    opDesc.finish = "finish"
    opDesc.task = "task"
    opDesc.color = "color"

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "fig = px.timeline(table, x_start='start', x_end='finish', y='task' , color='color' )"
        )
    )
  }
  it should "generate a plotly python figure with 3 columns and color and pattern" in {
    opDesc.start = "start"
    opDesc.finish = "finish"
    opDesc.task = "task"
    opDesc.color = "color"
    opDesc.pattern = "task"

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "fig = px.timeline(table, x_start='start', x_end='finish', y='task' , color='color' , pattern_shape='task')"
        )
    )
  }
}
