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

package org.apache.texera.amber.operator.visualization.ecdfPlot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class ECDFPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: ECDFPlotOpDesc = _

  before {
    opDesc = new ECDFPlotOpDesc()
  }

  it should "throw assertion error if value column is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "generate a plotly ecdf figure with optional parameters" in {
    opDesc.valueColumn = "score"
    opDesc.colorColumn = "group"
    opDesc.separateBy = "category"
    opDesc.yAxisMode = "count"
    opDesc.cdfMode = "reversed"
    opDesc.orientation = "horizontal"
    opDesc.showMarkers = true
    opDesc.marginal = "histogram"

    val plain = opDesc.createPlotlyFigure().plain

    assert(plain.contains("fig = px.ecdf(table"))
    assert(plain.contains("ecdfnorm=None"))
    assert(plain.contains("ecdfmode=self.decode_python_template"))
    assert(plain.contains("orientation='h'"))
    assert(plain.contains("markers=True"))
    assert(plain.contains("marginal=self.decode_python_template"))
    assert(plain.contains("x=self.decode_python_template"))
    assert(plain.contains("color=self.decode_python_template"))
    assert(plain.contains("facet_col=self.decode_python_template"))
  }
}
