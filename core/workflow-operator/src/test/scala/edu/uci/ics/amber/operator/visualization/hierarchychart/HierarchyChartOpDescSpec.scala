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

package edu.uci.ics.amber.operator.visualization.hierarchychart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class HierarchyChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: HierarchyChartOpDesc = _

  before {
    opDesc = new HierarchyChartOpDesc()
  }

  it should "generate a list of hierarchy sections in the python code" in {
    val attributes = Array.fill(3)(new HierarchySection())
    attributes(0).attributeName = "column_a"
    attributes(1).attributeName = "column_b"
    attributes(2).attributeName = "column_c"
    opDesc.hierarchy = attributes.toList
    opDesc.hierarchyChartType = HierarchyChartType.TREEMAP
    assert(opDesc.createPlotlyFigure().contains("['column_a','column_b','column_c']"))
    opDesc.hierarchyChartType = HierarchyChartType.SUNBURSTCHART
    assert(opDesc.createPlotlyFigure().contains("['column_a','column_b','column_c']"))
  }

  it should "throw assertion error if hierarchy is empty" in {
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }
}
