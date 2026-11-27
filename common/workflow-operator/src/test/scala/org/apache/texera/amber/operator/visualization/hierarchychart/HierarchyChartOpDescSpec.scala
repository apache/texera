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

package org.apache.texera.amber.operator.visualization.hierarchychart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

class HierarchyChartOpDescSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with GeneratedCodeAssertionChecks {

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
    opDesc.hierarchyChartType = HierarchyChartType.SUNBURSTCHART
  }

  it should "throw AssertionError naming the Value Column when value is left empty" in {
    expectEmptyFieldError("value")(opDesc.manipulateTable())
  }

  it should "throw AssertionError naming the hierarchy when the hierarchy path is empty" in {
    expectEmptyFieldError("hierarchy")(opDesc.createPlotlyFigure())
  }

  it should "render hierarchy attributes and the value column when fully configured" in {
    val sectionA = new HierarchySection()
    sectionA.attributeName = "level_one"
    val sectionB = new HierarchySection()
    sectionB.attributeName = "level_two"
    opDesc.hierarchy = List(sectionA, sectionB)
    opDesc.value = "tree_value_col"
    opDesc.hierarchyChartType = HierarchyChartType.TREEMAP

    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "tree_value_col"))
    assert(carries(tablePlain, "level_one"))
    assert(carries(tablePlain, "level_two"))

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "tree_value_col"))
    assert(carries(figurePlain, "level_one"))
    assert(carries(figurePlain, "level_two"))
  }
}
