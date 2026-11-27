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

package org.apache.texera.amber.operator.visualization.IcicleChart

import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchySection
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

class IcicleChartOpDescSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with GeneratedCodeAssertionChecks {

  var opDesc: IcicleChartOpDesc = _

  before {
    opDesc = new IcicleChartOpDesc()
  }

  private def section(name: String): HierarchySection = {
    val s = new HierarchySection()
    s.attributeName = name
    s
  }

  it should "throw AssertionError naming the hierarchy when the hierarchy path is empty" in {
    expectEmptyFieldError("hierarchy")(opDesc.createPlotlyFigure())
  }

  it should "throw AssertionError naming the Value Column in manipulateTable when value is left empty" in {
    // manipulateTable now guards `value` (it interpolates table[$value]),
    // closing the gap flagged in review round 1.
    opDesc.hierarchy = List(section("level_one"))
    val ex = expectError[AssertionError]("cannot be empty")(opDesc.manipulateTable())
    ex.getMessage should include("Value")
  }

  it should "render hierarchy attributes and the value column when configured" in {
    opDesc.hierarchy = List(section("level_one"), section("level_two"))
    opDesc.value = "icicle_value_col"
    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "level_one"))
    assert(carries(figurePlain, "level_two"))
    assert(carries(figurePlain, "icicle_value_col"))
    figurePlain should include("px.icicle")
  }

  it should "render the value column in manipulateTable when configured" in {
    opDesc.hierarchy = List(section("level_one"))
    opDesc.value = "icicle_value_col"
    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "icicle_value_col"))
    assert(carries(tablePlain, "level_one"))
  }
}
