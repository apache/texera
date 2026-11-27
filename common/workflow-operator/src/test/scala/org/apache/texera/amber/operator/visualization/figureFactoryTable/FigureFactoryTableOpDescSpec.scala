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

package org.apache.texera.amber.operator.visualization.figureFactoryTable

import org.apache.texera.amber.operator.visualization.ChartPythonTestSupport.verifyTableChartCases
import org.apache.texera.amber.util.PythonTestSupport.NeedsPythonPackages
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

class FigureFactoryTableOpDescSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with GeneratedCodeAssertionChecks {

  var opDesc: FigureFactoryTableOpDesc = _

  before {
    opDesc = new FigureFactoryTableOpDesc()
  }

  private def column(name: String): FigureFactoryTableConfig = {
    val config = new FigureFactoryTableConfig()
    config.attributeName = name
    config
  }

  private def withColumns(): Unit =
    opDesc.columns = List(column("col_one"), column("col_two"))

  it should "throw AssertionError with a 'cannot be empty' message when columns list is empty (manipulateTable)" in {
    expectError[AssertionError]("cannot be empty")(opDesc.manipulateTable())
  }

  it should "throw AssertionError with a 'cannot be empty' message when columns list is empty (createFigureFactoryTablePlotlyFigure)" in {
    expectError[AssertionError]("cannot be empty")(opDesc.createFigureFactoryTablePlotlyFigure())
  }

  it should "throw AssertionError mentioning 'at least 30' when rowHeight is below 30" in {
    withColumns()
    opDesc.rowHeight = 10.0
    expectError[AssertionError]("at least 30")(opDesc.createFigureFactoryTablePlotlyFigure())
  }

  it should "throw AssertionError mentioning 'non-negative' when fontSize is negative" in {
    withColumns()
    opDesc.fontSize = -1.0
    expectError[AssertionError]("non-negative")(opDesc.createFigureFactoryTablePlotlyFigure())
  }

  it should "not throw with default fontSize (12) and rowHeight (30) once columns are set" in {
    withColumns()
    val plain = opDesc.createFigureFactoryTablePlotlyFigure().plain
    assert(carries(plain, "col_one"))
    assert(carries(plain, "col_two"))
    plain should include("ff.create_table")
  }

  it should "accept boundary values rowHeight = 30 and fontSize = 0" in {
    withColumns()
    opDesc.rowHeight = 30.0
    opDesc.fontSize = 0.0
    noException should be thrownBy opDesc.createFigureFactoryTablePlotlyFigure()
  }

  it should "generate python code carrying the configured columns" in {
    withColumns()
    val code = opDesc.generatePythonCode()
    assert(carries(code, "col_one"))
    assert(carries(code, "col_two"))
    code should include("class TableChartOperator(UDFTableOperator)")
  }

  // Stubs only the pytexera import seam, then hands the operator a table that arrives
  // empty, one whose every row has a hole in a chosen column, and one that draws.
  // Both empty branches call self.render_error, which once was not defined.
  it should "render its message when the table is empty or every row has a hole" taggedAs NeedsPythonPackages in {
    withColumns()
    verifyTableChartCases(
      "figure_factory_table",
      opDesc.generatePythonCode(),
      "Figure Factory Table"
    )
  }
}
