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

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class FigureFactoryTableOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: FigureFactoryTableOpDesc = _

  before {
    opDesc = new FigureFactoryTableOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def column(name: String): FigureFactoryTableConfig = {
    val config = new FigureFactoryTableConfig()
    config.attributeName = name
    config
  }

  private def withColumns(): Unit =
    opDesc.columns = List(column("col_one"), column("col_two"))

  it should "throw AssertionError with a 'cannot be empty' message when columns list is empty (manipulateTable)" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  it should "throw AssertionError with a 'cannot be empty' message when columns list is empty (createFigureFactoryTablePlotlyFigure)" in {
    val ex = intercept[AssertionError](opDesc.createFigureFactoryTablePlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  it should "throw AssertionError mentioning 'at least 30' when rowHeight is below 30" in {
    withColumns()
    opDesc.rowHeight = 10.0
    val ex = intercept[AssertionError](opDesc.createFigureFactoryTablePlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("at least 30")
  }

  it should "throw AssertionError mentioning 'non-negative' when fontSize is negative" in {
    withColumns()
    opDesc.fontSize = -1.0
    val ex = intercept[AssertionError](opDesc.createFigureFactoryTablePlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("non-negative")
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
}
