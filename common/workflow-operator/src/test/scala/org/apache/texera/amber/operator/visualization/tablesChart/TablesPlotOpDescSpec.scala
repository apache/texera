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

package org.apache.texera.amber.operator.visualization.tablesChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class TablesPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: TablesPlotOpDesc = _

  before {
    opDesc = new TablesPlotOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def column(name: String): TablesConfig = {
    val config = new TablesConfig()
    config.attributeName = name
    config
  }

  it should "throw AssertionError with a 'cannot be empty' message when included columns list is empty (manipulateTable)" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  it should "throw AssertionError with a 'cannot be empty' message when included columns list is empty (createPlotlyFigure)" in {
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  it should "render the configured columns when the included columns list is set" in {
    opDesc.includedColumns = List(column("col_one"), column("col_two"))
    val tablePlain = opDesc.manipulateTable().plain
    assert(carries(tablePlain, "col_one"))
    assert(carries(tablePlain, "col_two"))

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "col_one"))
    assert(carries(figurePlain, "col_two"))
    figurePlain should include("go.Table")
  }

  it should "generate python code carrying the configured columns" in {
    opDesc.includedColumns = List(column("col_one"), column("col_two"))
    val code = opDesc.generatePythonCode()
    assert(carries(code, "col_one"))
    assert(carries(code, "col_two"))
    code should include("class TableChartOperator(UDFTableOperator)")
  }
}
