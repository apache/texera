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

import org.apache.texera.amber.util.PythonTestSupport.{
  NeedsPythonPackages,
  canImport,
  configuredPython,
  withDriver
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

class TablesPlotOpDescSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with GeneratedCodeAssertionChecks {

  var opDesc: TablesPlotOpDesc = _

  before {
    opDesc = new TablesPlotOpDesc()
  }

  private def column(name: String): TablesConfig = {
    val config = new TablesConfig()
    config.attributeName = name
    config
  }

  it should "throw AssertionError with a 'cannot be empty' message when included columns list is empty (manipulateTable)" in {
    expectError[AssertionError]("cannot be empty")(opDesc.manipulateTable())
  }

  it should "throw AssertionError with a 'cannot be empty' message when included columns list is empty (createPlotlyFigure)" in {
    expectError[AssertionError]("cannot be empty")(opDesc.createPlotlyFigure())
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

  it should "join multiple columns with a comma, not the literal ',' (valid Python)" in {
    // Each column renders to a decode(...) call, so they must be comma-joined;
    // joining with the literal ',' puts a string right after a call (invalid Python).
    opDesc.includedColumns = List(column("col_one"), column("col_two"))
    val code = opDesc.generatePythonCode()
    code should include(
      s"self.decode_python_template('${b64("col_one")}'),self.decode_python_template('${b64("col_two")}')"
    )
    code should not include "')','"
  }

  // Stubs only the pytexera import seam, then hands the operator a table that arrives
  // empty, one whose every row has a hole in a chosen column, and one that draws.
  // Both empty branches call self.render_error, which once was not defined.
  private val runtimeDriverScript: String =
    """import base64
      |import sys
      |import types
      |from typing import Iterator, Optional
      |
      |import pandas as pd
      |
      |class UDFTableOperator:
      |    def decode_python_template(self, data):
      |        return base64.b64decode(data).decode("utf-8")
      |
      |stub = types.ModuleType("pytexera")
      |stub.UDFTableOperator = UDFTableOperator
      |stub.overrides = lambda fn: fn
      |stub.Table = pd.DataFrame
      |stub.TableLike = object
      |stub.Iterator = Iterator
      |stub.Optional = Optional
      |sys.modules["pytexera"] = stub
      |
      |ns = {"__name__": "generated_table_chart"}
      |with open(sys.argv[1], encoding="utf-8") as f:
      |    exec(compile(f.read(), sys.argv[1], "exec"), ns)
      |op = ns["TableChartOperator"]()
      |
      |cases = [
      |    ("empty", pd.DataFrame({"col_one": [], "col_two": []})),
      |    ("every_row_holed", pd.DataFrame({"col_one": [1.0, None], "col_two": [None, "b"]})),
      |    ("filled", pd.DataFrame({"col_one": [1, 2], "col_two": ["a", "b"]})),
      |]
      |for cid, df in cases:
      |    page = list(op.process_table(df, 0))[0]["html-content"]
      |    print("CASE %s %s" % (cid, page if "not available" in page else "CHART"))
      |""".stripMargin

  it should "render its message when the table is empty or every row has a hole" taggedAs NeedsPythonPackages in {
    val python = configuredPython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImport(python, "pandas", "plotly")) cancel(s"'$python' cannot import pandas and plotly")

    opDesc.includedColumns = List(column("col_one"), column("col_two"))
    withDriver(
      python,
      "tables_plot_op_",
      "tables_plot_driver_",
      opDesc.generatePythonCode(),
      runtimeDriverScript,
      "Runtime driver timed out after 120s"
    ) { (exitCode, output) =>
      val page = "<h1>Tables Plot is not available.</h1><p>Reason is: "
      withClue(s"Driver output:\n$output\n") {
        exitCode shouldBe 0
        output should include(s"CASE empty ${page}input table is empty.</p>")
        output should include(
          s"CASE every_row_holed ${page}value column contains only non-positive numbers or nulls.</p>"
        )
        output should include("CASE filled CHART")
      }
    }
  }
}
