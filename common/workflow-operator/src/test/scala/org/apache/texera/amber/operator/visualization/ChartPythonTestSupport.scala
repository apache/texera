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

package org.apache.texera.amber.operator.visualization

import org.apache.texera.amber.util.PythonTestSupport.{requirePython, withDriver}
import org.scalatest.Assertions.withClue
import org.scalatest.matchers.should.Matchers._

private[visualization] object ChartPythonTestSupport {

  /** Parses `"CASE <id> <verdict>"` lines emitted by Python chart test drivers. */
  def parseCaseVerdicts(output: String): Map[String, String] =
    "CASE (\\S+) (\\S+)".r
      .findAllMatchIn(output)
      .map(m => m.group(1) -> m.group(2))
      .toMap

  /** Runs a generated operator module inside a minimal stubbed `pytexera` environment and
    * executes `driverBody` with the instantiated operator bound to `op`.
    */
  def withPytexeraTableDriver[A](
      prefix: String,
      generatedCode: => String,
      moduleName: String,
      operatorClass: String,
      driverBody: String,
      setupCode: String = "",
      timeoutMessage: String = "Runtime driver timed out after 120s"
  )(check: (Int, String) => A): A = {
    val python = requirePython("pandas", "plotly")
    val setupBlock = if (setupCode.isEmpty) "" else setupCode + "\n\n"
    val driver =
      s"""import base64
         |import sys
         |import types
         |from typing import Iterator, Optional
         |
         |import pandas as pd
         |${setupBlock}class UDFTableOperator:
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
         |ns = {"__name__": "$moduleName"}
         |with open(sys.argv[1], encoding="utf-8") as f:
         |    exec(compile(f.read(), sys.argv[1], "exec"), ns)
         |op = ns["$operatorClass"]()
         |
         |$driverBody
         |""".stripMargin
    withDriver(
      python,
      s"${prefix}_op_",
      s"${prefix}_driver_",
      generatedCode,
      driver,
      timeoutMessage
    )(check)
  }

  /** Runs generated standalone chart code once per `cases` entry, classifies each rendered HTML
    * output via `verdictBody` (assigning `verdict`), and asserts `parseCaseVerdicts(output)`.
    * Each run reads back the page it wrote, so a branch that raises instead of answering shows up
    * as a failure rather than as a missing file.
    */
  def verifyStandaloneChartCases(
      prefix: String,
      standaloneCode: => String,
      casesScript: String,
      verdictBody: String,
      expectedVerdicts: Map[String, String],
      plotlyScopeBinding: Option[(String, String)] = None,
      timeoutMessage: String = "Runtime driver timed out after 120s"
  ): Unit = {
    val python = requirePython("pandas", "plotly")
    val plotlyImport =
      plotlyScopeBinding.map { case (mod, alias) => s"import $mod as $alias\n" }.getOrElse("")
    val plotlyScope =
      plotlyScopeBinding
        .map { case (_, alias) => "\n        \"" + alias + "\": " + alias + "," }
        .getOrElse("")
    val driver =
      s"""import pathlib
         |import sys
         |import tempfile
         |
         |import pandas as pd
         |${plotlyImport}
         |# The module and the page are both UTF-8, which Windows does not read by
         |# default.
         |source = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
         |
         |$casesScript
         |
         |for name, frame in cases.items():
         |    directory = tempfile.mkdtemp()
         |    scope = {
         |        "in1df": frame,
         |        "outputHtml": directory + "/chart.html",
         |        "outputJson": directory + "/chart.json",
         |        "pd": pd,$plotlyScope
         |    }
         |    exec(compile(source, "standalone", "exec"), scope)
         |    page = pathlib.Path(scope["outputHtml"]).read_text(encoding="utf-8").strip()
         |$verdictBody
         |    print("CASE %s %s" % (name, verdict))
         |""".stripMargin
    withDriver(
      python,
      s"${prefix}_standalone_",
      s"${prefix}_driver_",
      standaloneCode,
      driver,
      timeoutMessage
    ) { (exitCode, output) =>
      withClue(s"Driver output:\n$output\n") {
        exitCode shouldBe 0
        parseCaseVerdicts(output) shouldBe expectedVerdicts
      }
    }
  }

  private val runtimeDriverBody: String =
    """cases = [
      |    ("empty", pd.DataFrame({"col_one": [], "col_two": []})),
      |    ("every_row_holed", pd.DataFrame({"col_one": [1.0, None], "col_two": [None, "b"]})),
      |    ("filled", pd.DataFrame({"col_one": [1, 2], "col_two": ["a", "b"]})),
      |]
      |for cid, df in cases:
      |    page = list(op.process_table(df, 0))[0]["html-content"]
      |    print("CASE %s %s" % (cid, page if "not available" in page else "CHART"))""".stripMargin

  def verifyTableChartCases(prefix: String, generatedCode: => String, title: String): Unit = {
    withPytexeraTableDriver(
      prefix,
      generatedCode,
      "generated_table_chart",
      "TableChartOperator",
      runtimeDriverBody
    ) { (exitCode, output) =>
      val page = s"<h1>$title is not available.</h1><p>Reason is: "
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
