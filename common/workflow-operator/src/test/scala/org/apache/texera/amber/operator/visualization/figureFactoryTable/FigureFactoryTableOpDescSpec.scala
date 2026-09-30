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

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.scalatest.BeforeAndAfter
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.util.Try

class FigureFactoryTableOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

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
      |with open(sys.argv[1]) as f:
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
    val python = resolvePythonExecutable().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandasAndPlotly(python)) cancel(s"'$python' cannot import pandas and plotly")

    withColumns()
    val moduleFile = Files.createTempFile("figure_factory_table_op_", ".py")
    val driverFile = Files.createTempFile("figure_factory_table_driver_", ".py")
    try {
      Files.write(moduleFile, opDesc.generatePythonCode().getBytes(StandardCharsets.UTF_8))
      Files.write(driverFile, runtimeDriverScript.getBytes(StandardCharsets.UTF_8))
      val process = new ProcessBuilder(python, driverFile.toString, moduleFile.toString)
        .redirectErrorStream(true)
        .start()
      if (!process.waitFor(120, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        fail("Runtime driver timed out after 120s")
      }
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      val page = "<h1>Figure Factory Table is not available.</h1><p>Reason is: "
      withClue(s"Driver output:\n$output\n") {
        process.exitValue() shouldBe 0
        output should include(s"CASE empty ${page}input table is empty.</p>")
        output should include(
          s"CASE every_row_holed ${page}value column contains only non-positive numbers or nulls.</p>"
        )
        output should include("CASE filled CHART")
      }
    } finally {
      Try(Files.deleteIfExists(moduleFile))
      Try(Files.deleteIfExists(driverFile))
      ()
    }
  }

  // udf.conf python.path (UDF_PYTHON_PATH), then python3 / python / py.
  private def resolvePythonExecutable(): Option[String] = {
    def fromConfig: Option[String] =
      Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
        .orElse(Try(ConfigFactory.load()).toOption)
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)

    def isRunnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(isRunnable)
  }

  private def canImportPandasAndPlotly(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas, plotly").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
