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

package org.apache.texera.amber.operator.visualization.waterfallChart

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.util.Try

class WaterfallChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: WaterfallChartOpDesc = _

  before {
    opDesc = new WaterfallChartOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // A column name is carried either literally (plain chunks) or as the
  // base64 payload of a runtime decode site (encoded chunks).
  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  it should "default xColumn and yColumn to non-null, empty strings" in {
    opDesc.xColumn should not be null
    opDesc.xColumn shouldBe ""
    opDesc.yColumn should not be null
    opDesc.yColumn shouldBe ""
  }

  it should "throw an AssertionError (not a NullPointerException) naming the X Axis when xColumn is left empty" in {
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex shouldBe a[AssertionError]
    ex.getMessage should not be null
    ex.getMessage should include("X Axis Values cannot be empty")
  }

  it should "throw an AssertionError naming the Y Axis when only xColumn is set" in {
    opDesc.xColumn = "x_col"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("Y Axis Values cannot be empty")
  }

  it should "render the configured x and y columns when both are set" in {
    opDesc.xColumn = "x_col"
    opDesc.yColumn = "y_col"

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "x_col"))
    assert(carries(figurePlain, "y_col"))
    figurePlain should include("go.Waterfall")

    val code = opDesc.generatePythonCode()
    assert(carries(code, "x_col"))
    assert(carries(code, "y_col"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }

  private def configured(): WaterfallChartOpDesc = {
    opDesc.xColumn = "x_col"
    opDesc.yColumn = "y_col"
    opDesc
  }

  it should "give every input row a relative bar and append the total as an extra bar" in {
    for (
      figure <- Seq(configured().createPlotlyFigure().plain, configured().generateStandaloneCode())
    ) {
      figure should include("""measure=["relative"] * len(y_values) + ["total"]""")
      figure should not include """(len(y_values) - 1)"""
      figure should include("""x=x_values + ["Total"]""")
      figure should include("y=y_values + [0]")
    }
  }

  it should "pin the x axis to categories so the bars keep their input order" in {
    for (
      figure <- Seq(configured().createPlotlyFigure().plain, configured().generateStandaloneCode())
    ) {
      figure should include("""xaxis_type="category"""")
    }
  }

  // Python executable resolution, following FilledAreaPlotOpDescSpec:
  // udf.conf python.path (UDF_PYTHON_PATH), then python3 / python / py.
  private def resolvePythonExecutable(): Option[String] = {
    def fromConfig: Option[String] = {
      val configOpt =
        Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
          .orElse(Try(ConfigFactory.load()).toOption)
      configOpt
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)
    }

    def isRunnable(exe: String): Boolean = {
      val pTry = Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start())
      pTry.toOption.exists { p =>
        val finished = p.waitFor(5, TimeUnit.SECONDS)
        if (!finished) { p.destroyForcibly(); false }
        else p.exitValue() == 0
      }
    }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(isRunnable)
  }

  private def canImportPandasAndPlotly(python: String): Boolean = {
    val pTry = Try(
      new ProcessBuilder(python, "-c", "import pandas, plotly").redirectErrorStream(true).start()
    )
    pTry.toOption.exists { p =>
      val finished = p.waitFor(60, TimeUnit.SECONDS)
      if (!finished) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
  }

  // Driver executed by the runtime test below. It stubs only the pytexera import seam and
  // swaps plotly's html renderer for its json one, so the trace the generated module hands
  // plotly can be read back bar by bar.
  private val runtimeDriverScript: String =
    """import base64
      |import json
      |import sys
      |import types
      |from typing import Iterator, Optional
      |
      |import pandas as pd
      |import plotly.io
      |
      |plotly.io.to_html = lambda fig, **kwargs: fig.to_json()
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
      |ns = {"__name__": "generated_waterfall_chart"}
      |with open(sys.argv[1]) as f:
      |    exec(compile(f.read(), sys.argv[1], "exec"), ns)
      |op = ns["ProcessTableOperator"]()
      |
      |cases = [
      |    ("labels", pd.DataFrame({"x_col": ["a", "b", "c", "d"], "y_col": [1, 2, 3, 4]})),
      |    ("unsorted_numeric_x", pd.DataFrame({"x_col": [30, 10, 20], "y_col": [1, 2, 3]})),
      |]
      |
      |for cid, df in cases:
      |    fig = json.loads(list(op.process_table(df, 0))[0]["html-content"])
      |    trace = fig["data"][0]
      |    print("CASE %s measure=%s x=%s y=%s text=%s xaxis=%s" % (
      |        cid,
      |        json.dumps(trace["measure"]),
      |        json.dumps(list(trace["x"])),
      |        json.dumps(list(trace["y"])),
      |        json.dumps(list(trace["text"])),
      |        fig["layout"]["xaxis"]["type"],
      |    ))
      |""".stripMargin

  it should "plot the last row and draw the total over the whole column at runtime" in {
    val python = resolvePythonExecutable().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandasAndPlotly(python)) {
      cancel(s"'$python' cannot import pandas and plotly; skipping runtime verification")
    }

    val moduleFile = Files.createTempFile("waterfall_chart_op_", ".py")
    val driverFile = Files.createTempFile("waterfall_chart_driver_", ".py")
    try {
      Files.write(moduleFile, configured().generatePythonCode().getBytes(StandardCharsets.UTF_8))
      Files.write(driverFile, runtimeDriverScript.getBytes(StandardCharsets.UTF_8))

      val process = new ProcessBuilder(python, driverFile.toString, moduleFile.toString)
        .redirectErrorStream(true)
        .start()
      val finished = process.waitFor(120, TimeUnit.SECONDS)
      if (!finished) {
        process.destroyForcibly()
        fail("Runtime verification driver timed out after 120s")
      }
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      withClue(s"Driver output:\n$output\n") {
        process.exitValue() shouldBe 0
        // Four rows of 1, 2, 3, 4 give four bars plus a total of 10. The old measure list
        // spent the last row on the total, drawing three bars and a fourth at 6 labelled +4.
        output should include(
          """CASE labels measure=["relative", "relative", "relative", "relative", "total"] """ +
            """x=["a", "b", "c", "d", "Total"] y=[1, 2, 3, 4, 0] """ +
            """text=["+1", "+2", "+3", "+4", "10"] xaxis=category"""
        )
        // An unsorted numeric x column keeps its input order, so the bar plotly accumulates
        // into is the bar the running total was computed from.
        output should include(
          """CASE unsorted_numeric_x measure=["relative", "relative", "relative", "total"] """ +
            """x=[30, 10, 20, "Total"] y=[1, 2, 3, 0] """ +
            """text=["+1", "+2", "+3", "6"] xaxis=category"""
        )
      }
    } finally {
      Try(Files.deleteIfExists(moduleFile))
      Try(Files.deleteIfExists(driverFile))
      ()
    }
  }
}
