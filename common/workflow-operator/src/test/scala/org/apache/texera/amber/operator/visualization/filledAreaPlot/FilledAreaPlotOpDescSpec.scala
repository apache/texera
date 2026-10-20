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

package org.apache.texera.amber.operator.visualization.filledAreaPlot

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.util.Try

class FilledAreaPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: FilledAreaPlotOpDesc = _

  before {
    opDesc = new FilledAreaPlotOpDesc()
  }

  // The part of an assert message that names the offending field.
  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw error if X is empty" in {
    val y = "test1"
    val group = "test2"
    opDesc.y = y
    opDesc.lineGroup = group

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw error if Y is empty" in {
    val x = "test1"
    val group = "test2"
    opDesc.x = x
    opDesc.lineGroup = group

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw error if LineGroup is not indicated facet column is checked" in {
    val x = "test1"
    val y = "test2"
    opDesc.x = x
    opDesc.y = y
    opDesc.facetColumn = true
    opDesc.color = "color"

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw AssertionError naming the X-axis Attribute when only x is missing" in {
    opDesc.y = "area_y"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("x")
  }

  it should "throw AssertionError naming the Y-axis Attribute when only y is missing" in {
    opDesc.x = "area_x"
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("y")
  }

  it should "throw AssertionError naming the Line Group when facetColumn is enabled without a line group" in {
    opDesc.x = "area_x"
    opDesc.y = "area_y"
    opDesc.facetColumn = true
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("line")
  }

  it should "render the configured x and y attributes when set" in {
    opDesc.x = "area_x"
    opDesc.y = "area_y"
    val plain = opDesc.createPlotlyFigure().plain
    plain should include("area_x")
    plain should include("area_y")
    plain should include("px.area")
  }

  "FilledAreaPlotOpDesc.getOutputSchemas" should
    "return exactly one html-content STRING column" in {
    opDesc.getOutputSchemas(Map.empty) shouldBe Map(
      opDesc.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "FilledAreaPlotOpDesc.createPlotlyFigure" should
    "emit the optional plotly args when color, facet, line group, and pattern are set" in {
    opDesc.x = "area_x"
    opDesc.y = "area_y"
    opDesc.color = "c"
    opDesc.facetColumn = true
    opDesc.lineGroup = "grp"
    opDesc.pattern = "p"
    val plain = opDesc.createPlotlyFigure().plain
    plain should include("px.area")
    plain should include("color=")
    plain should include("facet_col=")
    plain should include("line_group=")
    plain should include("pattern_shape=")
  }

  private def generatedGuardCode(): String = {
    opDesc.x = "x"
    opDesc.y = "y"
    opDesc.lineGroup = "g"
    opDesc.generatePythonCode()
  }

  "FilledAreaPlotOpDesc.generatePythonCode" should
    "compute the disjoint-group tolerance as five percent of the group count" in {
    val code = generatedGuardCode()
    code should include("(len(grouped) * 5) // 100")
    code should not include "(len(grouped) // 100) * 5"
  }

  it should "collect each group's unique x values exactly once per loop iteration" in {
    val code = generatedGuardCode()
    "\\.unique\\(\\)".r.findAllIn(code).size shouldBe 1
  }

  it should "compare x_values against None with an identity check" in {
    val code = generatedGuardCode()
    code should include("x_values is None")
    code should not include "x_values == None"
  }

  it should "treat the disjoint-group branch as a plain else" in {
    val code = generatedGuardCode()
    code should not include "elif not set("
    code should include("else:")
  }

  it should "break out of the group loop once the error is set" in {
    val code = generatedGuardCode()
    code should include("break")
  }

  it should "keep the shared-x guard with its tolerance and error message" in {
    val code = generatedGuardCode()
    code should include("tolerance")
    code should include("X attributes not shared across groups")
  }

  it should "never emit user-provided column names verbatim" in {
    opDesc.x = "distinctive_col_xyz"
    opDesc.y = "distinctive_col_abc"
    opDesc.lineGroup = "distinctive_grp_qrs"
    val code = opDesc.generatePythonCode()
    code should include("decode_python_template")
    code should not include "distinctive_col_xyz"
    code should not include "distinctive_col_abc"
    code should not include "distinctive_grp_qrs"
  }

  it should "place the break after the disjoint-group error assignment" in {
    val code = generatedGuardCode()
    val errorIndex = code.indexOf("error = \"X attributes not shared across groups\"")
    val breakIndex = code.indexOf("break")
    errorIndex should be > -1
    breakIndex should be > errorIndex
  }

  it should "emit the missing-attributes branch with its fallback text" in {
    val code = generatedGuardCode()
    code should include("error = \"missing attributes\"")
    code should include("X or Y attribute does not exist")
  }

  // Python executable resolution, following PythonCodeRawInvalidTextSpec:
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

  // Driver executed by the runtime test below. It stubs only the pytexera import seam
  // (base class, decorator, and type aliases); the generated module itself runs unmodified,
  // including the real pandas groupby guard and plotly rendering.
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
      |ns = {"__name__": "generated_filled_area_plot"}
      |with open(sys.argv[1]) as f:
      |    exec(compile(f.read(), sys.argv[1], "exec"), ns)
      |op = ns["ProcessTableOperator"]()
      |
      |def make_df(n_groups, disjoint):
      |    rows = []
      |    for i in range(n_groups):
      |        xs = range(101, 111) if i in disjoint else range(1, 11)
      |        rows += [{"g": "g%03d" % i, "x": x, "y": float(x) * (i + 1)} for x in xs]
      |    return pd.DataFrame(rows, columns=["g", "x", "y"])
      |
      |all_disjoint = pd.DataFrame(
      |    [{"g": "g%03d" % i, "x": x, "y": float(x)}
      |     for i in range(5) for x in range(i * 100 + 1, i * 100 + 11)]
      |)
      |
      |cases = [
      |    ("b19_1", make_df(19, {18})),
      |    ("b20_1", make_df(20, {19})),
      |    ("b40_1", make_df(40, {39})),
      |    ("b40_2", make_df(40, {38, 39})),
      |    ("b40_3", make_df(40, {37, 38, 39})),
      |    ("alldis", all_disjoint),
      |    ("single", make_df(1, set())),
      |    ("empty", pd.DataFrame({"x": [], "y": [], "g": []})),
      |    ("miscol", pd.DataFrame({"wrong": [1], "y": [1.0], "g": ["g0"]})),
      |]
      |
      |for cid, df in cases:
      |    html = list(op.process_table(df, 0))[0]["html-content"]
      |    if "not shared across all line groups" in html:
      |        verdict = "FALLBACK"
      |    elif "does not exist" in html:
      |        verdict = "MISSING"
      |    else:
      |        verdict = "CHART"
      |    print("CASE %s %s" % (cid, verdict))
      |""".stripMargin

  it should "enforce the five-percent tolerance at runtime boundaries" in {
    val python = resolvePythonExecutable().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandasAndPlotly(python)) {
      cancel(s"'$python' cannot import pandas and plotly; skipping runtime verification")
    }

    val moduleFile = Files.createTempFile("filled_area_plot_op_", ".py")
    val driverFile = Files.createTempFile("filled_area_plot_driver_", ".py")
    try {
      Files.write(moduleFile, generatedGuardCode().getBytes(StandardCharsets.UTF_8))
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
        val verdicts = "CASE (\\S+) (\\S+)".r
          .findAllMatchIn(output)
          .map(m => m.group(1) -> m.group(2))
          .toMap
        verdicts shouldBe Map(
          "b19_1" -> "FALLBACK", // 1/19 disjoint = 5.3% > 5%
          "b20_1" -> "CHART", // exactly 5.0%, tolerance is strict >
          "b40_1" -> "CHART", // 2.5%: the original floored-tolerance bug
          "b40_2" -> "CHART", // exactly 5.0%
          "b40_3" -> "FALLBACK", // 7.5%
          "alldis" -> "FALLBACK", // every group disjoint
          "single" -> "CHART", // one group, nothing to compare
          "empty" -> "CHART", // no rows, guard loop never runs
          "miscol" -> "MISSING" // x column absent
        )
      }
    } finally {
      Try(Files.deleteIfExists(moduleFile))
      Try(Files.deleteIfExists(driverFile))
      ()
    }
  }

  "FilledAreaPlotOpDesc" should "round-trip its config fields through the polymorphic base" in {
    opDesc.x = "area_x"
    opDesc.y = "area_y"
    opDesc.lineGroup = "grp"
    opDesc.color = "c"
    opDesc.facetColumn = true
    opDesc.pattern = "p"
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(opDesc), classOf[LogicalOp])
    restored shouldBe a[FilledAreaPlotOpDesc]
    val fp = restored.asInstanceOf[FilledAreaPlotOpDesc]
    fp.x shouldBe "area_x"
    fp.y shouldBe "area_y"
    fp.lineGroup shouldBe "grp"
    fp.color shouldBe "c"
    fp.facetColumn shouldBe true
    fp.pattern shouldBe "p"
  }
}
