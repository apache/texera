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

package org.apache.texera.amber.operator.visualization.polarChart

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.util.Try

class PolarChartOpDescSpec extends AnyFlatSpec with Matchers {

  "PolarChartOpDesc.operatorInfo" should
    "advertise the name and Scientific visualization group" in {
    val info = (new PolarChartOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Polar Chart"
    info.operatorDescription shouldBe "Displays data points in a polar scatter plot"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "PolarChartOpDesc" should "default r and theta to the empty string" in {
    val d = new PolarChartOpDesc
    d.r shouldBe ""
    d.theta shouldBe ""
  }

  "PolarChartOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new PolarChartOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "PolarChartOpDesc.generatePythonCode" should "emit a Plotly Scatterpolargl figure" in {
    val d = new PolarChartOpDesc
    d.r = "radius"
    d.theta = "angle"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.graph_objects")
    code should include("go.Scatterpolargl(")
  }

  "PolarChartOpDesc" should "round-trip r and theta through the polymorphic base" in {
    val d = new PolarChartOpDesc
    d.r = "radius"
    d.theta = "angle"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[PolarChartOpDesc]
    val p = restored.asInstanceOf[PolarChartOpDesc]
    p.r shouldBe "radius"
    p.theta shouldBe "angle"
  }

  // A source read from Arrow hands over nullable dtypes such as Int64, which
  // np.issubdtype cannot interpret; the check must draw them, not raise.
  "PolarChartOpDesc.generateStandaloneCode" should
    "draw nullable numeric columns and refuse non-numeric ones" in {
    val python = resolvePythonExecutable().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandasAndPlotly(python)) {
      cancel(s"'$python' cannot import pandas and plotly; skipping runtime verification")
    }

    val d = new PolarChartOpDesc
    d.r = "r"
    d.theta = "theta"
    val moduleFile = Files.createTempFile("polar_standalone_", ".py")
    val driverFile = Files.createTempFile("polar_driver_", ".py")
    try {
      Files.write(moduleFile, d.generateStandaloneCode().getBytes(StandardCharsets.UTF_8))
      Files.write(driverFile, driverScript.getBytes(StandardCharsets.UTF_8))

      val process = new ProcessBuilder(python, driverFile.toString, moduleFile.toString)
        .redirectErrorStream(true)
        .start()
      if (!process.waitFor(120, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        fail("Polar driver timed out after 120s")
      }
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      withClue(s"Driver output:\n$output\n") {
        process.exitValue() shouldBe 0
        val verdicts = "CASE (\\S+) (\\S+)".r
          .findAllMatchIn(output)
          .map(m => m.group(1) -> m.group(2))
          .toMap
        verdicts shouldBe Map(
          "numpy" -> "CHART",
          "nullable" -> "CHART",
          "boolean" -> "NOT_NUMERIC",
          "text" -> "NOT_NUMERIC"
        )
      }
    } finally {
      Try(Files.deleteIfExists(moduleFile))
      Try(Files.deleteIfExists(driverFile))
      ()
    }
  }

  // Runs the exported block over one frame per case and reports the page it
  // wrote, so a check that raises instead of answering fails here.
  private val driverScript: String =
    """import pathlib
      |import sys
      |import tempfile
      |
      |import pandas as pd
      |import plotly.graph_objects as go
      |
      |source = pathlib.Path(sys.argv[1]).read_text()
      |
      |cases = {
      |    "numpy": pd.DataFrame({"r": [1, 2], "theta": [0, 90]}),
      |    "nullable": pd.DataFrame(
      |        {"r": pd.array([1, 2], dtype="Int64"), "theta": pd.array([0, 90], dtype="Int64")}
      |    ),
      |    "boolean": pd.DataFrame({"r": [True, False], "theta": [0, 90]}),
      |    "text": pd.DataFrame({"r": [1, 2], "theta": ["a", "b"]}),
      |}
      |
      |for name, frame in cases.items():
      |    directory = tempfile.mkdtemp()
      |    scope = {
      |        "in1df": frame,
      |        "outputHtml": directory + "/chart.html",
      |        "outputJson": directory + "/chart.json",
      |        "pd": pd,
      |        "go": go,
      |    }
      |    exec(compile(source, "standalone", "exec"), scope)
      |    page = pathlib.Path(scope["outputHtml"]).read_text()
      |    if "Selected columns must be numeric" in page:
      |        verdict = "NOT_NUMERIC"
      |    elif "<h3>" in page:
      |        verdict = "OTHER_PAGE"
      |    else:
      |        verdict = "CHART"
      |    print("CASE %s %s" % (name, verdict))
      |""".stripMargin

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
}
