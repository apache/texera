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

package org.apache.texera.amber.operator.visualization.gaugeChart

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

class GaugeChartOpDescSpec extends AnyFlatSpec with Matchers {

  "GaugeChartOpDesc.operatorInfo" should
    "advertise the name and Financial visualization group" in {
    val info = (new GaugeChartOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Gauge Chart"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "GaugeChartOpDesc" should
    "default value to empty, delta/threshold to unset and steps to an empty list" in {
    val d = new GaugeChartOpDesc
    d.value shouldBe ""
    d.delta shouldBe None
    d.threshold shouldBe None
    d.steps shouldBe empty
  }

  "GaugeChartOpDesc.getOutputSchemas" should
    "produce a single html-content STRING column keyed by the declared output port" in {
    val op = new GaugeChartOpDesc
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING)
    )
  }

  "GaugeChartOpDesc.generatePythonCode" should "emit a Plotly Indicator figure" in {
    val d = new GaugeChartOpDesc
    d.value = "score"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("plotly.graph_objects")
    code should include("go.Indicator(")
  }

  "GaugeChartOpDesc" should
    "round-trip value/delta/threshold and steps through the polymorphic base" in {
    val d = new GaugeChartOpDesc
    d.value = "v"
    d.delta = Some(40)
    d.threshold = Some(80)
    val step = new GaugeChartSteps
    step.start = Some(0)
    step.end = Some(50)
    d.steps = List(step)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[GaugeChartOpDesc]
    val g = restored.asInstanceOf[GaugeChartOpDesc]
    g.value shouldBe "v"
    g.delta shouldBe Some(40)
    g.threshold shouldBe Some(80)
    g.steps should have length 1
    g.steps.head.start shouldBe Some(0)
    g.steps.head.end shouldBe Some(50)
  }

  /** An unset field has to arrive as Python's `None` for the template's
    * `is not None` guards to read it as "not configured".
    */
  "GaugeChartOpDesc.generatePythonCode" should
    "assign delta and threshold as numbers, and None when they are unset" in {
    val d = new GaugeChartOpDesc
    d.value = "score"
    d.generatePythonCode() should include("delta_ref = None")
    d.generatePythonCode() should include("threshold_val = None")
    d.delta = Some(40)
    d.threshold = Some(80.5)
    val code = d.generatePythonCode()
    code should include("delta_ref = 40.0")
    code should include("threshold_val = 80.5")
  }

  it should "emit only the steps whose bounds are both filled in" in {
    val d = new GaugeChartOpDesc
    d.value = "score"
    val complete = new GaugeChartSteps
    complete.start = Some(0)
    complete.end = Some(50)
    val halfFilled = new GaugeChartSteps
    halfFilled.start = Some(50)
    d.steps = List(complete, halfFilled)
    val code = d.generatePythonCode()
    code should include("""valid_steps = [{"start": 0.0, "end": 50.0}]""")
  }

  it should "emit no steps when the payload sets steps to null" in {
    // Steps is optional, so an explicit null leaves the field null rather than an empty
    // list; that is no steps, not a failure.
    val d = objectMapper
      .readValue(
        """{"operatorType": "GaugeChart", "value": "score", "steps": null}""",
        classOf[LogicalOp]
      )
      .asInstanceOf[GaugeChartOpDesc]
    d.steps shouldBe null

    d.generatePythonCode() should include("valid_steps = []")
  }

  // The operator draws inside a try and answers anything it did not foresee with
  // a page. Without the same catch, a column that is not in the table reaches
  // pandas as a KeyError and ends the whole exported run, not just this chart.
  "GaugeChartOpDesc.generateStandaloneCode" should
    "answer what it cannot draw the way the operator answers it" in {
    val python = resolvePythonExecutable().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandasAndPlotly(python)) {
      cancel(s"'$python' cannot import pandas and plotly; skipping runtime verification")
    }

    val d = new GaugeChartOpDesc
    d.value = "score"
    val moduleFile = Files.createTempFile("gauge_standalone_", ".py")
    val driverFile = Files.createTempFile("gauge_driver_", ".py")
    try {
      Files.write(moduleFile, d.generateStandaloneCode().getBytes(StandardCharsets.UTF_8))
      Files.write(driverFile, driverScript.getBytes(StandardCharsets.UTF_8))

      val process = new ProcessBuilder(python, driverFile.toString, moduleFile.toString)
        .redirectErrorStream(true)
        .start()
      if (!process.waitFor(120, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        fail("Gauge driver timed out after 120s")
      }
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      withClue(s"Driver output:\n$output\n") {
        process.exitValue() shouldBe 0
        val verdicts = "CASE (\\S+) (\\S+)".r
          .findAllMatchIn(output)
          .map(m => m.group(1) -> m.group(2))
          .toMap
        verdicts shouldBe Map(
          "missing" -> "GENERAL_ERROR",
          "empty" -> "EMPTY",
          "nulls" -> "NO_ROWS",
          "good" -> "CHART"
        )
      }
    } finally {
      Try(Files.deleteIfExists(moduleFile))
      Try(Files.deleteIfExists(driverFile))
      ()
    }
  }

  // Runs the exported block over one frame per case and reports the page it
  // wrote, so a branch that raises instead of answering fails here.
  private val driverScript: String =
    """import pathlib
      |import sys
      |import tempfile
      |
      |import pandas as pd
      |
      |source = pathlib.Path(sys.argv[1]).read_text()
      |
      |cases = {
      |    "good": pd.DataFrame({"score": [42.0]}),
      |    "missing": pd.DataFrame({"other": [42.0]}),
      |    "empty": pd.DataFrame({"score": []}),
      |    "nulls": pd.DataFrame({"score": [None, None]}),
      |}
      |
      |for name, frame in cases.items():
      |    directory = tempfile.mkdtemp()
      |    scope = {
      |        "in1df": frame,
      |        "outputHtml": directory + "/chart.html",
      |        "outputJson": directory + "/chart.json",
      |        "pd": pd,
      |    }
      |    exec(compile(source, "standalone", "exec"), scope)
      |    page = pathlib.Path(scope["outputHtml"]).read_text()
      |    if "General error" in page:
      |        verdict = "GENERAL_ERROR"
      |    elif "Input table is empty" in page:
      |        verdict = "EMPTY"
      |    elif "No non-null rows" in page:
      |        verdict = "NO_ROWS"
      |    elif "Gauge chart is not available" in page:
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
