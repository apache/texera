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

import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.operator.visualization.bulletChart.{
  BulletChartOpDesc,
  BulletChartStepDefinition
}
import org.apache.texera.amber.operator.visualization.continuousErrorBands.{
  BandConfig,
  ContinuousErrorBandsOpDesc
}
import org.apache.texera.amber.operator.visualization.lineChart.LineMode
import org.apache.texera.amber.operator.visualization.networkGraph.NetworkGraphOpDesc
import org.apache.texera.amber.operator.visualization.treeplot.TreePlotOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.PythonTestSupport.{
  NeedsPythonPackages,
  canImport,
  configuredPython,
  withDriver
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
  * Runs the Python four charts generate for the operator, and the script each one
  * exports, over the same tables.
  *
  * Both paths of these charts are assembled from shared Python fragments, and a
  * fragment that reads a name its caller never bound still compiles: only running
  * it fails. Each case runs both paths and expects the same figures, or the same
  * refusal; the values pinned below keep the two from drifting together. Each
  * chart has settings whose text carries a quote, which the operator decodes at
  * runtime and the exported script has to spell as an escaped literal.
  */
class RuntimeStandaloneParitySpec extends AnyFlatSpec with Matchers {

  private def networkGraph: NetworkGraphOpDesc = {
    val op = new NetworkGraphOpDesc
    op.source = "from's \"node\""
    op.destination = "to's \"node\""
    op.title = "Friends \"graph\""
    op
  }

  private def bulletChart: BulletChartOpDesc = {
    val op = new BulletChartOpDesc
    op.value = "it's \"score\""
    op.deltaReference = Some(50)
    op.thresholdValue = Some(80)
    // The second step runs backwards, so it is reported rather than drawn; the last
    // has no end, so it reaches neither script.
    op.steps = Seq(
      new BulletChartStepDefinition(Some(0), Some(40)),
      new BulletChartStepDefinition(Some(60), Some(40)),
      new BulletChartStepDefinition(Some(40), Some(100)),
      new BulletChartStepDefinition(Some(100), None)
    ).asJava
    op
  }

  private def treePlot: TreePlotOpDesc = {
    val op = new TreePlotOpDesc
    op.edgeListColumn = "edge \"pairs\""
    op
  }

  private def errorBands: ContinuousErrorBandsOpDesc = {
    val named = new BandConfig
    named.xValue = "t"
    named.yValue = "mean \"v\""
    named.yUpper = "hi"
    named.yLower = "lo"
    named.name = "band's one"
    named.color = "#ff0000"
    named.fillColor = "rgba(255, 0, 0, 0.2)"
    named.mode = LineMode.LINE
    // Left unnamed and uncoloured: named after its y column, in plotly's own colours.
    val unnamed = new BandConfig
    unnamed.xValue = "t"
    unnamed.yValue = "other"
    unnamed.yUpper = "other_hi"
    unnamed.yLower = "other_lo"
    val op = new ContinuousErrorBandsOpDesc
    op.xLabel = "Time \"s\""
    op.yLabel = "it's y"
    op.bands = Seq(named, unnamed).asJava
    op
  }

  /** Both paths of every chart, keyed by the names the driver's tables use. The
    * exported script gets the imports the translator writes above it.
    */
  private def jobs: String = {
    val charts: Seq[(String, PythonVisualizationOpDesc)] = Seq(
      "network_graph" -> networkGraph,
      "bullet_chart" -> bulletChart,
      "tree_plot" -> treePlot,
      "error_bands" -> errorBands
    )
    val root = objectMapper.createObjectNode()
    charts.foreach {
      case (name, op) =>
        val imports = "import pandas as pd" +: op.standaloneImports()
        root
          .putObject(name)
          .put("operator", op.generatePythonCode())
          .put("script", (imports :+ op.generateStandaloneCode()).mkString("\n"))
    }
    objectMapper.writeValueAsString(root)
  }

  /** What one path left for one table: the figures it drew, the reason on any error
    * page, the step errors listed under its charts, and the traceback if it raised.
    */
  private case class Drawn(
      figures: Seq[JsonNode],
      reasons: Seq[String],
      stepErrors: Seq[String],
      failure: Option[String]
  )

  private case class Outcome(operator: Drawn, script: Drawn)

  private def drawn(node: JsonNode): Drawn =
    Drawn(
      node.get("figures").asScala.toSeq,
      node.get("reasons").asScala.map(_.asText).toSeq,
      node.get("stepErrors").asScala.map(_.asText).toSeq,
      Option(node.get("failure")).filterNot(_.isNull).map(_.asText)
    )

  /** Every case's outcome by `chart/case`. One run for the whole suite, so pandas,
    * plotly and networkx are imported once.
    */
  private lazy val outcomes: Map[String, Outcome] = {
    val python = configuredPython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    // networkx stays out of the gate: only the network graph imports it, inside each
    // path's try, so without it those two cases fail instead of all ten cancelling.
    if (!canImport(python, "pandas", "plotly")) {
      cancel(s"'$python' cannot import pandas and plotly; skipping runtime verification")
    }

    withDriver(
      python,
      "chart_parity_jobs_",
      "chart_parity_driver_",
      jobs,
      driverScript,
      "Parity driver timed out after 120s"
    ) { (exitCode, output) =>
      withClue(s"Driver output:\n$output\n") {
        exitCode shouldBe 0
      }
      "(?m)^CASE (\\S+) (.*)$".r
        .findAllMatchIn(output)
        .map { m =>
          val outcome = objectMapper.readTree(m.group(2))
          m.group(1) -> Outcome(drawn(outcome.get("operator")), drawn(outcome.get("script")))
        }
        .toMap
    }
  }

  /** The case's outcome, once both paths are known to have run to the end. */
  private def outcome(name: String): Outcome = {
    val result = outcomes.getOrElse(name, fail(s"The driver reported nothing for $name"))
    withClue(s"$name, operator: ") { result.operator.failure shouldBe None }
    withClue(s"$name, exported script: ") { result.script.failure shouldBe None }
    result
  }

  /** Both paths drew nothing for the case and gave this reason instead. */
  private def refusedAlike(name: String, reason: String): Unit = {
    val refused = outcome(name)
    Seq("operator" -> refused.operator, "exported script" -> refused.script).foreach {
      case (path, result) =>
        withClue(s"$name, $path: ") {
          result.figures shouldBe empty
          result.reasons shouldBe Seq(reason)
        }
    }
  }

  private def texts(node: JsonNode): Seq[String] = node.asScala.map(_.asText).toSeq

  // A gap between line segments is a null.
  private def numbers(node: JsonNode): Seq[Option[Double]] =
    node.asScala.map(n => Option.when(!n.isNull)(n.asDouble)).toSeq

  "NetworkGraphOpDesc" should
    "draw the same graph in the operator and the exported script" taggedAs NeedsPythonPackages in {
    val drawn = outcome("network_graph/drawn")
    drawn.script.figures shouldBe drawn.operator.figures
    drawn.operator.figures should have size 1
    val graph = drawn.operator.figures.head
    graph.at("/layout/title/text").asText shouldBe "<br>Friends \"graph\""
    // The row without a source names no edge, so d never becomes a node and the
    // other three close a triangle.
    texts(graph.at("/data/1/text")) shouldBe
      Seq("a: 2 connections.", "b: 2 connections.", "c: 2 connections.")
    numbers(graph.at("/data/1/marker/color")) shouldBe Seq(Some(2.0), Some(2.0), Some(2.0))
    numbers(graph.at("/data/0/x")).grouped(3).map(_.last).toSeq shouldBe Seq(None, None, None)
  }

  it should "refuse a table with no complete edge the same way in both" taggedAs NeedsPythonPackages in {
    refusedAlike(
      "network_graph/no_complete_edge",
      "Table should not have any empty/null values or fields."
    )
  }

  "BulletChartOpDesc" should
    "draw the same first ten charts in the operator and the exported script" taggedAs NeedsPythonPackages in {
    val drawn = outcome("bullet_chart/drawn")
    drawn.script.figures shouldBe drawn.operator.figures
    val indicators = drawn.operator.figures.map(_.at("/data/0"))
    // Twelve values survive the dropped hole, and only the first ten are drawn.
    indicators.map(_.get("value").asDouble) shouldBe
      Seq(50.0, 150.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0)
    indicators.foreach { indicator =>
      indicator.at("/title/text").asText shouldBe "it's \"score\""
      indicator.at("/delta/reference").asDouble shouldBe 50.0
      indicator.at("/gauge/threshold/value").asDouble shouldBe 80.0
      // The backwards step is left out, so two remain, shaded light to dark.
      indicator
        .at("/gauge/steps")
        .asScala
        .map(step => (numbers(step.get("range")), step.get("color").asText))
        .toSeq shouldBe Seq(
        (Seq(Some(0.0), Some(40.0)), "hsl(0, 0%, 90.0%)"),
        (Seq(Some(40.0), Some(100.0)), "hsl(0, 0%, 30.0%)")
      )
    }
    // The axis runs a fifth past the largest of the value, the reference, the
    // threshold and the step ends.
    indicators.head.at("/gauge/axis/range/1").asDouble shouldBe 120.0 +- 1e-9
    indicators(1).at("/gauge/axis/range/1").asDouble shouldBe 180.0 +- 1e-9
  }

  // The operator's message has always said ≥ and the exported script's >=.
  it should "list the backwards step under every chart, each path in its own glyph" taggedAs NeedsPythonPackages in {
    val drawn = outcome("bullet_chart/drawn")
    drawn.operator.stepErrors shouldBe Seq.fill(10)("Step 2: start ≥ end (60.0 ≥ 40.0)")
    drawn.script.stepErrors shouldBe Seq.fill(10)("Step 2: start >= end (60.0 >= 40.0)")
  }

  it should "refuse a table without the value column the same way in both" taggedAs NeedsPythonPackages in {
    refusedAlike(
      "bullet_chart/missing_column",
      "Column 'it's \"score\"' not found in input table."
    )
  }

  it should "refuse a table whose values are all missing the same way in both" taggedAs NeedsPythonPackages in {
    refusedAlike("bullet_chart/all_null", "No valid data rows found after dropping nulls.")
  }

  "TreePlotOpDesc" should
    "draw the same tree in the operator and the exported script" taggedAs NeedsPythonPackages in {
    val drawn = outcome("tree_plot/drawn")
    drawn.script.figures shouldBe drawn.operator.figures
    drawn.operator.figures should have size 1
    val tree = drawn.operator.figures.head
    // Three rows hold a pair; the missing cell, the text and the triple are skipped.
    tree
      .at("/layout/annotations")
      .asScala
      .map(node => (node.get("text").asText, node.get("x").asDouble, node.get("y").asDouble))
      .toSeq shouldBe Seq(
      ("root", 0.5, 0.0),
      ("left", 0.0, -1.0),
      ("right", 1.0, -1.0),
      ("leaf", 0.0, -2.0)
    )
    numbers(tree.at("/data/0/x")) shouldBe
      Seq(Some(0.5), Some(0.0), None, Some(0.5), Some(1.0), None, Some(0.0), Some(0.0), None)
    numbers(tree.at("/data/0/y")) shouldBe
      Seq(Some(0.0), Some(-1.0), None, Some(0.0), Some(-1.0), None, Some(-1.0), Some(-2.0), None)
    // Too small a tree to grow past the minimum size.
    tree.at("/layout/width").asInt shouldBe 800
    tree.at("/layout/height").asInt shouldBe 600
  }

  it should "refuse a column without a single pair the same way in both" taggedAs NeedsPythonPackages in {
    refusedAlike(
      "tree_plot/no_pairs",
      "No valid [parent, child] pairs found in column edge \"pairs\"."
    )
  }

  "ContinuousErrorBandsOpDesc" should
    "draw the same bands in the operator and the exported script" taggedAs NeedsPythonPackages in {
    val drawn = outcome("error_bands/drawn")
    drawn.script.figures shouldBe drawn.operator.figures
    drawn.operator.figures should have size 1
    val figure = drawn.operator.figures.head
    val traces = figure.get("data").asScala.toSeq
    // Each band is its upper bound, its lower bound filled up to it, then the line.
    traces.map(trace => numbers(trace.get("y")).flatten) shouldBe Seq(
      Seq(1.5, 2.5, 3.5),
      Seq(0.5, 1.5, 2.5),
      Seq(1.0, 2.0, 3.0),
      Seq(3.25, 2.25, 1.25),
      Seq(2.75, 1.75, 0.75),
      Seq(3.0, 2.0, 1.0)
    )
    traces.foreach(trace => numbers(trace.get("x")).flatten shouldBe Seq(1.0, 2.0, 3.0))
    traces.map(_.get("name").asText) shouldBe Seq.fill(3)("band's one") ++ Seq.fill(3)("other")
    traces.map(_.path("fill").asText) shouldBe Seq("", "tonexty", "", "", "tonexty", "")
    traces(1).get("fillcolor").asText shouldBe "rgba(255, 0, 0, 0.2)"
    traces(4).has("fillcolor") shouldBe false
    traces(2).get("mode").asText shouldBe "lines"
    traces(2).at("/line/color").asText shouldBe "#ff0000"
    traces(2).at("/marker/color").asText shouldBe "#ff0000"
    traces(5).get("mode").asText shouldBe "lines+markers"
    traces(5).has("line") shouldBe false
    figure.at("/layout/xaxis/title/text").asText shouldBe "Time \"s\""
    figure.at("/layout/yaxis/title/text").asText shouldBe "it's y"
  }

  it should "refuse an empty table the same way in both" taggedAs NeedsPythonPackages in {
    refusedAlike("error_bands/empty", "input table is empty.")
  }

  // Runs both paths of every chart over each of its tables and prints what each
  // left behind. Only the pytexera import seam is stubbed, as in the operator
  // specs, so the generated module runs unmodified; the exported script runs with
  // its placeholders bound, as in GaugeChartOpDescSpec.
  private val driverScript: String =
    """import base64
      |import json
      |import pathlib
      |import re
      |import sys
      |import tempfile
      |import traceback
      |import types
      |from typing import Iterator, Optional
      |
      |import pandas as pd
      |import plotly.io
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
      |def report(figures, page, failure=None):
      |    for figure in figures:
      |        # plotly's default theme: the same in both paths, and most of the bytes.
      |        figure.get("layout", {}).pop("template", None)
      |    return {
      |        "figures": figures,
      |        "reasons": re.findall(r"<p>Reason[^:]*: (.*?) </p>", page, re.S),
      |        "stepErrors": [
      |            item
      |            for notes in re.findall(r"<b>Step Errors:</b><ul>(.*?)</ul>", page, re.S)
      |            for item in re.findall(r"<li>(.*?)</li>", notes, re.S)
      |        ],
      |        "failure": failure,
      |    }
      |
      |def run_operator(source, table):
      |    # Every chart the operator draws goes through plotly.io.to_html, so the
      |    # figure is read there rather than out of the page around it.
      |    figures = []
      |    def record(fig, *args, **kwargs):
      |        figures.append(json.loads(fig.to_json()))
      |        return ""
      |    to_html = plotly.io.to_html
      |    plotly.io.to_html = record
      |    try:
      |        ns = {"__name__": "generated_operator"}
      |        exec(compile(source, "operator", "exec"), ns)
      |        outputs = ns["ProcessTableOperator"]().process_table(table.copy(), 0)
      |        return report(figures, "".join(output["html-content"] for output in outputs))
      |    except Exception:
      |        return report([], "", traceback.format_exc())
      |    finally:
      |        plotly.io.to_html = to_html
      |
      |def run_script(source, table):
      |    with tempfile.TemporaryDirectory() as directory:
      |        scope = {
      |            "in1df": table.copy(),
      |            "outputHtml": directory + "/chart.html",
      |            "outputJson": directory + "/chart.json",
      |        }
      |        try:
      |            exec(compile(source, "script", "exec"), scope)
      |            page = pathlib.Path(scope["outputHtml"]).read_text(encoding="utf-8")
      |            written = pathlib.Path(scope["outputJson"])
      |            figures = json.loads(written.read_text(encoding="utf-8")) if written.exists() else []
      |        except Exception:
      |            return report([], "", traceback.format_exc())
      |    # One chart is written as an object, several as a list.
      |    return report(figures if isinstance(figures, list) else [figures], page)
      |
      |CASES = {
      |    "network_graph": {
      |        "drawn": pd.DataFrame({
      |            'from\'s "node"': ["a", "a", "b", None],
      |            'to\'s "node"': ["b", "c", "c", "d"],
      |        }),
      |        "no_complete_edge": pd.DataFrame({
      |            'from\'s "node"': ["a", None],
      |            'to\'s "node"': [None, "b"],
      |        }),
      |    },
      |    "bullet_chart": {
      |        "drawn": pd.DataFrame({
      |            'it\'s "score"': [50.0, None, 150.0, 60.0, 70.0, 80.0, 90.0,
      |                              100.0, 110.0, 120.0, 130.0, 140.0, 160.0],
      |        }),
      |        "missing_column": pd.DataFrame({"other": [1.0]}),
      |        "all_null": pd.DataFrame({'it\'s "score"': [None, None]}),
      |    },
      |    "tree_plot": {
      |        "drawn": pd.DataFrame({
      |            'edge "pairs"': ["['root', 'left']", None, "not a pair", "[1, 2, 3]",
      |                             "['root', 'right']", "['left', 'leaf']"],
      |        }),
      |        "no_pairs": pd.DataFrame({'edge "pairs"': [None, "not a pair", "[1, 2, 3]"]}),
      |    },
      |    "error_bands": {
      |        "drawn": pd.DataFrame({
      |            "t": [1, 2, 3],
      |            'mean "v"': [1.0, 2.0, 3.0],
      |            "hi": [1.5, 2.5, 3.5],
      |            "lo": [0.5, 1.5, 2.5],
      |            "other": [3.0, 2.0, 1.0],
      |            "other_hi": [3.25, 2.25, 1.25],
      |            "other_lo": [2.75, 1.75, 0.75],
      |        }),
      |        "empty": pd.DataFrame({
      |            column: []
      |            for column in ["t", 'mean "v"', "hi", "lo", "other", "other_hi", "other_lo"]
      |        }),
      |    },
      |}
      |
      |jobs = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
      |for chart, tables in CASES.items():
      |    for case, table in tables.items():
      |        outcome = {
      |            "operator": run_operator(jobs[chart]["operator"], table),
      |            "script": run_script(jobs[chart]["script"], table),
      |        }
      |        print("CASE %s/%s %s" % (chart, case, json.dumps(outcome)))
      |""".stripMargin
}
