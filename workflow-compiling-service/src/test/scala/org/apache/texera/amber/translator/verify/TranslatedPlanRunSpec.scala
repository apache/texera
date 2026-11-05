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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.operator.visualization.contourPlot.{
  ContourPlotColoringFunction,
  ContourPlotOpDesc
}
import org.apache.texera.amber.operator.visualization.pieChart.PieChartOpDesc
import org.apache.texera.amber.translator.WorkflowToPythonTranslator
import org.apache.texera.amber.translator.verify.tags.IntegrationTest
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlan, LogicalPlanPojo}
import org.apache.texera.common.compiler.{CompilationErrorHandling, WorkflowCompiler}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.{Process, ProcessLogger}

/** The translated script itself, run as one program.
  *
  * Every other verify spec runs a single operator's fragment with the harness
  * binding its placeholders, so a fault that appears only once fragments share
  * one script and one directory has nowhere to show. The ones the engine can
  * answer too run in amber's WorkflowParitySpec, against the engine. The two
  * left here are the script's own: two charts writing one file, and a branch
  * that ends the process the other branches still need.
  *
  * Tagged @IntegrationTest with the rest of the specs that fork Python.
  */
@IntegrationTest
class TranslatedPlanRunSpec extends AnyFlatSpec with Matchers {

  private val CsvName = "input.csv"

  // The same chain StandaloneRunner uses: the env var CI and the shared venv
  // set, then the conventional name.
  private val python =
    sys.env.get("UDF_PYTHON_PATH").filter(_.nonEmpty).getOrElse("python3.12")

  private def csvSource(id: String, file: String = CsvName): CSVScanSourceOpDesc = {
    val op = new CSVScanSourceOpDesc
    // The generated read strips this to its basename, and run() below gives the
    // script the temp directory as its cwd, so the plain name resolves.
    op.fileName = Some(file)
    op.setOperatorId(id)
    op
  }

  private def link(from: LogicalOp, to: LogicalOp): LogicalLink =
    LogicalLink(from.operatorIdentifier, PortIdentity(0), to.operatorIdentifier, PortIdentity(0))

  private def run(csv: String, ops: List[LogicalOp], links: List[LogicalLink]): (Path, String) =
    run(Map(CsvName -> csv), ops, links)

  /** Translates the plan, writes it next to the CSVs it reads, and runs it.
    * Returns the directory it ran in, which holds whatever files it wrote, and
    * what it printed.
    */
  private def run(
      csvs: Map[String, String],
      ops: List[LogicalOp],
      links: List[LogicalLink]
  ): (Path, String) = {
    val dir = Files.createTempDirectory("translated-plan-")
    csvs.foreach {
      case (name, content) =>
        Files.write(dir.resolve(name), content.getBytes(StandardCharsets.UTF_8))
    }

    // The service compiles before translating, so a column's declared type
    // reaches the operator that needs it. The compiler reads the source from
    // here, so it takes the full path; the generated read strips it back.
    ops.foreach {
      case scan: CSVScanSourceOpDesc => scan.fileName = scan.fileName.map(dir.resolve(_).toString)
      case _                         => ()
    }
    val schemas = new WorkflowCompiler(new WorkflowContext(workflowId = WorkflowIdentity(0)))
      .compile(
        LogicalPlanPojo(ops, links, opsToViewResult = List.empty, opsToReuseResult = List.empty),
        CompilationErrorHandling.Lenient
      )
      .operatorIdToOutputSchemas
    val script = new WorkflowToPythonTranslator().translate(LogicalPlan(ops, links), schemas)
    val scriptPath = dir.resolve("script.py")
    Files.write(scriptPath, script.getBytes(StandardCharsets.UTF_8))

    val out = ArrayBuffer.empty[String]
    val err = ArrayBuffer.empty[String]
    val exit = Process(Seq(python, scriptPath.toString), Some(dir.toFile))
      .!(ProcessLogger(line => out += line, line => err += line))
    val stdout = out.mkString("\n")
    withClue(
      s"script: $scriptPath\n--- stdout ---\n$stdout\n--- stderr ---\n${err.mkString("\n")}\n"
    ) {
      exit shouldBe 0
    }
    (dir, stdout)
  }

  // The files the plan wrote, script and inputs aside.
  private def written(dir: Path): Seq[String] =
    Option(dir.toFile.list()).toSeq.flatten
      .filterNot(name => name.endsWith(".csv") || name == "script.py")
      .sorted

  /** Two charts run in one directory, so a name either had chosen for itself
    * would leave one file where the plan drew two pictures.
    */
  it should "give two charts in one plan a file each" in {
    val source = csvSource("source")
    val charts = List("pie1", "pie2").map { id =>
      val op = new PieChartOpDesc
      op.name = "label"
      op.value = "amount"
      op.setOperatorId(id)
      op
    }
    val (dir, _) = run(
      "label,amount\na,1\nb,2\n",
      source :: charts,
      charts.map(chart => link(source, chart))
    )
    written(dir) shouldBe Seq(
      "pie_chart_1.html",
      "pie_chart_1.json",
      "pie_chart_2.html",
      "pie_chart_2.json"
    )
  }

  /** A chart with nothing to draw writes its reason and stops there. Ending the
    * process would take the sibling branches with it, and the frames the plan
    * prints at the end are the last thing to run.
    */
  it should "let the rest of a plan finish when one chart cannot be drawn" in {
    val source = csvSource("source")
    val contour = new ContourPlotOpDesc
    contour.setOperatorId("contour")
    contour.x = "x"
    contour.y = "y"
    contour.z = "z"
    contour.coloringMethod = ContourPlotColoringFunction.HEATMAP
    val distinct = new DistinctOpDesc
    distinct.setOperatorId("distinct")
    // Collinear points: the contour has no area to interpolate over.
    val (dir, stdout) = run(
      "x,y,z\n0,0,1\n1,1,2\n2,2,3\n",
      List(source, contour, distinct),
      List(link(source, contour), link(source, distinct))
    )
    stdout should include("[Distinct]")
    // The error page and nothing else: no chart was drawn, so no JSON figure.
    written(dir) shouldBe Seq("contour_plot_1.html")
  }
}
