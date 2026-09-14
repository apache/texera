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

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.regex.RegexOpDesc
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.typecasting.{TypeCastingOpDesc, TypeCastingUnit}
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
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
  * one script and one directory has nowhere to show. Three of them are pinned
  * here: a column named after a placeholder, two charts writing one file, and a
  * branch that ends the process the other branches still need.
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

  private def csvSource(id: String): CSVScanSourceOpDesc = {
    val op = new CSVScanSourceOpDesc
    // The generated read strips this to its basename, and run() below gives the
    // script the temp directory as its cwd, so the plain name resolves.
    op.fileName = Some(CsvName)
    op.setOperatorId(id)
    op
  }

  private def link(from: LogicalOp, to: LogicalOp): LogicalLink =
    LogicalLink(from.operatorIdentifier, PortIdentity(0), to.operatorIdentifier, PortIdentity(0))

  /** Translates the plan, writes it next to the CSV it reads, and runs it.
    * Returns the directory it ran in, which holds whatever files it wrote, and
    * what it printed.
    */
  private def run(csv: String, ops: List[LogicalOp], links: List[LogicalLink]): (Path, String) = {
    val dir = Files.createTempDirectory("translated-plan-")
    val csvPath = dir.resolve(CsvName)
    Files.write(csvPath, csv.getBytes(StandardCharsets.UTF_8))

    // The service compiles before translating, so a column's declared type
    // reaches the operator that needs it. The compiler reads the source from
    // here, so it takes the full path; the generated read strips it back.
    ops.foreach {
      case scan: CSVScanSourceOpDesc => scan.fileName = Some(csvPath.toString)
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

  // The files the plan wrote, script and input aside.
  private def written(dir: Path): Seq[String] =
    Option(dir.toFile.list()).toSeq.flatten
      .filterNot(name => name == CsvName || name == "script.py")
      .sorted

  /** Four hops, so every variable the translator assigns is read by the next
    * fragment rather than by the print at the end. A two-operator plan cannot
    * show a chain threaded wrong, since df1 is the only name to get right.
    */
  it should "carry a frame through a chain of operators" in {
    val source = csvSource("source")
    val filter = new SpecializedFilterOpDesc
    filter.setOperatorId("filter")
    filter.predicates = List(new FilterPredicate("amount", ComparisonType.GREATER_THAN, "1"))
    val sort = new SortOpDesc
    sort.setOperatorId("sort")
    val criterion = new SortCriteriaUnit
    criterion.attributeName = "amount"
    criterion.sortPreference = SortPreference.DESC
    sort.attributes = List(criterion)
    val limit = new LimitOpDesc
    limit.setOperatorId("limit")
    limit.limit = 2
    val projection = new ProjectionOpDesc
    projection.setOperatorId("projection")
    projection.attributes ++= List(new AttributeUnit("label", "label"))

    val chain = List(source, filter, sort, limit, projection)
    val (_, stdout) = run(
      "label,amount\nant,3\nbee,1\ncat,5\ndog,2\n",
      chain,
      chain.sliding(2).map { case Seq(from, to) => link(from, to) }.toList
    )
    // bee is filtered out, dog falls outside the limit, and cat sorts above ant.
    stdout should not include "bee"
    stdout should not include "dog"
    stdout.indexOf("cat") should be < stdout.indexOf("ant")
  }

  /** A CSV carries no types, so a blank cell widens an integer column and its 6
    * would be matched as "6.0". Only a whole plan shows it: the type belongs to
    * the source, a hop away with a file in between.
    */
  it should "match an integer column on the type its source declared" in {
    val source = csvSource("source")
    val regex = new RegexOpDesc
    regex.setOperatorId("regex")
    regex.attribute = "n"
    regex.regex = "^6$"
    val (_, stdout) = run(
      "label,n\nant,6\nbee,\ncat,70\n",
      List(source, regex),
      List(link(source, regex))
    )
    stdout should include("ant")
    stdout should not include "cat"
  }

  /** A timestamp column read from a file is text and renders itself; only a cast
    * makes one a real datetime, and Python's str() then writes no fraction at all
    * on a whole second where `Timestamp.toString` writes ".0". One operator cannot
    * show it, since the executor casts each column once from its original value.
    */
  it should "render a cast timestamp the way the engine's toString does" in {
    val source = csvSource("source")
    val cast = new TypeCastingOpDesc
    cast.setOperatorId("cast")
    val unit = new TypeCastingUnit
    unit.attribute = "ts"
    unit.resultType = AttributeType.TIMESTAMP
    cast.typeCastingUnits = List(unit)
    val regex = new RegexOpDesc
    regex.setOperatorId("regex")
    regex.attribute = "ts"
    regex.regex = "\\.0$"

    val (_, stdout) = run(
      "label,ts\nant,2024-01-07 00:00:00\nbee,2024-03-15 08:30:00.123\n",
      List(source, cast, regex),
      List(link(source, cast), link(cast, regex))
    )
    // ant's whole second grows the ".0" the engine writes; bee's fraction keeps
    // three digits rather than the six Python pads to, so it does not match.
    stdout should include("ant")
    stdout should not include "bee"
  }

  /** A variadic port is the one placeholder a fragment cannot name, so the list
    * the translator writes in its place is only exercised by a real plan.
    */
  it should "hand a variadic port the frames its upstreams produced" in {
    val sources = List("left", "right").map(csvSource)
    val union = new UnionOpDesc
    union.setOperatorId("union")
    val (_, stdout) = run(
      "label,amount\nant,3\nbee,1\n",
      sources :+ union,
      sources.map(source => link(source, union))
    )
    // Both upstreams read the same file, so each row arrives twice.
    stdout.sliding("ant".length).count(_ == "ant") shouldBe 2
  }

  /** The substitution rewrites the variable a fragment reads with, not the
    * column name it asks that variable for. A fragment tested on its own never
    * shows the difference, because the harness binds the placeholder to itself.
    */
  it should "keep a column whose name matches a placeholder" in {
    val source = csvSource("source")
    val projection = new ProjectionOpDesc
    projection.setOperatorId("projection")
    projection.attributes ++= List(new AttributeUnit("in1df", "kept"))
    val (_, stdout) = run(
      "in1df,other\n1,a\n2,b\n",
      List(source, projection),
      List(link(source, projection))
    )
    stdout should include("kept")
  }

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
