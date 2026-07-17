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

import com.fasterxml.jackson.annotation.JsonSubTypes
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.translator.verify.tags.IntegrationTest
import org.scalatest.ParallelTestExecution
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Auto-discovered behavioral-parity tests: for every operator registered
  * with [[LogicalOp]]'s `@JsonSubTypes` that implements
  * [[StandaloneCodeGenerator]], emit a test that runs both Path A (Texera
  * exec) and Path B (translator-generated Python via [[StandaloneRunner]])
  * and asserts their outputs are equivalent.
  *
  * Dispatch is auto-first: [[TransformVerificationRunner]] classifies each
  * non-source transform as `Runnable("auto")` (auto-configured fixture),
  * `Runnable("curated")` (hand-written fixture from [[CuratedHandlers]]),
  * or `Flagged(reason)` (shown as ignored with the reason in the test name).
  * Sources route to [[SourceCategoryRunner]] unchanged.
  *
  * No edits to this spec are needed when a new operator is added — reflection
  * discovers it automatically via `@JsonSubTypes`. The tier label appears in
  * the test name so the report shows which path exercised each operator.
  *
  * Requires Python 3 with pandas on the [[Comparator]] / [[StandaloneRunner]]
  * resolution chain (`UDF_PYTHON_PATH` env var, then `python3.12`).
  */
// Tagged @IntegrationTest: this is the only verify spec that forks a real
// Python process end-to-end, so CI routes it to the Python-provisioned
// integration job (see workflow-compiling-service/build.sbt WCS_TEST_FILTER).
@IntegrationTest
class OperatorBehaviorSpec extends AnyFlatSpec with Matchers with ParallelTestExecution {

  // Build the test list at class construction. Each branch below registers
  // one test (`in` for runnable, `ignore` for skipped) so the test report
  // shows every translator-eligible operator and why it did or didn't run.
  OperatorBehaviorSpec.discoverStandaloneOperators().foreach { opClass =>
    val name = opClass.getSimpleName

    if (!OperatorBehaviorSpec.isSelected(name)) {
      // Local-dev filter via VERIFY_SKIP / VERIFY_ONLY env vars (see the
      // object below). Empty (the CI default) selects everything; when set,
      // deselected operators are marked `ignore` so they don't run but still
      // show up in the report.
      name should "SKIPPED — family not yet in the verified set (override via VERIFY_ONLY/VERIFY_SKIP)" ignore {}
    } else if (classOf[SourceOperatorDescriptor].isAssignableFrom(opClass)) {
      // Sources keep their handler-per-source design: each needs a real file
      // in its specific format, which a generic fixture can't supply.
      if (SourceCategoryRunner.canRun(opClass)) {
        name should "produce equivalent output in Texera and standalone Python (source)" in {
          SourceCategoryRunner.run(opClass)
        }
      } else {
        name should s"FLAGGED — ${SourceCategoryRunner.flagReason(opClass)}" ignore {}
      }
    } else {
      TransformVerificationRunner.disposition(opClass) match {
        case TransformVerificationRunner.Runnable(tier) =>
          name should s"produce equivalent output in Texera and standalone Python ($tier)" in {
            TransformVerificationRunner.run(opClass)
          }
        case TransformVerificationRunner.Flagged(reason) =>
          name should s"FLAGGED — $reason" ignore {
            // Reason is in the test name so the report carries it; the
            // coverage table in ConfigCoverageSpec aggregates these.
          }
      }
    }
  }
}

object OperatorBehaviorSpec {

  // Selection knobs. Case-sensitive substrings matched against the operator's
  // simple name decide which operators run.
  //
  // Precedence (first match wins):
  //   1. VERIFY_ONLY env set  -> run ONLY operators matching it.
  //   2. VERIFY_SKIP env set   -> run everything EXCEPT operators matching it.
  //   3. Default (local AND CI) -> skip DefaultLocalSkip: the operator families
  //                                not yet ready for verification (ML / viz /
  //                                external-source / UDF). Same everywhere, so
  //                                CI only exercises the vetted core operators.
  //
  // As a family becomes ready, delete it from DefaultLocalSkip to start
  // verifying it (local + CI). Override ad hoc with VERIFY_ONLY / VERIFY_SKIP.
  private def patterns(envVar: String): Seq[String] =
    sys.env.getOrElse(envVar, "").split(",").iterator.map(_.trim).filter(_.nonEmpty).toSeq

  // Families not yet ready for verification — skipped by default everywhere
  // (local + CI) until they're worked on. ML uses family substrings (Sklearn /
  // HuggingFace catch every variant); viz / source / UDF ops are listed by exact
  // name because substrings like "Chart"/"Plot" would also match core viz ops we
  // want to keep (BarChart, DotPlot, ...).
  private val DefaultLocalSkip: Seq[String] = Seq(
    "Sklearn",
    "HuggingFace",
    "MachineLearningScorer",
    "FigureFactoryTableOpDesc",
    "FilledAreaPlotOpDesc",
    "FunnelPlotOpDesc",
    "GanttChartOpDesc",
    "GaugeChartOpDesc",
    "HeatMapOpDesc",
    "HierarchyChartOpDesc",
    "HistogramChartOpDesc",
    "Histogram2DOpDesc",
    "HtmlVizOpDesc",
    "LineChartOpDesc",
    "NestedTableOpDesc",
    "NetworkGraphOpDesc",
    "ParallelCoordinatesPlotOpDesc",
    "PieChartOpDesc",
    "PolarChartOpDesc",
    "QuiverPlotOpDesc",
    "RadarChartOpDesc",
    "RadarPlotOpDesc",
    "RangeSliderOpDesc",
    "SankeyDiagramOpDesc",
    "Scatter3dChartOpDesc",
    "ScatterplotOpDesc",
    "StripChartOpDesc",
    "TablesPlotOpDesc",
    "TernaryContourOpDesc",
    "TernaryPlotOpDesc",
    "TimeSeriesOpDesc",
    "TreePlotOpDesc",
    "UrlVizOpDesc",
    "VolcanoPlotOpDesc",
    "WaterfallChartOpDesc",
    "WindRoseChartOpDesc",
    "WordCloudOpDesc",
    "FileListerSourceOpDesc",
    "AsterixDBSourceOpDesc",
    "MySQLSourceOpDesc",
    "PostgreSQLSourceOpDesc",
    "TwitterFullArchiveSearchSourceOpDesc",
    "TwitterSearchSourceOpDesc",
    "RedditSearchSourceOpDesc",
    "PythonLambdaFunctionOpDesc",
    "PythonTableReducerOpDesc",
    "JavaUDFOpDesc",
    "RUDFOpDesc",
    "RUDFSourceOpDesc"
  )

  private lazy val onlyPatterns: Seq[String] = patterns("VERIFY_ONLY")

  // Effective skip list per the precedence above. Empty means "skip nothing".
  private lazy val skipPatterns: Seq[String] = {
    val explicitSkip = patterns("VERIFY_SKIP")
    if (onlyPatterns.nonEmpty || explicitSkip.nonEmpty) explicitSkip
    else DefaultLocalSkip
  }

  /** True if `name` should run under the current selection (see precedence above). */
  def isSelected(name: String): Boolean = {
    val included = onlyPatterns.isEmpty || onlyPatterns.exists(name.contains)
    val excluded = skipPatterns.exists(name.contains)
    included && !excluded
  }

  /**
    * Enumerates every concrete subclass of [[LogicalOp]] declared in its
    * `@JsonSubTypes` annotation, filters to those implementing
    * [[StandaloneCodeGenerator]], and returns them sorted by simple name
    * (stable test report order).
    *
    * Uses the same registry Jackson uses to deserialize operators — no
    * separate discovery mechanism needed. Adding an operator to
    * `LogicalOp.@JsonSubTypes` makes it visible here automatically.
    */
  def discoverStandaloneOperators(): Seq[Class[_ <: LogicalOp]] = {
    val annotation = classOf[LogicalOp].getAnnotation(classOf[JsonSubTypes])
    if (annotation == null) Seq.empty
    else
      annotation
        .value()
        .toSeq
        .map(_.value())
        .filter(classOf[StandaloneCodeGenerator].isAssignableFrom)
        .map(_.asInstanceOf[Class[_ <: LogicalOp]])
        .distinct
        .sortBy(_.getSimpleName)
  }
}
