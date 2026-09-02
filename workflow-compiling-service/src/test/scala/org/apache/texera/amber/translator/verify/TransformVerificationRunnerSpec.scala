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

// This spec pins the tier-routing logic (disposition). Per-operator end-to-end
// runs are NOT duplicated here: OperatorBehaviorSpec auto-discovers every
// registered operator and runs TransformVerificationRunner.run on each, and a
// single operator can be run in isolation with e.g.
//   sbt "WorkflowCompilingService/testOnly *OperatorBehaviorSpec -- -z LimitOpDesc"
// (the auto-generated test name starts with the operator's simple name). What
// disposition asserts — which tier an operator routes to — is the one thing
// OperatorBehaviorSpec does not check, so it lives here.

import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnPredictionOpDesc
import org.apache.texera.amber.operator.visualization.DotPlot.DotPlotOpDesc
import org.apache.texera.amber.operator.visualization.IcicleChart.IcicleChartOpDesc
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc
import org.apache.texera.amber.operator.visualization.ScatterMatrixChart.ScatterMatrixChartOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotOpDesc
import org.apache.texera.amber.operator.visualization.bubbleChart.BubbleChartOpDesc
import org.apache.texera.amber.operator.visualization.candlestickChart.CandlestickChartOpDesc
import org.apache.texera.amber.operator.visualization.continuousErrorBands.ContinuousErrorBandsOpDesc
import org.apache.texera.amber.operator.visualization.dendrogram.DendrogramOpDesc
import org.apache.texera.amber.operator.visualization.ecdfPlot.ECDFPlotOpDesc
import org.apache.texera.amber.operator.visualization.figureFactoryTable.FigureFactoryTableOpDesc
import org.apache.texera.amber.operator.visualization.funnelPlot.FunnelPlotOpDesc
import org.apache.texera.amber.operator.visualization.heatMap.HeatMapOpDesc
import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchyChartOpDesc
import org.apache.texera.amber.operator.visualization.histogram.HistogramChartOpDesc
import org.apache.texera.amber.operator.visualization.histogram2d.Histogram2DOpDesc
import org.apache.texera.amber.operator.visualization.lineChart.LineChartOpDesc
import org.apache.texera.amber.operator.visualization.nestedTable.NestedTableOpDesc
import org.apache.texera.amber.operator.visualization.networkGraph.NetworkGraphOpDesc
import org.apache.texera.amber.operator.visualization.parallelCoordinatesPlot.ParallelCoordinatesPlotOpDesc
import org.apache.texera.amber.operator.visualization.pieChart.PieChartOpDesc
import org.apache.texera.amber.operator.visualization.rangeSlider.RangeSliderOpDesc
import org.apache.texera.amber.operator.visualization.sankeyDiagram.SankeyDiagramOpDesc
import org.apache.texera.amber.operator.visualization.scatterplot.ScatterplotOpDesc
import org.apache.texera.amber.operator.visualization.stripChart.StripChartOpDesc
import org.apache.texera.amber.operator.visualization.tablesChart.TablesPlotOpDesc
import org.apache.texera.amber.operator.visualization.treeplot.TreePlotOpDesc
import org.apache.texera.amber.operator.visualization.waterfallChart.WaterfallChartOpDesc
import org.apache.texera.amber.operator.visualization.wordCloud.WordCloudOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformVerificationRunnerSpec extends AnyFlatSpec with Matchers {
  import TransformVerificationRunner._

  "disposition" should "flag knownIssues operators with the triage reason" in {
    // The prediction op consumes a trained model on its input port, which a
    // JVM-written JSONL fixture can't carry; triaged as a known issue, not run.
    disposition(classOf[SklearnPredictionOpDesc]) match {
      case Flagged(reason) => reason should include("trained-model")
      case other           => fail(s"expected Flagged, got $other")
    }
  }

  // Both were flagged for drawing a different picture each run, which stopped
  // being true once their placement was seeded. Lifting those rows is what
  // exposed the three defects the reason had been hiding.
  it should "run the two seeded visualizations" in {
    disposition(classOf[WordCloudOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[NetworkGraphOpDesc]) shouldBe Runnable("visualization")
  }

  it should "run the union now that its code names every upstream" in {
    // It used to be flagged for naming exactly two, which was wrong in both
    // directions: a third link was dropped and a lone link left the second
    // frame unbound. The runner draws one link per port, so what runs here is
    // the one-upstream case — the one the old code got wrong.
    disposition(classOf[UnionOpDesc]) shouldBe Runnable("auto")
  }

  it should "route auto-configurable operators to the auto tier" in {
    disposition(classOf[LimitOpDesc]) shouldBe Runnable("auto")
  }

  it should "route this batch's visualizations to the visualization tier" in {
    val batch = Seq(
      classOf[BarChartOpDesc],
      classOf[BoxViolinPlotOpDesc],
      classOf[BubbleChartOpDesc],
      classOf[CandlestickChartOpDesc],
      classOf[ContinuousErrorBandsOpDesc],
      classOf[DendrogramOpDesc],
      classOf[DotPlotOpDesc],
      classOf[ECDFPlotOpDesc],
      classOf[FigureFactoryTableOpDesc],
      classOf[FunnelPlotOpDesc],
      classOf[HeatMapOpDesc],
      classOf[HierarchyChartOpDesc],
      classOf[Histogram2DOpDesc],
      classOf[HistogramChartOpDesc],
      classOf[IcicleChartOpDesc],
      classOf[ImageVisualizerOpDesc],
      classOf[LineChartOpDesc],
      classOf[NestedTableOpDesc],
      classOf[ParallelCoordinatesPlotOpDesc],
      classOf[PieChartOpDesc],
      classOf[RangeSliderOpDesc],
      classOf[SankeyDiagramOpDesc],
      classOf[ScatterMatrixChartOpDesc],
      classOf[ScatterplotOpDesc],
      classOf[StripChartOpDesc],
      classOf[TablesPlotOpDesc],
      classOf[TreePlotOpDesc],
      classOf[WaterfallChartOpDesc]
    )
    batch.foreach(op =>
      withClue(op.getSimpleName)(disposition(op) shouldBe Runnable("visualization"))
    )
  }

  // A UDF's body is written by whoever drops the operator, so there is nothing
  // for a generator to emit. It stands here for the shape of the report: an
  // operator that cannot be exported is carried as a row, not passed over.
  it should "flag an operator that has no standalone generator" in {
    disposition(classOf[PythonUDFOpDescV2]) shouldBe
      Flagged("does not implement StandaloneCodeGenerator")
  }
}
