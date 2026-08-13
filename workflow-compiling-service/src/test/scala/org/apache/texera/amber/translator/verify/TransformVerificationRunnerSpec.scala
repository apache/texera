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

import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.apache.texera.amber.operator.visualization.DotPlot.DotPlotOpDesc
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc
import org.apache.texera.amber.operator.visualization.IcicleChart.IcicleChartOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotOpDesc
import org.apache.texera.amber.operator.visualization.bubbleChart.BubbleChartOpDesc
import org.apache.texera.amber.operator.visualization.bulletChart.BulletChartOpDesc
import org.apache.texera.amber.operator.visualization.candlestickChart.CandlestickChartOpDesc
import org.apache.texera.amber.operator.visualization.carpetPlot.CarpetPlotOpDesc
import org.apache.texera.amber.operator.visualization.choroplethMap.ChoroplethMapOpDesc
import org.apache.texera.amber.operator.visualization.continuousErrorBands.ContinuousErrorBandsOpDesc
import org.apache.texera.amber.operator.visualization.contourPlot.ContourPlotOpDesc
import org.apache.texera.amber.operator.visualization.dendrogram.DendrogramOpDesc
import org.apache.texera.amber.operator.visualization.dumbbellPlot.DumbbellPlotOpDesc
import org.apache.texera.amber.operator.visualization.ecdfPlot.ECDFPlotOpDesc
import org.apache.texera.amber.operator.visualization.figureFactoryTable.FigureFactoryTableOpDesc
import org.apache.texera.amber.operator.visualization.filledAreaPlot.FilledAreaPlotOpDesc
import org.apache.texera.amber.operator.visualization.funnelPlot.FunnelPlotOpDesc
import org.apache.texera.amber.operator.visualization.ganttChart.GanttChartOpDesc
import org.apache.texera.amber.operator.visualization.gaugeChart.GaugeChartOpDesc
import org.apache.texera.amber.operator.visualization.ScatterMatrixChart.ScatterMatrixChartOpDesc
import org.apache.texera.amber.operator.machineLearning.Scorer.MachineLearningScorerOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnPredictionOpDesc
import org.apache.texera.amber.operator.sklearn.training.SklearnTrainingLogisticRegressionOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer.SklearnAdvancedSVCTrainerOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformVerificationRunnerSpec extends AnyFlatSpec with Matchers {
  import TransformVerificationRunner._

  "disposition" should "flag knownIssues operators with the triage reason" in {
    disposition(classOf[UnionOpDesc]) match {
      case Flagged(reason) => reason should include("known issue")
      case other           => fail(s"expected Flagged, got $other")
    }
    // The prediction op consumes a trained model on its input port, which a
    // JVM-written JSONL fixture can't carry; triaged as a known issue, not run.
    disposition(classOf[SklearnPredictionOpDesc]) match {
      case Flagged(reason) => reason should include("trained-model")
      case other           => fail(s"expected Flagged, got $other")
    }
  }

  it should "route visualization operators with JSON validation support to the visualization tier" in {
    disposition(classOf[BarChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[BulletChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[CandlestickChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[CarpetPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ChoroplethMapOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ContinuousErrorBandsOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ContourPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[DendrogramOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[DumbbellPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ECDFPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[FigureFactoryTableOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[FilledAreaPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[FunnelPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[GanttChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[GaugeChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[DotPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[IcicleChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[BubbleChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[BoxViolinPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ImageVisualizerOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ScatterMatrixChartOpDesc]) shouldBe Runnable("visualization")
  }

  it should "route genuine one-off curated ops to the curated tier" in {
    disposition(classOf[HashJoinOpDesc[_]]) shouldBe Runnable("curated")
    disposition(classOf[MachineLearningScorerOpDesc]) shouldBe Runnable("curated")
  }

  it should "route a sklearn estimator to the auto tier on the numeric projection" in {
    disposition(classOf[SklearnTrainingLogisticRegressionOpDesc]) shouldBe
      Runnable("auto, countVectorizer=false, tfidfTransformer=false")
    fixtureFor(classOf[SklearnTrainingLogisticRegressionOpDesc]) shouldBe
      CanonicalFixture.sklearnNumeric
  }

  it should "keep the advanced trainers curated, their paraList being unresolvable" in {
    disposition(classOf[SklearnAdvancedSVCTrainerOpDesc]) shouldBe Runnable("curated")
  }

  it should "route auto-configurable operators to the auto tier" in {
    disposition(classOf[LimitOpDesc]) shouldBe Runnable("auto")
  }
}
