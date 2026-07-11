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

import org.apache.texera.amber.operator.intersect.IntersectOpDesc
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
import org.apache.texera.amber.operator.sklearn.training.{
  SklearnTrainingLogisticRegressionOpDesc,
  SklearnTrainingRandomForestOpDesc,
  SklearnTrainingDecisionTreeOpDesc,
  SklearnTrainingGaussianNaiveBayesOpDesc,
  SklearnTrainingKNNOpDesc,
  SklearnTrainingSVMOpDesc,
  SklearnTrainingAdaptiveBoostingOpDesc,
  SklearnTrainingBaggingOpDesc,
  SklearnTrainingBernoulliNaiveBayesOpDesc,
  SklearnTrainingComplementNaiveBayesOpDesc,
  SklearnTrainingDummyClassifierOpDesc,
  SklearnTrainingExtraTreeOpDesc,
  SklearnTrainingExtraTreesOpDesc,
  SklearnTrainingGradientBoostingOpDesc,
  SklearnTrainingLinearRegressionOpDesc,
  SklearnTrainingLinearSVMOpDesc,
  SklearnTrainingLogisticRegressionCVOpDesc,
  SklearnTrainingMultiLayerPerceptronOpDesc,
  SklearnTrainingMultinomialNaiveBayesOpDesc,
  SklearnTrainingNearestCentroidOpDesc,
  SklearnTrainingPassiveAggressiveOpDesc,
  SklearnTrainingPerceptronOpDesc,
  SklearnTrainingProbabilityCalibrationOpDesc,
  SklearnTrainingRidgeOpDesc,
  SklearnTrainingRidgeCVOpDesc,
  SklearnTrainingSDGOpDesc
}
import org.apache.texera.amber.operator.visualization.heatMap.HeatMapOpDesc
import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchyChartOpDesc
import org.apache.texera.amber.operator.visualization.histogram2d.Histogram2DOpDesc
import org.apache.texera.amber.operator.visualization.histogram.HistogramChartOpDesc
import org.apache.texera.amber.operator.visualization.lineChart.LineChartOpDesc
import org.apache.texera.amber.operator.visualization.nestedTable.NestedTableOpDesc
import org.apache.texera.amber.operator.visualization.parallelCoordinatesPlot.ParallelCoordinatesPlotOpDesc
import org.apache.texera.amber.operator.visualization.pieChart.PieChartOpDesc
import org.apache.texera.amber.operator.visualization.polarChart.PolarChartOpDesc
import org.apache.texera.amber.operator.visualization.quiverPlot.QuiverPlotOpDesc
import org.apache.texera.amber.operator.visualization.radarChart.RadarChartOpDesc
import org.apache.texera.amber.operator.visualization.radarPlot.RadarPlotOpDesc
import org.apache.texera.amber.operator.visualization.rangeSlider.RangeSliderOpDesc
import org.apache.texera.amber.operator.visualization.sankeyDiagram.SankeyDiagramOpDesc
import org.apache.texera.amber.operator.visualization.scatter3DChart.Scatter3dChartOpDesc
import org.apache.texera.amber.operator.visualization.scatterplot.ScatterplotOpDesc
import org.apache.texera.amber.operator.visualization.stripChart.StripChartOpDesc
import org.apache.texera.amber.operator.visualization.tablesChart.TablesPlotOpDesc
import org.apache.texera.amber.operator.visualization.ternaryContour.TernaryContourOpDesc
import org.apache.texera.amber.operator.visualization.ternaryPlot.TernaryPlotOpDesc
import org.apache.texera.amber.operator.visualization.timeSeriesplot.TimeSeriesOpDesc
import org.apache.texera.amber.operator.visualization.treeplot.TreePlotOpDesc
import org.apache.texera.amber.operator.visualization.volcanoPlot.VolcanoPlotOpDesc
import org.apache.texera.amber.operator.visualization.waterfallChart.WaterfallChartOpDesc
import org.apache.texera.amber.operator.visualization.windRoseChart.WindRoseChartOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnAdaptiveBoostingOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnBaggingOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnBernoulliNaiveBayesOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnComplementNaiveBayesOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnDecisionTreeOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnDummyClassifierOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnExtraTreeOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnExtraTreesOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnGaussianNaiveBayesOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnGradientBoostingOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnKNNOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnLinearSVMOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnLogisticRegressionOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnLogisticRegressionCVOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnMultiLayerPerceptronOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnMultinomialNaiveBayesOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnNearestCentroidOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnPassiveAggressiveOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnPerceptronOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnProbabilityCalibrationOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnRandomForestOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnRidgeOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnRidgeCVOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnSDGOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnSVMOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnLinearRegressionOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer.SklearnAdvancedSVCTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVRTrainer.SklearnAdvancedSVRTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer.SklearnAdvancedKNNClassifierTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer.SklearnAdvancedKNNRegressorTrainerOpDesc
import org.apache.texera.amber.operator.ifStatement.IfOpDesc
import org.apache.texera.amber.operator.visualization.htmlviz.HtmlVizOpDesc
import org.apache.texera.amber.operator.visualization.urlviz.UrlVizOpDesc
import org.apache.texera.amber.operator.huggingFace.{
  HuggingFaceSentimentAnalysisOpDesc,
  HuggingFaceSpamSMSDetectionOpDesc,
  HuggingFaceTextSummarizationOpDesc,
  HuggingFaceIrisLogisticRegressionOpDesc
}
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
    disposition(classOf[IntersectOpDesc]) shouldBe Runnable("curated")
    disposition(classOf[MachineLearningScorerOpDesc]) shouldBe Runnable("curated")
  }

  it should "route sklearn ops to the ml-auto tier (auto-discovered shared fixture)" in {
    disposition(classOf[SklearnTrainingLogisticRegressionOpDesc]) shouldBe Runnable("ml-auto")
  }

  it should "route auto-configurable operators to the auto tier" in {
    disposition(classOf[LimitOpDesc]) shouldBe Runnable("auto")
  }

  // End-to-end smoke of the curated path: Intersect is fast, JVM-native, and
  // exercises two input ports + the order-insensitive comparator branch.
  "run" should "verify IntersectOpDesc end-to-end via the curated tier" in {
    TransformVerificationRunner.run(classOf[IntersectOpDesc])
  }

  // End-to-end smoke of the auto path: Limit is single-input single-output
  // and its config is fully derivable.
  it should "verify LimitOpDesc end-to-end via the auto tier" in {
    TransformVerificationRunner.run(classOf[LimitOpDesc])
  }

  // Hugging Face operators: python-udf transforms that add columns. Their
  // @SampleColumn tags bind the text/numeric fields to the fixture's
  // short_text / long_text / petal_length / petal_width columns. Parity loads
  // the pretrained model once per path and compares the added columns. Requires
  // transformers/torch/huggingface_hub in the UDF venv + network for the models.
  it should "verify HuggingFaceSentimentAnalysisOpDesc end-to-end" in {
    TransformVerificationRunner.run(classOf[HuggingFaceSentimentAnalysisOpDesc])
  }

  it should "verify HuggingFaceSpamSMSDetectionOpDesc end-to-end" in {
    TransformVerificationRunner.run(classOf[HuggingFaceSpamSMSDetectionOpDesc])
  }

  it should "verify HuggingFaceTextSummarizationOpDesc end-to-end" in {
    TransformVerificationRunner.run(classOf[HuggingFaceTextSummarizationOpDesc])
  }

  it should "verify HuggingFaceIrisLogisticRegressionOpDesc end-to-end" in {
    TransformVerificationRunner.run(classOf[HuggingFaceIrisLogisticRegressionOpDesc])
  }

  it should "verify DotPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[DotPlotOpDesc])
  }

  it should "verify BarChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[BarChartOpDesc])
  }

  it should "verify BulletChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[BulletChartOpDesc])
  }

  it should "verify CandlestickChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[CandlestickChartOpDesc])
  }

  it should "verify CarpetPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[CarpetPlotOpDesc])
  }

  it should "verify ChoroplethMapOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ChoroplethMapOpDesc])
  }

  it should "verify ContinuousErrorBandsOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ContinuousErrorBandsOpDesc])
  }

  it should "verify ContourPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ContourPlotOpDesc])
  }

  it should "verify DendrogramOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[DendrogramOpDesc])
  }

  it should "verify DumbbellPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[DumbbellPlotOpDesc])
  }

  it should "verify ECDFPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ECDFPlotOpDesc])
  }

  it should "verify FigureFactoryTableOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[FigureFactoryTableOpDesc])
  }

  it should "verify FilledAreaPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[FilledAreaPlotOpDesc])
  }

  it should "verify FunnelPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[FunnelPlotOpDesc])
  }

  it should "verify GanttChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[GanttChartOpDesc])
  }

  it should "verify GaugeChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[GaugeChartOpDesc])
  }

  it should "verify IcicleChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[IcicleChartOpDesc])
  }

  it should "verify BubbleChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[BubbleChartOpDesc])
  }

  it should "verify BoxViolinPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[BoxViolinPlotOpDesc])
  }

  it should "verify ImageVisualizerOpDesc end-to-end via HTML comparison" in {
    TransformVerificationRunner.run(classOf[ImageVisualizerOpDesc])
  }

  it should "verify ScatterMatrixChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ScatterMatrixChartOpDesc])
  }

  it should "verify MachineLearningScorerOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[MachineLearningScorerOpDesc])
  }

  // The Sklearn training operators all share one execution path (only the
  // estimator class differs); these cover a diverse set of model families.
  // The trained model lands in a BINARY column the comparator ignores, so each
  // case verifies that both paths run to completion and emit matching shape.
  it should "verify SklearnTrainingLogisticRegressionOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingLogisticRegressionOpDesc])
  }

  it should "verify SklearnTrainingRandomForestOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingRandomForestOpDesc])
  }

  it should "verify SklearnTrainingDecisionTreeOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingDecisionTreeOpDesc])
  }

  it should "verify SklearnTrainingGaussianNaiveBayesOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingGaussianNaiveBayesOpDesc])
  }

  it should "verify SklearnTrainingKNNOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingKNNOpDesc])
  }

  it should "verify SklearnTrainingSVMOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingSVMOpDesc])
  }

  it should "verify SklearnTrainingAdaptiveBoostingOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingAdaptiveBoostingOpDesc])
  }

  it should "verify SklearnTrainingBaggingOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingBaggingOpDesc])
  }

  it should "verify SklearnTrainingBernoulliNaiveBayesOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingBernoulliNaiveBayesOpDesc])
  }

  it should "verify SklearnTrainingComplementNaiveBayesOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingComplementNaiveBayesOpDesc])
  }

  it should "verify SklearnTrainingDummyClassifierOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingDummyClassifierOpDesc])
  }

  it should "verify SklearnTrainingExtraTreeOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingExtraTreeOpDesc])
  }

  it should "verify SklearnTrainingExtraTreesOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingExtraTreesOpDesc])
  }

  it should "verify SklearnTrainingGradientBoostingOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingGradientBoostingOpDesc])
  }

  it should "verify SklearnTrainingLinearRegressionOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingLinearRegressionOpDesc])
  }

  it should "verify SklearnTrainingLinearSVMOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingLinearSVMOpDesc])
  }

  it should "verify SklearnTrainingLogisticRegressionCVOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingLogisticRegressionCVOpDesc])
  }

  it should "verify SklearnTrainingMultiLayerPerceptronOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingMultiLayerPerceptronOpDesc])
  }

  it should "verify SklearnTrainingMultinomialNaiveBayesOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingMultinomialNaiveBayesOpDesc])
  }

  it should "verify SklearnTrainingNearestCentroidOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingNearestCentroidOpDesc])
  }

  it should "verify SklearnTrainingPassiveAggressiveOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingPassiveAggressiveOpDesc])
  }

  it should "verify SklearnTrainingPerceptronOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingPerceptronOpDesc])
  }

  it should "verify SklearnTrainingProbabilityCalibrationOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingProbabilityCalibrationOpDesc])
  }

  it should "verify SklearnTrainingRidgeOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingRidgeOpDesc])
  }

  it should "verify SklearnTrainingRidgeCVOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingRidgeCVOpDesc])
  }

  it should "verify SklearnTrainingSDGOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[SklearnTrainingSDGOpDesc])
  }

  it should "verify HeatMapOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[HeatMapOpDesc])
  }

  it should "verify HierarchyChartOpDesc end-to-end via Plotly JSON comparison" in { TransformVerificationRunner.run(classOf[HierarchyChartOpDesc]) }

  it should "verify HistogramChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[HistogramChartOpDesc])
  }

  it should "verify Histogram2DOpDesc end-to-end via Plotly JSON comparison" in { TransformVerificationRunner.run(classOf[Histogram2DOpDesc]) }

  it should "verify LineChartOpDesc end-to-end via Plotly JSON comparison" in { TransformVerificationRunner.run(classOf[LineChartOpDesc]) }


  it should "verify ParallelCoordinatesPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ParallelCoordinatesPlotOpDesc])
  }

  it should "verify PieChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[PieChartOpDesc])
  }

  it should "verify PolarChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[PolarChartOpDesc])
  }

  it should "verify QuiverPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[QuiverPlotOpDesc])
  }

  it should "verify RadarChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[RadarChartOpDesc])
  }

  it should "verify RadarPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[RadarPlotOpDesc])
  }

  it should "verify RangeSliderOpDesc end-to-end via Plotly JSON comparison" in { TransformVerificationRunner.run(classOf[RangeSliderOpDesc]) }

  it should "verify SankeyDiagramOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[SankeyDiagramOpDesc])
  }

  it should "verify Scatter3dChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[Scatter3dChartOpDesc])
  }

  it should "verify ScatterplotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ScatterplotOpDesc])
  }

  it should "verify StripChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[StripChartOpDesc])
  }

  it should "verify TablesPlotOpDesc end-to-end via Plotly JSON comparison" in { TransformVerificationRunner.run(classOf[TablesPlotOpDesc]) }

  it should "verify TernaryContourOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[TernaryContourOpDesc])
  }

  it should "verify TernaryPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[TernaryPlotOpDesc])
  }

  it should "verify TimeSeriesOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[TimeSeriesOpDesc])
  }

  it should "verify TreePlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[TreePlotOpDesc])
  }

  it should "verify VolcanoPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[VolcanoPlotOpDesc])
  }

  it should "verify WaterfallChartOpDesc end-to-end via Plotly JSON comparison" in { TransformVerificationRunner.run(classOf[WaterfallChartOpDesc]) }

  it should "verify WindRoseChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[WindRoseChartOpDesc])
  }

  it should "verify SklearnAdaptiveBoostingOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnAdaptiveBoostingOpDesc])
  }

  it should "verify SklearnBaggingOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnBaggingOpDesc])
  }

  it should "verify SklearnBernoulliNaiveBayesOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnBernoulliNaiveBayesOpDesc])
  }

  it should "verify SklearnComplementNaiveBayesOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnComplementNaiveBayesOpDesc])
  }

  it should "verify SklearnDecisionTreeOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnDecisionTreeOpDesc])
  }

  it should "verify SklearnDummyClassifierOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnDummyClassifierOpDesc])
  }

  it should "verify SklearnExtraTreeOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnExtraTreeOpDesc])
  }

  it should "verify SklearnExtraTreesOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnExtraTreesOpDesc])
  }

  it should "verify SklearnGaussianNaiveBayesOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnGaussianNaiveBayesOpDesc])
  }

  it should "verify SklearnGradientBoostingOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnGradientBoostingOpDesc])
  }

  it should "verify SklearnKNNOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnKNNOpDesc])
  }

  it should "verify SklearnLinearSVMOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnLinearSVMOpDesc])
  }

  it should "verify SklearnLogisticRegressionOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnLogisticRegressionOpDesc])
  }

  it should "verify SklearnLogisticRegressionCVOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnLogisticRegressionCVOpDesc])
  }

  it should "verify SklearnMultiLayerPerceptronOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnMultiLayerPerceptronOpDesc])
  }

  it should "verify SklearnMultinomialNaiveBayesOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnMultinomialNaiveBayesOpDesc])
  }

  it should "verify SklearnNearestCentroidOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnNearestCentroidOpDesc])
  }

  it should "verify SklearnPassiveAggressiveOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnPassiveAggressiveOpDesc])
  }

  it should "verify SklearnPerceptronOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnPerceptronOpDesc])
  }

  it should "verify SklearnProbabilityCalibrationOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnProbabilityCalibrationOpDesc])
  }

  it should "verify SklearnRandomForestOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnRandomForestOpDesc])
  }

  it should "verify SklearnRidgeOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnRidgeOpDesc])
  }

  it should "verify SklearnRidgeCVOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnRidgeCVOpDesc])
  }

  it should "verify SklearnSDGOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnSDGOpDesc])
  }

  it should "verify SklearnSVMOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnSVMOpDesc])
  }

  it should "verify SklearnLinearRegressionOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnLinearRegressionOpDesc])
  }

  it should "verify SklearnAdvancedSVCTrainerOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnAdvancedSVCTrainerOpDesc])
  }

  it should "verify SklearnAdvancedSVRTrainerOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnAdvancedSVRTrainerOpDesc])
  }

  it should "verify SklearnAdvancedKNNClassifierTrainerOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnAdvancedKNNClassifierTrainerOpDesc])
  }

  it should "verify SklearnAdvancedKNNRegressorTrainerOpDesc end-to-end via curated model comparison" in {
    TransformVerificationRunner.run(classOf[SklearnAdvancedKNNRegressorTrainerOpDesc])
  }

  it should "verify IfOpDesc end-to-end via curated comparison (empty condition port, default True branch)" in {
    TransformVerificationRunner.run(classOf[IfOpDesc])
  }

  it should "verify HtmlVizOpDesc end-to-end via curated DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[HtmlVizOpDesc])
  }

  it should "verify UrlVizOpDesc end-to-end via auto DataFrame comparison" in {
    TransformVerificationRunner.run(classOf[UrlVizOpDesc])
  }
}
