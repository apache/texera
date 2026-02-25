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

package org.apache.texera.amber.operator

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation._
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PhysicalPlan, PortIdentity}
import org.apache.texera.amber.operator.aggregate.AggregateOpDesc
import org.apache.texera.amber.operator.cartesianProduct.CartesianProductOpDesc
import org.apache.texera.amber.operator.dictionary.DictionaryMatcherOpDesc
import org.apache.texera.amber.operator.difference.DifferenceOpDesc
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.dummy.DummyOpDesc
import org.apache.texera.amber.operator.filter.SpecializedFilterOpDesc
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc
import org.apache.texera.amber.operator.huggingFace.{
  HuggingFaceIrisLogisticRegressionOpDesc,
  HuggingFaceSentimentAnalysisOpDesc,
  HuggingFaceSpamSMSDetectionOpDesc,
  HuggingFaceTextSummarizationOpDesc
}
import org.apache.texera.amber.operator.ifStatement.IfOpDesc
import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.intervalJoin.IntervalJoinOpDesc
import org.apache.texera.amber.operator.keywordSearch.KeywordSearchOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.machineLearning.Scorer.MachineLearningScorerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer.{
  SklearnAdvancedKNNClassifierTrainerOpDesc,
  SklearnAdvancedKNNRegressorTrainerOpDesc
}
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer.SklearnAdvancedSVCTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVRTrainer.SklearnAdvancedSVRTrainerOpDesc
import org.apache.texera.amber.operator.metadata.{OPVersion, OperatorInfo, PropertyNameConstants}
import org.apache.texera.amber.operator.projection.ProjectionOpDesc
import org.apache.texera.amber.operator.randomksampling.RandomKSamplingOpDesc
import org.apache.texera.amber.operator.regex.RegexOpDesc
import org.apache.texera.amber.operator.reservoirsampling.ReservoirSamplingOpDesc
import org.apache.texera.amber.operator.sklearn._
import org.apache.texera.amber.operator.sklearn.training._
import org.apache.texera.amber.operator.sleep.SleepOpDesc
import org.apache.texera.amber.operator.sort.{SortOpDesc, StableMergeSortOpDesc}
import org.apache.texera.amber.operator.sortPartitions.SortPartitionsOpDesc
import org.apache.texera.amber.operator.source.apis.reddit.RedditSearchSourceOpDesc
import org.apache.texera.amber.operator.source.apis.twitter.v2.{
  TwitterFullArchiveSearchSourceOpDesc,
  TwitterSearchSourceOpDesc
}
import org.apache.texera.amber.operator.source.fetcher.URLFetcherOpDesc
import org.apache.texera.amber.operator.source.scan.FileScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.arrow.ArrowSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.json.JSONLScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.source.sql.asterixdb.AsterixDBSourceOpDesc
import org.apache.texera.amber.operator.source.sql.mysql.MySQLSourceOpDesc
import org.apache.texera.amber.operator.source.sql.postgresql.PostgreSQLSourceOpDesc
import org.apache.texera.amber.operator.split.SplitOpDesc
import org.apache.texera.amber.operator.substringSearch.SubstringSearchOpDesc
import org.apache.texera.amber.operator.symmetricDifference.SymmetricDifferenceOpDesc
import org.apache.texera.amber.operator.typecasting.TypeCastingOpDesc
import org.apache.texera.amber.operator.udf.java.JavaUDFOpDesc
import org.apache.texera.amber.operator.udf.python._
import org.apache.texera.amber.operator.udf.python.source.PythonUDFSourceOpDescV2
import org.apache.texera.amber.operator.udf.r.{RUDFOpDesc, RUDFSourceOpDesc}
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.operator.unneststring.UnnestStringOpDesc
import org.apache.texera.amber.operator.visualization.DotPlot.DotPlotOpDesc
import org.apache.texera.amber.operator.visualization.IcicleChart.IcicleChartOpDesc
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc
import org.apache.texera.amber.operator.visualization.ScatterMatrixChart.ScatterMatrixChartOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotOpDesc
import org.apache.texera.amber.operator.visualization.bubbleChart.BubbleChartOpDesc
import org.apache.texera.amber.operator.visualization.bulletChart.BulletChartOpDesc
import org.apache.texera.amber.operator.visualization.candlestickChart.CandlestickChartOpDesc
import org.apache.texera.amber.operator.visualization.choroplethMap.ChoroplethMapOpDesc
import org.apache.texera.amber.operator.visualization.continuousErrorBands.ContinuousErrorBandsOpDesc
import org.apache.texera.amber.operator.visualization.contourPlot.ContourPlotOpDesc
import org.apache.texera.amber.operator.visualization.dendrogram.DendrogramOpDesc
import org.apache.texera.amber.operator.visualization.dumbbellPlot.DumbbellPlotOpDesc
import org.apache.texera.amber.operator.visualization.figureFactoryTable.FigureFactoryTableOpDesc
import org.apache.texera.amber.operator.visualization.filledAreaPlot.FilledAreaPlotOpDesc
import org.apache.texera.amber.operator.visualization.funnelPlot.FunnelPlotOpDesc
import org.apache.texera.amber.operator.visualization.ganttChart.GanttChartOpDesc
import org.apache.texera.amber.operator.visualization.gaugeChart.GaugeChartOpDesc
import org.apache.texera.amber.operator.visualization.heatMap.HeatMapOpDesc
import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchyChartOpDesc
import org.apache.texera.amber.operator.visualization.histogram.HistogramChartOpDesc
import org.apache.texera.amber.operator.visualization.histogram2d.Histogram2DOpDesc
import org.apache.texera.amber.operator.visualization.htmlviz.HtmlVizOpDesc
import org.apache.texera.amber.operator.visualization.lineChart.LineChartOpDesc
import org.apache.texera.amber.operator.visualization.nestedTable.NestedTableOpDesc
import org.apache.texera.amber.operator.visualization.networkGraph.NetworkGraphOpDesc
import org.apache.texera.amber.operator.visualization.pieChart.PieChartOpDesc
import org.apache.texera.amber.operator.visualization.quiverPlot.QuiverPlotOpDesc
import org.apache.texera.amber.operator.visualization.rangeSlider.RangeSliderOpDesc
import org.apache.texera.amber.operator.visualization.sankeyDiagram.SankeyDiagramOpDesc
import org.apache.texera.amber.operator.visualization.scatter3DChart.Scatter3dChartOpDesc
import org.apache.texera.amber.operator.visualization.scatterplot.ScatterplotOpDesc
import org.apache.texera.amber.operator.visualization.tablesChart.TablesPlotOpDesc
import org.apache.texera.amber.operator.visualization.ternaryPlot.TernaryPlotOpDesc
import org.apache.texera.amber.operator.visualization.timeSeriesplot.TimeSeriesOpDesc
import org.apache.texera.amber.operator.visualization.treeplot.TreePlotOpDesc
import org.apache.texera.amber.operator.visualization.urlviz.UrlVizOpDesc
import org.apache.texera.amber.operator.visualization.volcanoPlot.VolcanoPlotOpDesc
import org.apache.texera.amber.operator.visualization.waterfallChart.WaterfallChartOpDesc
import org.apache.texera.amber.operator.visualization.wordCloud.WordCloudOpDesc
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}
import org.apache.texera.amber.operator.sklearn.testing.SklearnTestingOpDesc
import org.apache.texera.amber.operator.visualization.stripChart.StripChartOpDesc

import java.util.UUID
import scala.util.Try

trait StateTransferFunc
    extends ((OperatorExecutor, OperatorExecutor) => Unit)
    with java.io.Serializable

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operatorType"
)
@JsonSubTypes(
  Array(
    new Type(value = classOf[IfOpDesc], name = "If"),
    new Type(value = classOf[SankeyDiagramOpDesc], name = "SankeyDiagram"),
    new Type(value = classOf[IcicleChartOpDesc], name = "IcicleChart"),
    new Type(value = classOf[CSVScanSourceOpDesc], name = "CSVFileScan"),
    // disabled the ParallelCSVScanSourceOpDesc so that it does not confuse user. it can be re-enabled when doing experiments.
    // new Type(value = classOf[ParallelCSVScanSourceOpDesc], name = "ParallelCSVFileScan"),
    new Type(value = classOf[JSONLScanSourceOpDesc], name = "JSONLFileScan"),
    new Type(value = classOf[FileScanSourceOpDesc], name = "FileScan"),
    new Type(value = classOf[TextInputSourceOpDesc], name = "TextInput"),
    new Type(
      value = classOf[TwitterFullArchiveSearchSourceOpDesc],
      name = "TwitterFullArchiveSearch"
    ),
    new Type(
      value = classOf[TwitterSearchSourceOpDesc],
      name = "TwitterSearch"
    ),
    new Type(value = classOf[ChoroplethMapOpDesc], name = "ChoroplethMap"),
    new Type(value = classOf[TimeSeriesOpDesc], name = "TimeSeriesPlot"),
    new Type(value = classOf[CandlestickChartOpDesc], name = "CandlestickChart"),
    new Type(value = classOf[SplitOpDesc], name = "Split"),
    new Type(value = classOf[ContourPlotOpDesc], name = "ContourPlot"),
    new Type(value = classOf[RegexOpDesc], name = "Regex"),
    new Type(value = classOf[SpecializedFilterOpDesc], name = "Filter"),
    new Type(value = classOf[ProjectionOpDesc], name = "Projection"),
    new Type(value = classOf[StripChartOpDesc], name = "StripChart"),
    new Type(value = classOf[UnionOpDesc], name = "Union"),
    new Type(value = classOf[KeywordSearchOpDesc], name = "KeywordSearch"),
    new Type(value = classOf[SubstringSearchOpDesc], name = "SubstringSearch"),
    new Type(value = classOf[AggregateOpDesc], name = "Aggregate"),
    new Type(value = classOf[LineChartOpDesc], name = "LineChart"),
    new Type(value = classOf[WaterfallChartOpDesc], name = "WaterfallChart"),
    new Type(value = classOf[BarChartOpDesc], name = "BarChart"),
    new Type(value = classOf[RangeSliderOpDesc], name = "RangeSlider"),
    new Type(value = classOf[PieChartOpDesc], name = "PieChart"),
    new Type(value = classOf[QuiverPlotOpDesc], name = "QuiverPlot"),
    new Type(value = classOf[WordCloudOpDesc], name = "WordCloud"),
    new Type(value = classOf[HtmlVizOpDesc], name = "HTMLVisualizer"),
    new Type(value = classOf[UrlVizOpDesc], name = "URLVisualizer"),
    new Type(value = classOf[ScatterplotOpDesc], name = "Scatterplot"),
    new Type(value = classOf[PythonUDFOpDescV2], name = "PythonUDFV2"),
    new Type(value = classOf[PythonUDFSourceOpDescV2], name = "PythonUDFSourceV2"),
    new Type(value = classOf[DualInputPortsPythonUDFOpDescV2], name = "DualInputPortsPythonUDFV2"),
    new Type(value = classOf[MySQLSourceOpDesc], name = "MySQLSource"),
    new Type(value = classOf[PostgreSQLSourceOpDesc], name = "PostgreSQLSource"),
    new Type(value = classOf[AsterixDBSourceOpDesc], name = "AsterixDBSource"),
    new Type(value = classOf[TypeCastingOpDesc], name = "TypeCasting"),
    new Type(value = classOf[LimitOpDesc], name = "Limit"),
    new Type(value = classOf[SleepOpDesc], name = "Sleep"),
    new Type(value = classOf[RandomKSamplingOpDesc], name = "RandomKSampling"),
    new Type(value = classOf[ReservoirSamplingOpDesc], name = "ReservoirSampling"),
    new Type(value = classOf[HashJoinOpDesc[String]], name = "HashJoin"),
    new Type(value = classOf[DistinctOpDesc], name = "Distinct"),
    new Type(value = classOf[IntersectOpDesc], name = "Intersect"),
    new Type(value = classOf[SymmetricDifferenceOpDesc], name = "SymmetricDifference"),
    new Type(value = classOf[DifferenceOpDesc], name = "Difference"),
    new Type(value = classOf[IntervalJoinOpDesc], name = "IntervalJoin"),
    new Type(value = classOf[UnnestStringOpDesc], name = "UnnestString"),
    new Type(value = classOf[DictionaryMatcherOpDesc], name = "DictionaryMatcher"),
    new Type(value = classOf[SortPartitionsOpDesc], name = "SortPartitions"),
    new Type(value = classOf[CSVOldScanSourceOpDesc], name = "CSVOldFileScan"),
    new Type(value = classOf[RedditSearchSourceOpDesc], name = "RedditSearch"),
    new Type(value = classOf[PythonLambdaFunctionOpDesc], name = "PythonLambdaFunction"),
    new Type(value = classOf[PythonTableReducerOpDesc], name = "PythonTableReducer"),
    new Type(value = classOf[URLFetcherOpDesc], name = "URLFetcher"),
    new Type(value = classOf[VolcanoPlotOpDesc], name = "VolcanoPlot"),
    new Type(value = classOf[CartesianProductOpDesc], name = "CartesianProduct"),
    new Type(value = classOf[FilledAreaPlotOpDesc], name = "FilledAreaPlot"),
    new Type(value = classOf[DotPlotOpDesc], name = "DotPlot"),
    new Type(value = classOf[TreePlotOpDesc], name = "TreePlot"),
    new Type(value = classOf[BubbleChartOpDesc], name = "BubbleChart"),
    new Type(value = classOf[BulletChartOpDesc], name = "BulletChart"),
    new Type(value = classOf[GanttChartOpDesc], name = "GanttChart"),
    new Type(value = classOf[GaugeChartOpDesc], name = "GaugeChart"),
    new Type(value = classOf[ImageVisualizerOpDesc], name = "ImageVisualizer"),
    new Type(value = classOf[HierarchyChartOpDesc], name = "HierarchyChart"),
    new Type(value = classOf[DumbbellPlotOpDesc], name = "DumbbellPlot"),
    new Type(value = classOf[DummyOpDesc], name = "Dummy"),
    new Type(value = classOf[BoxViolinPlotOpDesc], name = "BoxViolinPlot"),
    new Type(value = classOf[NetworkGraphOpDesc], name = "NetworkGraph"),
    new Type(value = classOf[HistogramChartOpDesc], name = "Histogram"),
    new Type(value = classOf[Histogram2DOpDesc], name = "Histogram2D"),
    new Type(value = classOf[ScatterMatrixChartOpDesc], name = "ScatterMatrixChart"),
    new Type(value = classOf[HeatMapOpDesc], name = "HeatMap"),
    new Type(value = classOf[Scatter3dChartOpDesc], name = "Scatter3DChart"),
    new Type(value = classOf[FunnelPlotOpDesc], name = "FunnelPlot"),
    new Type(value = classOf[TablesPlotOpDesc], name = "TablesPlot"),
    new Type(value = classOf[ContinuousErrorBandsOpDesc], name = "ContinuousErrorBands"),
    new Type(value = classOf[FigureFactoryTableOpDesc], name = "FigureFactoryTable"),
    new Type(value = classOf[TernaryPlotOpDesc], name = "TernaryPlot"),
    new Type(value = classOf[DendrogramOpDesc], name = "Dendrogram"),
    new Type(value = classOf[NestedTableOpDesc], name = "NestedTable"),
    new Type(value = classOf[JavaUDFOpDesc], name = "JavaUDF"),
    new Type(value = classOf[RUDFOpDesc], name = "RUDF"),
    new Type(value = classOf[RUDFSourceOpDesc], name = "RUDFSource"),
    new Type(value = classOf[ArrowSourceOpDesc], name = "ArrowSource"),
    new Type(value = classOf[MachineLearningScorerOpDesc], name = "Scorer"),
    new Type(value = classOf[SortOpDesc], name = "Sort"),
    new Type(value = classOf[StableMergeSortOpDesc], name = "StableMergeSort"),
    new Type(value = classOf[SklearnLogisticRegressionOpDesc], name = "SklearnLogisticRegression"),
    new Type(
      value = classOf[SklearnLogisticRegressionCVOpDesc],
      name = "SklearnLogisticRegressionCV"
    ),
    new Type(value = classOf[SklearnTrainingRidgeOpDesc], name = "SklearnTrainingRidge"),
    new Type(value = classOf[SklearnTrainingRidgeCVOpDesc], name = "SklearnTrainingRidgeCV"),
    new Type(value = classOf[SklearnTrainingSDGOpDesc], name = "SklearnTrainingSDG"),
    new Type(
      value = classOf[SklearnTrainingPassiveAggressiveOpDesc],
      name = "SklearnTrainingPassiveAggressive"
    ),
    new Type(value = classOf[SklearnTrainingPerceptronOpDesc], name = "SklearnTrainingPerceptron"),
    new Type(value = classOf[SklearnTrainingKNNOpDesc], name = "SklearnTrainingKNN"),
    new Type(
      value = classOf[SklearnTrainingNearestCentroidOpDesc],
      name = "SklearnTrainingNearestCentroid"
    ),
    new Type(value = classOf[SklearnTrainingSVMOpDesc], name = "SklearnTrainingSVM"),
    new Type(value = classOf[SklearnTrainingLinearSVMOpDesc], name = "SklearnTrainingLinearSVM"),
    new Type(
      value = classOf[SklearnTrainingDecisionTreeOpDesc],
      name = "SklearnTrainingDecisionTree"
    ),
    new Type(value = classOf[SklearnTrainingExtraTreeOpDesc], name = "SklearnTrainingExtraTree"),
    new Type(
      value = classOf[SklearnTrainingMultiLayerPerceptronOpDesc],
      name = "SklearnTrainingMultiLayerPerceptron"
    ),
    new Type(
      value = classOf[SklearnTrainingProbabilityCalibrationOpDesc],
      name = "SklearnTrainingProbabilityCalibration"
    ),
    new Type(
      value = classOf[SklearnTrainingRandomForestOpDesc],
      name = "SklearnTrainingRandomForest"
    ),
    new Type(value = classOf[SklearnTrainingBaggingOpDesc], name = "SklearnTrainingBagging"),
    new Type(
      value = classOf[SklearnTrainingGradientBoostingOpDesc],
      name = "SklearnTrainingGradientBoosting"
    ),
    new Type(
      value = classOf[SklearnTrainingAdaptiveBoostingOpDesc],
      name = "SklearnTrainingAdaptiveBoosting"
    ),
    new Type(value = classOf[SklearnTrainingExtraTreesOpDesc], name = "SklearnTrainingExtraTrees"),
    new Type(
      value = classOf[SklearnTrainingGaussianNaiveBayesOpDesc],
      name = "SklearnTrainingGaussianNaiveBayes"
    ),
    new Type(
      value = classOf[SklearnTrainingMultinomialNaiveBayesOpDesc],
      name = "SklearnTrainingMultinomialNaiveBayes"
    ),
    new Type(
      value = classOf[SklearnTrainingComplementNaiveBayesOpDesc],
      name = "SklearnTrainingComplementNaiveBayes"
    ),
    new Type(
      value = classOf[SklearnTrainingBernoulliNaiveBayesOpDesc],
      name = "SklearnTrainingBernoulliNaiveBayes"
    ),
    new Type(
      value = classOf[SklearnTrainingDummyClassifierOpDesc],
      name = "SklearnTrainingDummyClassifier"
    ),
    new Type(
      value = classOf[SklearnTrainingLinearRegressionOpDesc],
      name = "SklearnTrainingLinearRegression"
    ),
    new Type(
      value = classOf[SklearnTrainingLogisticRegressionOpDesc],
      name = "SklearnTrainingLogisticRegression"
    ),
    new Type(
      value = classOf[SklearnTrainingLogisticRegressionCVOpDesc],
      name = "SklearnTrainingLogisticRegressionCV"
    ),
    new Type(value = classOf[SklearnLogisticRegressionOpDesc], name = "SklearnLogisticRegression"),
    new Type(
      value = classOf[SklearnLogisticRegressionCVOpDesc],
      name = "SklearnLogisticRegressionCV"
    ),
    new Type(value = classOf[SklearnRidgeOpDesc], name = "SklearnRidge"),
    new Type(value = classOf[SklearnRidgeCVOpDesc], name = "SklearnRidgeCV"),
    new Type(value = classOf[SklearnSDGOpDesc], name = "SklearnSDG"),
    new Type(value = classOf[SklearnPassiveAggressiveOpDesc], name = "SklearnPassiveAggressive"),
    new Type(value = classOf[SklearnPerceptronOpDesc], name = "SklearnPerceptron"),
    new Type(value = classOf[SklearnKNNOpDesc], name = "SklearnKNN"),
    new Type(value = classOf[SklearnNearestCentroidOpDesc], name = "SklearnNearestCentroid"),
    new Type(value = classOf[SklearnSVMOpDesc], name = "SklearnSVM"),
    new Type(value = classOf[SklearnLinearSVMOpDesc], name = "SklearnLinearSVM"),
    new Type(value = classOf[SklearnLinearRegressionOpDesc], name = "SklearnLinearRegression"),
    new Type(value = classOf[SklearnDecisionTreeOpDesc], name = "SklearnDecisionTree"),
    new Type(value = classOf[SklearnExtraTreeOpDesc], name = "SklearnExtraTree"),
    new Type(
      value = classOf[SklearnMultiLayerPerceptronOpDesc],
      name = "SklearnMultiLayerPerceptron"
    ),
    new Type(
      value = classOf[SklearnProbabilityCalibrationOpDesc],
      name = "SklearnProbabilityCalibration"
    ),
    new Type(value = classOf[SklearnRandomForestOpDesc], name = "SklearnRandomForest"),
    new Type(value = classOf[SklearnBaggingOpDesc], name = "SklearnBagging"),
    new Type(value = classOf[SklearnGradientBoostingOpDesc], name = "SklearnGradientBoosting"),
    new Type(value = classOf[SklearnAdaptiveBoostingOpDesc], name = "SklearnAdaptiveBoosting"),
    new Type(value = classOf[SklearnExtraTreesOpDesc], name = "SklearnExtraTrees"),
    new Type(value = classOf[SklearnGaussianNaiveBayesOpDesc], name = "SklearnGaussianNaiveBayes"),
    new Type(
      value = classOf[SklearnMultinomialNaiveBayesOpDesc],
      name = "SklearnMultinomialNaiveBayes"
    ),
    new Type(
      value = classOf[SklearnComplementNaiveBayesOpDesc],
      name = "SklearnComplementNaiveBayes"
    ),
    new Type(
      value = classOf[SklearnBernoulliNaiveBayesOpDesc],
      name = "SklearnBernoulliNaiveBayes"
    ),
    new Type(value = classOf[SklearnDummyClassifierOpDesc], name = "SklearnDummyClassifier"),
    new Type(value = classOf[SklearnPredictionOpDesc], name = "SklearnPrediction"),
    new Type(
      value = classOf[HuggingFaceSentimentAnalysisOpDesc],
      name = "HuggingFaceSentimentAnalysis"
    ),
    new Type(
      value = classOf[HuggingFaceTextSummarizationOpDesc],
      name = "HuggingFaceTextSummarization"
    ),
    new Type(
      value = classOf[HuggingFaceSpamSMSDetectionOpDesc],
      name = "HuggingFaceSpamSMSDetection"
    ),
    new Type(
      value = classOf[HuggingFaceIrisLogisticRegressionOpDesc],
      name = "HuggingFaceIrisLogisticRegression"
    ),
    new Type(
      value = classOf[SklearnAdvancedKNNClassifierTrainerOpDesc],
      name = "KNNClassifierTrainer"
    ),
    new Type(
      value = classOf[SklearnAdvancedKNNRegressorTrainerOpDesc],
      name = "KNNRegressorTrainer"
    ),
    new Type(
      value = classOf[SklearnAdvancedSVCTrainerOpDesc],
      name = "SVCTrainer"
    ),
    new Type(
      value = classOf[SklearnAdvancedSVRTrainerOpDesc],
      name = "SVRTrainer"
    ),
    new Type(value = classOf[SklearnTestingOpDesc], name = "SklearnTesting")
  )
)
abstract class LogicalOp extends PortDescriptor with Serializable {

  @JsonProperty(PropertyNameConstants.OPERATOR_ID)
  private var operatorId: String = getClass.getSimpleName + "-" + UUID.randomUUID.toString

  @JsonProperty(PropertyNameConstants.OPERATOR_VERSION)
  var operatorVersion: String = getOperatorVersion

  def operatorIdentifier: OperatorIdentity = OperatorIdentity(operatorId)

  def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = ???

  // a logical operator corresponds multiple physical operators (a small DAG)
  def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {
    PhysicalPlan(
      operators = Set(getPhysicalOp(workflowId, executionId)),
      links = Set.empty
    )
  }

  def operatorInfo: OperatorInfo

  private def getOperatorVersion: String = {
    val path = "amber/src/main/scala/"
    val operatorPath = path + this.getClass.getPackage.getName.replace(".", "/")
    OPVersion.getVersion(this.getClass.getSimpleName, operatorPath)
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that, "context")

  override def toString: String = ToStringBuilder.reflectionToString(this)

  def setOperatorId(id: String): Unit = {
    operatorId = id
  }

  def runtimeReconfiguration(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      oldOpDesc: LogicalOp,
      newOpDesc: LogicalOp
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    throw new UnsupportedOperationException(
      "operator " + getClass.getSimpleName + " does not support reconfiguration"
    )
  }

  @JsonProperty
  @JsonSchemaTitle("Dummy Property List")
  @JsonPropertyDescription("Add dummy property if needed")
  var dummyPropertyList: List[DummyProperties] = List()

  /**
    * Propagates the schema from external input ports to external output ports.
    * This method is primarily used to derive the output schemas for logical operators.
    *
    * @param inputSchemas A map containing the schemas of the external input ports.
    * @return A map of external output port identities to their corresponding schemas.
    */
  def getExternalOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    this
      .getPhysicalPlan(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
      .propagateSchema(inputSchemas)
      .operators
      .flatMap { operator =>
        operator.outputPorts.values
          .filterNot { case (port, _, _) => port.id.internal } // Exclude internal ports
          .map {
            case (port, _, schemaEither) =>
              schemaEither match {
                case Left(error) => throw error
                case Right(schema) =>
                  port.id -> schema // Map external port ID to its schema
              }
          }
      }
      .toMap
  }
}
