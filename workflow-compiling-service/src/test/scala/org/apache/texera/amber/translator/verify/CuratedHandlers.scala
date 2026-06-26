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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.dictionary.{DictionaryMatcherOpDesc, MatchingType}
import org.apache.texera.amber.operator.difference.DifferenceOpDesc
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.hashJoin.{HashJoinOpDesc, JoinType}
import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.symmetricDifference.SymmetricDifferenceOpDesc
import org.apache.texera.amber.operator.visualization.DotPlot.DotPlotOpDesc
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc
import org.apache.texera.amber.operator.visualization.IcicleChart.IcicleChartOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotQuartileFunction
import org.apache.texera.amber.operator.visualization.bubbleChart.BubbleChartOpDesc
import org.apache.texera.amber.operator.visualization.bulletChart.{
  BulletChartOpDesc,
  BulletChartStepDefinition
}
import org.apache.texera.amber.operator.visualization.candlestickChart.CandlestickChartOpDesc
import org.apache.texera.amber.operator.visualization.carpetPlot.CarpetPlotOpDesc
import org.apache.texera.amber.operator.visualization.choroplethMap.ChoroplethMapOpDesc
import org.apache.texera.amber.operator.visualization.continuousErrorBands.{
  BandConfig,
  ContinuousErrorBandsOpDesc
}
import org.apache.texera.amber.operator.visualization.contourPlot.{
  ContourPlotColoringFunction,
  ContourPlotOpDesc
}
import org.apache.texera.amber.operator.visualization.dendrogram.DendrogramOpDesc
import org.apache.texera.amber.operator.visualization.dumbbellPlot.DumbbellPlotOpDesc
import org.apache.texera.amber.operator.visualization.ecdfPlot.ECDFPlotOpDesc
import org.apache.texera.amber.operator.visualization.figureFactoryTable.{
  FigureFactoryTableConfig,
  FigureFactoryTableOpDesc
}
import org.apache.texera.amber.operator.visualization.filledAreaPlot.FilledAreaPlotOpDesc
import org.apache.texera.amber.operator.visualization.funnelPlot.FunnelPlotOpDesc
import org.apache.texera.amber.operator.visualization.ganttChart.GanttChartOpDesc
import org.apache.texera.amber.operator.visualization.gaugeChart.GaugeChartOpDesc
import org.apache.texera.amber.operator.visualization.lineChart.LineMode
import org.apache.texera.amber.operator.visualization.ScatterMatrixChart.ScatterMatrixChartOpDesc
import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchySection
import org.apache.texera.amber.operator.machineLearning.Scorer.classificationMetricsFnc
import org.apache.texera.amber.operator.machineLearning.Scorer.MachineLearningScorerOpDesc
import org.apache.texera.amber.operator.sklearn.training.{
  SklearnTrainingOpDesc,
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

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.visualization.heatMap.HeatMapOpDesc
import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchyChartOpDesc
import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchyChartType
import org.apache.texera.amber.operator.visualization.histogram.HistogramChartOpDesc
import org.apache.texera.amber.operator.visualization.histogram2d.Histogram2DOpDesc
import org.apache.texera.amber.operator.visualization.histogram2d.NormalizationType
import org.apache.texera.amber.operator.visualization.lineChart.LineChartOpDesc
import org.apache.texera.amber.operator.visualization.lineChart.LineConfig
import org.apache.texera.amber.operator.visualization.nestedTable.NestedTableConfig
import org.apache.texera.amber.operator.visualization.nestedTable.NestedTableOpDesc
import org.apache.texera.amber.operator.visualization.parallelCoordinatesPlot.ParallelCoordinatesPlotOpDesc
import org.apache.texera.amber.operator.visualization.pieChart.PieChartOpDesc
import org.apache.texera.amber.operator.visualization.polarChart.PolarChartOpDesc
import org.apache.texera.amber.operator.visualization.quiverPlot.QuiverPlotOpDesc
import org.apache.texera.amber.operator.visualization.radarChart.RadarChartOpDesc
import org.apache.texera.amber.operator.visualization.radarPlot.RadarPlotLinePattern
import org.apache.texera.amber.operator.visualization.radarPlot.RadarPlotOpDesc
import org.apache.texera.amber.operator.visualization.rangeSlider.RangeSliderOpDesc
import org.apache.texera.amber.operator.visualization.sankeyDiagram.SankeyDiagramOpDesc
import org.apache.texera.amber.operator.visualization.scatter3DChart.Scatter3dChartOpDesc
import org.apache.texera.amber.operator.visualization.scatterplot.ScatterplotOpDesc
import org.apache.texera.amber.operator.visualization.stripChart.StripChartOpDesc
import org.apache.texera.amber.operator.visualization.tablesChart.TablesConfig
import org.apache.texera.amber.operator.visualization.tablesChart.TablesPlotOpDesc
import org.apache.texera.amber.operator.visualization.ternaryContour.TernaryContourOpDesc
import org.apache.texera.amber.operator.visualization.ternaryPlot.TernaryPlotOpDesc
import org.apache.texera.amber.operator.visualization.timeSeriesplot.TimeSeriesOpDesc
import org.apache.texera.amber.operator.visualization.treeplot.TreePlotOpDesc
import org.apache.texera.amber.operator.visualization.volcanoPlot.VolcanoPlotOpDesc
import org.apache.texera.amber.operator.visualization.waterfallChart.WaterfallChartOpDesc
import org.apache.texera.amber.operator.visualization.windRoseChart.WindRoseChartOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnClassifierOpDesc
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
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer.SklearnAdvancedSVCTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVRTrainer.SklearnAdvancedSVRTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer.SklearnAdvancedKNNClassifierTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer.SklearnAdvancedKNNRegressorTrainerOpDesc
import java.nio.file.Path
import java.util

/**
  * A curated handler ships a configured OpDesc and the input fixtures it
  * needs, written once into `testRoot`. Register it in [[CuratedHandlers.all]]
  * to override the auto-config tier for that operator.
  */
trait TransformHandler {
  def opDescClass: Class[_ <: LogicalOp]
  def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path])
}

/**
  * The curated override tier of the config/fixture resolution chain: an
  * operator listed here is verified with its hand-written fixture instead of
  * the auto-generated one. This is also the seam where Xuan's curated
  * operator-field-values JSON plugs in later, as a second curated source.
  */
object CuratedHandlers {
  val all: Seq[TransformHandler] = Seq(
    SpecializedFilterTransformHandler,
    IntersectTransformHandler,
    DifferenceTransformHandler,
    SymmetricDifferenceTransformHandler,
    HashJoinTransformHandler,
    SortTransformHandler,
    AggregateTransformHandler,
    DictionaryMatcherTransformHandler,
    ProjectionTransformHandler,
    BarChartVisualizationHandler,
    BulletChartVisualizationHandler,
    CandlestickChartVisualizationHandler,
    CarpetPlotVisualizationHandler,
    ChoroplethMapVisualizationHandler,
    ContinuousErrorBandsVisualizationHandler,
    ContourPlotVisualizationHandler,
    DotPlotVisualizationHandler,
    IcicleChartVisualizationHandler,
    BubbleChartVisualizationHandler,
    BoxViolinPlotVisualizationHandler,
    ImageVisualizerVisualizationHandler,
    ScatterMatrixVisualizationHandler,
    DumbbellPlotVisualizationHandler,
    ECDFPlotVisualizationHandler,
    FigureFactoryTableVisualizationHandler,
    FilledAreaPlotVisualizationHandler,
    FunnelPlotVisualizationHandler,
    GanttChartVisualizationHandler,
    GaugeChartVisualizationHandler,
    DendrogramVisualizationHandler,
    SklearnTrainingLogisticRegressionTransformHandler,
    SklearnTrainingRandomForestTransformHandler,
    SklearnTrainingDecisionTreeTransformHandler,
    SklearnTrainingGaussianNaiveBayesTransformHandler,
    SklearnTrainingKNNTransformHandler,
    SklearnTrainingSVMTransformHandler,
    SklearnTrainingAdaptiveBoostingTransformHandler,
    SklearnTrainingBaggingTransformHandler,
    SklearnTrainingBernoulliNaiveBayesTransformHandler,
    SklearnTrainingComplementNaiveBayesTransformHandler,
    SklearnTrainingDummyClassifierTransformHandler,
    SklearnTrainingExtraTreeTransformHandler,
    SklearnTrainingExtraTreesTransformHandler,
    SklearnTrainingGradientBoostingTransformHandler,
    SklearnTrainingLinearRegressionTransformHandler,
    SklearnTrainingLinearSVMTransformHandler,
    SklearnTrainingLogisticRegressionCVTransformHandler,
    SklearnTrainingMultiLayerPerceptronTransformHandler,
    SklearnTrainingMultinomialNaiveBayesTransformHandler,
    SklearnTrainingNearestCentroidTransformHandler,
    SklearnTrainingPassiveAggressiveTransformHandler,
    SklearnTrainingPerceptronTransformHandler,
    SklearnTrainingProbabilityCalibrationTransformHandler,
    SklearnTrainingRidgeTransformHandler,
    SklearnTrainingRidgeCVTransformHandler,
    SklearnTrainingSDGTransformHandler,
    HeatMapVisualizationHandler,
    HierarchyChartVisualizationHandler,
    HistogramChartVisualizationHandler,
    Histogram2DVisualizationHandler,
    LineChartVisualizationHandler,
    ParallelCoordinatesPlotVisualizationHandler,
    PieChartVisualizationHandler,
    PolarChartVisualizationHandler,
    QuiverPlotVisualizationHandler,
    RadarChartVisualizationHandler,
    RadarPlotVisualizationHandler,
    RangeSliderVisualizationHandler,
    SankeyDiagramVisualizationHandler,
    Scatter3dChartVisualizationHandler,
    ScatterplotVisualizationHandler,
    StripChartVisualizationHandler,
    TablesPlotVisualizationHandler,
    TernaryContourVisualizationHandler,
    TernaryPlotVisualizationHandler,
    TimeSeriesVisualizationHandler,
    TreePlotVisualizationHandler,
    VolcanoPlotVisualizationHandler,
    WaterfallChartVisualizationHandler,
    WindRoseChartVisualizationHandler,
    SklearnAdaptiveBoostingTransformHandler,
    SklearnBaggingTransformHandler,
    SklearnBernoulliNaiveBayesTransformHandler,
    SklearnComplementNaiveBayesTransformHandler,
    SklearnDecisionTreeTransformHandler,
    SklearnDummyClassifierTransformHandler,
    SklearnExtraTreeTransformHandler,
    SklearnExtraTreesTransformHandler,
    SklearnGaussianNaiveBayesTransformHandler,
    SklearnGradientBoostingTransformHandler,
    SklearnKNNTransformHandler,
    SklearnLinearSVMTransformHandler,
    SklearnLogisticRegressionTransformHandler,
    SklearnLogisticRegressionCVTransformHandler,
    SklearnMultiLayerPerceptronTransformHandler,
    SklearnMultinomialNaiveBayesTransformHandler,
    SklearnNearestCentroidTransformHandler,
    SklearnPassiveAggressiveTransformHandler,
    SklearnPerceptronTransformHandler,
    SklearnProbabilityCalibrationTransformHandler,
    SklearnRandomForestTransformHandler,
    SklearnRidgeTransformHandler,
    SklearnRidgeCVTransformHandler,
    SklearnSDGTransformHandler,
    SklearnSVMTransformHandler,
    SklearnLinearRegressionTransformHandler,
    SklearnAdvancedSVCTrainerTransformHandler,
    SklearnAdvancedSVRTrainerTransformHandler,
    SklearnAdvancedKNNClassifierTrainerTransformHandler,
    SklearnAdvancedKNNRegressorTrainerTransformHandler,
    MachineLearningScorerTransformHandler
  )

  val byClass: Map[Class[_ <: LogicalOp], TransformHandler] =
    all.map(h => h.opDescClass -> h).toMap

  /** Generic fixture writer: builds a JSONL file with the given typed columns
    * and rows, boxing each value per its declared [[AttributeType]]. Lets a
    * curated handler declare bespoke per-operator input data in one call
    * instead of hand-rolling a Schema + Tuple.builder loop. */
  def writeFixture(
      path: Path,
      columns: Seq[(String, AttributeType)],
      rows: Seq[Seq[Any]]
  ): Path = {
    val schema = new Schema(columns.map { case (n, t) => new Attribute(n, t) }: _*)
    val tuples = rows.map { row =>
      val builder = Tuple.builder(schema)
      columns.zip(row).foreach {
        case ((name, attrType), value) =>
          val boxed: AnyRef = (attrType, value) match {
            case (_, null)                           => null
            case (AttributeType.INTEGER, x: Int)     => Int.box(x)
            case (AttributeType.INTEGER, x: Long)    => Int.box(x.toInt)
            case (AttributeType.INTEGER, x: Double)  => Int.box(x.toInt)
            case (AttributeType.LONG, x: Long)       => Long.box(x)
            case (AttributeType.LONG, x: Int)        => Long.box(x.toLong)
            case (AttributeType.DOUBLE, x: Double)   => Double.box(x)
            case (AttributeType.DOUBLE, x: Int)      => Double.box(x.toDouble)
            case (AttributeType.DOUBLE, x: Long)     => Double.box(x.toDouble)
            case (AttributeType.BOOLEAN, x: Boolean) => Boolean.box(x)
            case (AttributeType.STRING, x)           => x.toString
            case (_, x)                              => x.toString
          }
          builder.add(schema.getAttribute(name), boxed)
      }
      builder.build()
    }
    TupleIO.writeTuples(path, tuples.iterator, schema)
    path
  }

  /** Two-input balanced binary-classification fixture (train on port 0, test on
    * port 1) for the Sklearn classifier/regressor operators. */
  def writeClassification2Input(testRoot: Path): (Path, Path) = {
    val cols = Seq(
      "x1" -> AttributeType.DOUBLE,
      "x2" -> AttributeType.DOUBLE,
      "y" -> AttributeType.INTEGER
    )
    val rows: Seq[Seq[Any]] = Seq(
      Seq(0.0, 0.0, 0), Seq(0.1, 0.2, 0), Seq(0.2, 0.1, 0), Seq(0.3, 0.3, 0),
      Seq(0.15, 0.05, 0), Seq(0.05, 0.25, 0),
      Seq(1.0, 1.0, 1), Seq(0.9, 0.8, 1), Seq(1.1, 0.9, 1), Seq(0.8, 1.2, 1),
      Seq(1.15, 1.05, 1), Seq(0.95, 1.25, 1)
    )
    val train = writeFixture(testRoot.resolve("input_port_0.jsonl"), cols, rows)
    val test = writeFixture(testRoot.resolve("input_port_1.jsonl"), cols, rows)
    (train, test)
  }
}

/**
  * Handler for `SpecializedFilterOpDesc`. Writes a 5-row table with an
  * integer and a string column, then filters on `age > 18 OR name == "eve"`.
  * Exercises numeric comparison, string equality (the JSON predicate `value`
  * is a string), and OR-combination of predicates — three corners of the
  * generator's coercion logic in one fixture.
  *
  * Both JVM `SpecializedFilterOpExec` and pandas boolean indexing preserve
  * input row order, so positional comparator equality holds.
  */
object SpecializedFilterTransformHandler extends TransformHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[SpecializedFilterOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("age", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )

    val rows: Seq[Tuple] = Seq(
      tupleOf(schema, 21, "alice"),
      tupleOf(schema, 17, "bob"),
      tupleOf(schema, 30, "carol"),
      tupleOf(schema, 15, "eve"),
      tupleOf(schema, 14, "dave")
    )

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new SpecializedFilterOpDesc()
    desc.predicates = List(
      new FilterPredicate("age", ComparisonType.GREATER_THAN, "18"),
      new FilterPredicate("name", ComparisonType.EQUAL_TO, "eve")
    )

    (desc, Map(PortIdentity(0) -> inputPath))
  }

  private def tupleOf(schema: Schema, age: Int, name: String): Tuple = {
    val builder = Tuple.builder(schema)
    builder.add(schema.getAttribute("age"), Int.box(age))
    builder.add(schema.getAttribute("name"), name)
    builder.build()
  }
}

/**
  * Two-port (id INTEGER, name STRING) fixture shared by the set-op handlers.
  * 25 left rows × 31 right rows with deliberate overlap — large enough to
  * defeat the small-set / identity-hash bucket coincidences that a 3-row
  * fixture lets through, while still small enough to compare in milliseconds.
  */
private object SetOpFixture {
  val schema: Schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING)
  )

  private val names = Vector(
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"
  )

  private def tup(id: Int, name: String): Tuple = {
    val b = Tuple.builder(schema)
    b.add(schema.getAttribute("id"), Int.box(id))
    b.add(schema.getAttribute("name"), name)
    b.build()
  }

  def writeLeftRight(testRoot: Path): Map[PortIdentity, Path] = {
    val leftPath = testRoot.resolve("input_port_0.jsonl")
    val rightPath = testRoot.resolve("input_port_1.jsonl")
    val left = (1 to 25).map(i => tup(i, names((i - 1) % names.size)))
    val right = (10 to 40).map(i => tup(i, names((i - 1) % names.size)))
    TupleIO.writeTuples(leftPath, left.iterator, schema)
    TupleIO.writeTuples(rightPath, right.iterator, schema)
    Map(PortIdentity(0) -> leftPath, PortIdentity(1) -> rightPath)
  }
}

/** Intersect: JVM keeps two `mutable.HashSet[Tuple]` and emits the
  *  intersection in bucket-iteration order. Row order isn't deterministic vs
  *  the pandas `concat + duplicated(keep="first")` path — order policy lives
  *  in [[TransformVerificationRunner.orderInsensitiveOps]]. */
object IntersectTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[IntersectOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new IntersectOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** Difference: `leftHashSet.diff(rightHashSet).iterator` — same hash-bucket
  *  order divergence as Intersect. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object DifferenceTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DifferenceOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new DifferenceOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** SymmetricDifference: union of the two diffs, hash-set backed on both
  *  sides. Most divergent of the set ops in practice — small fixtures can
  *  accidentally pass, larger ones fail. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object SymmetricDifferenceTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SymmetricDifferenceOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new SymmetricDifferenceOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** HashJoin INNER on `id`. Build (port 0) and probe (port 1) intentionally
  *  arrive in different id orders so any probe-major / left-major mismatch
  *  between the JVM emit and `pd.merge` shows up. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object HashJoinTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HashJoinOpDesc[_]]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val buildSchema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )
    val probeSchema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("score", AttributeType.INTEGER)
    )

    def buildTup(id: Int, name: String): Tuple = {
      val b = Tuple.builder(buildSchema)
      b.add(buildSchema.getAttribute("id"), Int.box(id))
      b.add(buildSchema.getAttribute("name"), name)
      b.build()
    }
    def probeTup(id: Int, score: Int): Tuple = {
      val b = Tuple.builder(probeSchema)
      b.add(probeSchema.getAttribute("id"), Int.box(id))
      b.add(probeSchema.getAttribute("score"), Int.box(score))
      b.build()
    }

    val buildRows = Seq(
      buildTup(3, "carol"),
      buildTup(1, "alice"),
      buildTup(5, "eve"),
      buildTup(2, "bob"),
      buildTup(4, "dave")
    )
    val probeRows = Seq(
      probeTup(1, 95),
      probeTup(2, 80),
      probeTup(3, 88),
      probeTup(4, 72),
      probeTup(5, 91)
    )
    val buildPath = testRoot.resolve("input_port_0.jsonl")
    val probePath = testRoot.resolve("input_port_1.jsonl")
    TupleIO.writeTuples(buildPath, buildRows.iterator, buildSchema)
    TupleIO.writeTuples(probePath, probeRows.iterator, probeSchema)

    val desc = new HashJoinOpDesc[Integer]()
    desc.buildAttributeName = "id"
    desc.probeAttributeName = "id"
    desc.joinType = JoinType.INNER

    (desc, Map(PortIdentity(0) -> buildPath, PortIdentity(1) -> probePath))
  }
}

/** Aggregate: the harness runs the JVM path (getPhysicalPlan) before
  *  standalone codegen, and getPhysicalPlan mutates `aggregations` via
  *  getFinal, setting attribute := resultAttribute — auto-config's free-form
  *  resultAttribute "1" then leaks into the generated pandas as a column ref
  *  (KeyError). Choosing resultAttribute == attribute makes the mutation a
  *  no-op. Emit-order policy (hash-partition vs first-occurrence) lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object AggregateTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[AggregateOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new AggregateOpDesc()
    val agg = new AggregationOperation()
    agg.aggFunction = AggregationFunction.SUM
    agg.attribute = "score"
    agg.resultAttribute = "score" // must equal attribute; see scaladoc
    desc.aggregations = List(agg)
    desc.groupByKeys = List("name")
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/** DictionaryMatcher: auto-config autofills the canonical fixture's first
  *  column (`id`, INTEGER) into `attribute`, but DictionaryMatcherOpExec
  *  casts that field to String (ClassCastException). Point it at `name` with
  *  a dictionary matching a strict subset of rows. Map op — both paths keep
  *  input row order, so strict positional comparison holds. */
object DictionaryMatcherTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DictionaryMatcherOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new DictionaryMatcherOpDesc()
    desc.dictionary = "alice,bob,zelda" // alice/bob recur in the fixture; zelda never matches
    desc.attribute = "name"
    desc.resultAttribute = "matched"
    desc.matchingType = MatchingType.SCANBASED
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/** Projection: `attributes` carries no @JsonProperty, so the auto-config tier
  *  leaves it empty and ProjectionOpExec rejects the empty list
  *  (Preconditions.checkArgument). Keep two columns, renaming one, to
  *  exercise both select and alias. Map op — strict order holds. */
object ProjectionTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ProjectionOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new ProjectionOpDesc()
    desc.attributes = List(
      new AttributeUnit("id", ""),
      new AttributeUnit("score", "points")
    )
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/**
  * Handler for `SortOpDesc`. Writes a tiny 4-row table with one integer and
  * one string column and sorts by the integer descending — touches both
  * column types and exercises the comparator on a non-trivial reordering.
  */
object SortTransformHandler extends TransformHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[SortOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )

    val rows: Seq[Tuple] = Seq(
      tupleOf(schema, 3, "carol"),
      tupleOf(schema, 1, "alice"),
      tupleOf(schema, 4, "dave"),
      tupleOf(schema, 2, "bob")
    )

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new SortOpDesc()
    val criterion = new SortCriteriaUnit()
    criterion.attributeName = "id"
    criterion.sortPreference = SortPreference.DESC
    desc.attributes = List(criterion)

    (desc, Map(PortIdentity(0) -> inputPath))
  }

  private def tupleOf(schema: Schema, id: Int, name: String): Tuple = {
    val builder = Tuple.builder(schema)
    builder.add(schema.getAttribute("id"), Int.box(id))
    builder.add(schema.getAttribute("name"), name)
    builder.build()
  }
}

/** BulletChart visualization fixture. Uses two numeric values so the runtime
  * path can render multiple bullet charts while the JSON comparator validates
  * the first Plotly payload. */
object BulletChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BulletChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("actual", AttributeType.DOUBLE))

    def tup(actual: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("actual"), actual)
      builder.build()
    }

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, Seq(tup(82.0), tup(91.0)).iterator, schema)

    val steps = new util.ArrayList[BulletChartStepDefinition]()
    steps.add(new BulletChartStepDefinition("0", "70"))
    steps.add(new BulletChartStepDefinition("70", "100"))

    val desc = new BulletChartOpDesc()
    desc.value = "actual"
    desc.deltaReference = "85"
    desc.thresholdValue = "90"
    desc.steps = steps

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Candlestick visualization fixture with deterministic OHLC rows. */
object CandlestickChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[CandlestickChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("date", AttributeType.STRING),
      new Attribute("open", AttributeType.DOUBLE),
      new Attribute("high", AttributeType.DOUBLE),
      new Attribute("low", AttributeType.DOUBLE),
      new Attribute("close", AttributeType.DOUBLE)
    )

    def tup(date: String, open: Double, high: Double, low: Double, close: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("date"), date)
      builder.add(schema.getAttribute("open"), open)
      builder.add(schema.getAttribute("high"), high)
      builder.add(schema.getAttribute("low"), low)
      builder.add(schema.getAttribute("close"), close)
      builder.build()
    }

    val rows = Seq(
      tup("day-1", 100.0, 110.0, 95.0, 108.0),
      tup("day-2", 108.0, 112.0, 101.0, 104.0),
      tup("day-3", 104.0, 116.0, 103.0, 115.0)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new CandlestickChartOpDesc()
    desc.date = "date"
    desc.open = "open"
    desc.high = "high"
    desc.low = "low"
    desc.close = "close"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** CarpetPlot visualization fixture using numeric parameter axes. */
object CarpetPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[CarpetPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("a", AttributeType.DOUBLE),
      new Attribute("b", AttributeType.DOUBLE),
      new Attribute("value", AttributeType.DOUBLE)
    )

    def tup(a: Double, b: Double, value: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("a"), a)
      builder.add(schema.getAttribute("b"), b)
      builder.add(schema.getAttribute("value"), value)
      builder.build()
    }

    val rows = Seq(
      tup(0.0, 0.0, 1.0),
      tup(1.0, 0.0, 2.0),
      tup(0.0, 1.0, 3.0),
      tup(1.0, 1.0, 4.0)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new CarpetPlotOpDesc()
    desc.a = "a"
    desc.b = "b"
    desc.y = "value"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ChoroplethMap visualization fixture with ISO-3 country codes. */
object ChoroplethMapVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ChoroplethMapOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("country", AttributeType.STRING),
      new Attribute("score", AttributeType.DOUBLE)
    )

    def tup(country: String, score: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("country"), country)
      builder.add(schema.getAttribute("score"), score)
      builder.build()
    }

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(
      inputPath,
      Seq(tup("USA", 10.0), tup("CAN", 7.5), tup("MEX", 6.0)).iterator,
      schema
    )

    val desc = new ChoroplethMapOpDesc()
    desc.locations = "country"
    desc.color = "score"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ContinuousErrorBands fixture with one line and deterministic upper/lower
  * bounds. */
object ContinuousErrorBandsVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ContinuousErrorBandsOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("upper", AttributeType.DOUBLE),
      new Attribute("lower", AttributeType.DOUBLE)
    )

    def tup(x: Double, y: Double, upper: Double, lower: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("upper"), upper)
      builder.add(schema.getAttribute("lower"), lower)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 2.0, 2.4, 1.6),
      tup(2.0, 3.0, 3.6, 2.5),
      tup(3.0, 2.5, 3.0, 2.0),
      tup(4.0, 4.0, 4.8, 3.3)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val band = new BandConfig()
    band.xValue = "x"
    band.yValue = "y"
    band.yUpper = "upper"
    band.yLower = "lower"
    band.mode = LineMode.LINE_WITH_DOTS
    band.name = "series"
    band.color = "#1f77b4"
    band.fillColor = "rgba(31, 119, 180, 0.2)"

    val bands = new util.ArrayList[BandConfig]()
    bands.add(band)

    val desc = new ContinuousErrorBandsOpDesc()
    desc.xLabel = "X Axis"
    desc.yLabel = "Y Axis"
    desc.bands = bands

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ContourPlot visualization fixture. The 3x3 grid gives scipy cubic
  * interpolation enough non-collinear points to produce a stable surface. */
object ContourPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ContourPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("z", AttributeType.DOUBLE)
    )

    def tup(x: Double, y: Double, z: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("z"), z)
      builder.build()
    }

    val rows =
      for {
        x <- Seq(0.0, 1.0, 2.0)
        y <- Seq(0.0, 1.0, 2.0)
      } yield tup(x, y, x * x + y)
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new ContourPlotOpDesc()
    desc.x = "x"
    desc.y = "y"
    desc.z = "z"
    desc.gridSize = "3"
    desc.connectGaps = true
    desc.coloringMethod = ContourPlotColoringFunction.HEATMAP

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** BarChart visualization fixture. Covers categorical x values, numeric y
  * values, and optional color grouping. */
object BarChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BarChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("title", AttributeType.STRING),
      new Attribute("runtime_min", AttributeType.INTEGER),
      new Attribute("genre", AttributeType.STRING)
    )

    def tup(title: String, runtime: Int, genre: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("title"), title)
      builder.add(schema.getAttribute("runtime_min"), Int.box(runtime))
      builder.add(schema.getAttribute("genre"), genre)
      builder.build()
    }

    val rows = Seq(
      tup("Silent Harbor", 85, "Horror"),
      tup("Neon Skies", 84, "Sci-Fi"),
      tup("The Last Signal", 178, "Drama"),
      tup("Paper Moons", 99, "Action")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new BarChartOpDesc()
    desc.value = "runtime_min"
    desc.fields = "title"
    desc.categoryColumn = "genre"
    desc.horizontalOrientation = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** DotPlot visualization fixture. Uses a string category column with repeated
  * values so the grouped counts in the Plotly payload are stable and
  * non-trivial. */
object DotPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DotPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("category", AttributeType.STRING))

    def tup(category: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("category"), category)
      builder.build()
    }

    val rows = Seq(
      tup("alpha"),
      tup("beta"),
      tup("alpha"),
      tup("gamma"),
      tup("beta"),
      tup("alpha")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new DotPlotOpDesc()
    desc.countAttribute = "category"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** IcicleChart visualization fixture. Uses a three-level hierarchy and
  * positive numeric values so both Plotly paths produce a stable tree. */
object IcicleChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[IcicleChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("continent", AttributeType.STRING),
      new Attribute("country", AttributeType.STRING),
      new Attribute("city", AttributeType.STRING),
      new Attribute("sales", AttributeType.DOUBLE)
    )

    def tup(continent: String, country: String, city: String, sales: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("continent"), continent)
      builder.add(schema.getAttribute("country"), country)
      builder.add(schema.getAttribute("city"), city)
      builder.add(schema.getAttribute("sales"), sales)
      builder.build()
    }

    val rows = Seq(
      tup("North America", "United States", "Irvine", 12.0),
      tup("North America", "United States", "Seattle", 8.0),
      tup("Europe", "Germany", "Berlin", 7.5),
      tup("Europe", "France", "Paris", 9.5)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    def hierarchySection(attribute: String): HierarchySection = {
      val section = new HierarchySection()
      section.attributeName = attribute
      section
    }

    val desc = new IcicleChartOpDesc()
    desc.hierarchy = List(
      hierarchySection("continent"),
      hierarchySection("country"),
      hierarchySection("city")
    )
    desc.value = "sales"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** BubbleChart visualization fixture. Covers numeric x/y/size columns and the
  * optional color category path. */
object BubbleChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BubbleChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("size", AttributeType.DOUBLE),
      new Attribute("category", AttributeType.STRING)
    )

    def tup(x: Double, y: Double, size: Double, category: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("size"), size)
      builder.add(schema.getAttribute("category"), category)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 4.0, 10.0, "alpha"),
      tup(2.0, 3.0, 18.0, "beta"),
      tup(3.0, 2.0, 25.0, "alpha"),
      tup(4.0, 1.0, 12.0, "gamma")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new BubbleChartOpDesc()
    desc.xValue = "x"
    desc.yValue = "y"
    desc.zValue = "size"
    desc.enableColor = true
    desc.colorCategory = "category"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Box/Violin Plot visualization fixture. Uses one numeric column with a
  * stable distribution so both the box and violin branches render
  * deterministically. */
object BoxViolinPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BoxViolinPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("value", AttributeType.DOUBLE))

    def tup(value: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("value"), value)
      builder.build()
    }

    val rows = Seq(
      tup(1.0),
      tup(2.0),
      tup(2.5),
      tup(4.0),
      tup(5.5),
      tup(7.0)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new BoxViolinPlotOpDesc()
    desc.value = "value"
    desc.quartileType = BoxViolinPlotQuartileFunction.LINEAR
    desc.horizontalOrientation = false
    desc.violinPlot = true

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ImageVisualizer fixture. Uses deterministic binary payloads; the operator
  * base64-encodes them into img tags. */
object ImageVisualizerVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ImageVisualizerOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("image_bytes", AttributeType.BINARY))

    def tup(bytes: Array[Byte]): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("image_bytes"), bytes)
      builder.build()
    }

    val rows = Seq(
      tup(Array[Byte](1, 2, 3, 4)),
      tup(Array[Byte](10, 20, 30, 40))
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new ImageVisualizerOpDesc()
    desc.binaryContent = "image_bytes"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ScatterMatrix visualization fixture. Uses three numeric dimensions and a
  * stable categorical color column. */
object ScatterMatrixVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ScatterMatrixChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("z", AttributeType.DOUBLE),
      new Attribute("group", AttributeType.STRING)
    )

    def tup(x: Double, y: Double, z: Double, group: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("z"), z)
      builder.add(schema.getAttribute("group"), group)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 4.0, 7.0, "alpha"),
      tup(2.0, 3.0, 6.0, "beta"),
      tup(3.0, 2.0, 5.0, "alpha"),
      tup(4.0, 1.0, 8.0, "gamma"),
      tup(5.0, 5.0, 9.0, "beta")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new ScatterMatrixChartOpDesc()
    desc.selectedAttributes = List("x", "y", "z")
    desc.color = "group"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** DumbbellPlot visualization fixture with two entities and start/end periods. */
object DumbbellPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DumbbellPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("entity", AttributeType.STRING),
      new Attribute("period", AttributeType.STRING),
      new Attribute("value", AttributeType.DOUBLE)
    )

    def tup(entity: String, period: String, value: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("entity"), entity)
      builder.add(schema.getAttribute("period"), period)
      builder.add(schema.getAttribute("value"), value)
      builder.build()
    }

    val rows = Seq(
      tup("Alpha", "start", 10.0),
      tup("Alpha", "end", 20.0),
      tup("Beta", "start", 15.0),
      tup("Beta", "end", 25.0)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new DumbbellPlotOpDesc()
    desc.categoryColumnName = "period"
    desc.dumbbellStartValue = "start"
    desc.dumbbellEndValue = "end"
    desc.measurementColumnName = "value"
    desc.comparedColumnName = "entity"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ECDFPlot visualization fixture with a stable numeric sample. */
object ECDFPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ECDFPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("value", AttributeType.DOUBLE))

    def tup(value: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("value"), value)
      builder.build()
    }

    val rows = Seq(tup(1.0), tup(2.0), tup(3.0), tup(4.0), tup(5.0))
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new ECDFPlotOpDesc()
    desc.valueColumn = "value"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** FigureFactoryTable visualization fixture with two text columns. */
object FigureFactoryTableVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[FigureFactoryTableOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("name", AttributeType.STRING),
      new Attribute("score", AttributeType.INTEGER)
    )

    def tup(name: String, score: Int): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("name"), name)
      builder.add(schema.getAttribute("score"), Int.box(score))
      builder.build()
    }

    val rows = Seq(tup("alpha", 10), tup("beta", 20), tup("gamma", 30))
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val nameCol = new FigureFactoryTableConfig()
    nameCol.attributeName = "name"
    val scoreCol = new FigureFactoryTableConfig()
    scoreCol.attributeName = "score"

    val desc = new FigureFactoryTableOpDesc()
    desc.columns = List(nameCol, scoreCol)
    desc.fontSize = 12
    desc.rowHeight = 30
    desc.fontColor = "#000000"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** FilledAreaPlot visualization fixture with a simple monotonic series. */
object FilledAreaPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[FilledAreaPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.INTEGER),
      new Attribute("y", AttributeType.INTEGER)
    )

    def tup(x: Int, y: Int): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), Int.box(x))
      builder.add(schema.getAttribute("y"), Int.box(y))
      builder.build()
    }

    val rows = Seq(tup(1, 2), tup(2, 4), tup(3, 3), tup(4, 5))
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new FilledAreaPlotOpDesc()
    desc.x = "x"
    desc.y = "y"
    desc.facetColumn = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** FunnelPlot visualization fixture with decreasing stage counts. */
object FunnelPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[FunnelPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("stage", AttributeType.STRING),
      new Attribute("count", AttributeType.INTEGER)
    )

    def tup(stage: String, count: Int): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("stage"), stage)
      builder.add(schema.getAttribute("count"), Int.box(count))
      builder.build()
    }

    val rows = Seq(tup("Visit", 100), tup("Signup", 60), tup("Purchase", 30))
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new FunnelPlotOpDesc()
    desc.x = "count"
    desc.y = "stage"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** GanttChart visualization fixture with two non-overlapping tasks. */
object GanttChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[GanttChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    // TupleIO MVP writes STRING/INT/DOUBLE only; ISO datetime strings are
    // sufficient for px.timeline on both the Texera and standalone paths.
    val schema = new Schema(
      new Attribute("task", AttributeType.STRING),
      new Attribute("start", AttributeType.STRING),
      new Attribute("finish", AttributeType.STRING)
    )

    def tup(task: String, start: String, finish: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("task"), task)
      builder.add(schema.getAttribute("start"), start)
      builder.add(schema.getAttribute("finish"), finish)
      builder.build()
    }

    val rows = Seq(
      tup("Design", "2024-01-01 09:00:00", "2024-01-01 11:00:00"),
      tup("Build", "2024-01-01 11:00:00", "2024-01-01 15:00:00")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new GanttChartOpDesc()
    desc.task = "task"
    desc.start = "start"
    desc.finish = "finish"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** GaugeChart visualization fixture with a single numeric reading. */
object GaugeChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[GaugeChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("score", AttributeType.DOUBLE))

    def tup(score: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("score"), score)
      builder.build()
    }

    val rows = Seq(tup(72.5))
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new GaugeChartOpDesc()
    desc.value = "score"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Dendrogram visualization fixture. Four 2-D points in two tight pairs give
  * scipy linkage a stable, non-degenerate hierarchy for Plotly JSON parity. */
object DendrogramVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DendrogramOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("label", AttributeType.STRING)
    )

    def tup(x: Double, y: Double, label: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("label"), label)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 1.0, "alpha"),
      tup(1.1, 1.2, "beta"),
      tup(4.0, 4.0, "gamma"),
      tup(4.2, 4.1, "delta")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new DendrogramOpDesc()
    desc.xVal = "x"
    desc.yVal = "y"
    desc.labels = "label"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Shared fixture for the Sklearn training operators: a small, separable
  * 2-feature binary-classification dataset (numeric only — the canonical
  * auto-fixture's string column can't be fit by sklearn). The fitted model
  * lands in a BINARY column the comparator ignores (functionally equivalent
  * but not bit-identical across paths); model_name and output shape are still
  * compared, so each operator is verified to run to completion on both paths.
  * Each concrete handler only supplies its estimator-specific OpDesc. */
abstract class SklearnTrainingTransformHandler extends TransformHandler {
  protected def newDesc(): SklearnTrainingOpDesc

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x1", AttributeType.DOUBLE),
      new Attribute("x2", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.INTEGER)
    )

    def tup(x1: Double, x2: Double, y: Int): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x1"), x1)
      builder.add(schema.getAttribute("x2"), x2)
      builder.add(schema.getAttribute("y"), Int.box(y))
      builder.build()
    }

    // Two well-separated clusters, 6 rows per class. Enough members per class
    // for cross-validated estimators (LogisticRegressionCV / probability
    // calibration use cv=5, which needs >= 5 samples in each class).
    val rows = Seq(
      tup(0.0, 0.0, 0),
      tup(0.1, 0.2, 0),
      tup(0.2, 0.1, 0),
      tup(0.3, 0.3, 0),
      tup(0.15, 0.05, 0),
      tup(0.05, 0.25, 0),
      tup(1.0, 1.0, 1),
      tup(0.9, 0.8, 1),
      tup(1.1, 0.9, 1),
      tup(0.8, 1.2, 1),
      tup(1.15, 1.05, 1),
      tup(0.95, 1.25, 1)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = newDesc()
    desc.target = "y"
    desc.countVectorizer = false
    desc.tfidfTransformer = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

object SklearnTrainingLogisticRegressionTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingLogisticRegressionOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingLogisticRegressionOpDesc()
}

object SklearnTrainingRandomForestTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingRandomForestOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingRandomForestOpDesc()
}

object SklearnTrainingDecisionTreeTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingDecisionTreeOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingDecisionTreeOpDesc()
}

object SklearnTrainingGaussianNaiveBayesTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingGaussianNaiveBayesOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingGaussianNaiveBayesOpDesc()
}

object SklearnTrainingKNNTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingKNNOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingKNNOpDesc()
}

object SklearnTrainingSVMTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingSVMOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingSVMOpDesc()
}

object SklearnTrainingAdaptiveBoostingTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingAdaptiveBoostingOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingAdaptiveBoostingOpDesc()
}

object SklearnTrainingBaggingTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingBaggingOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingBaggingOpDesc()
}

object SklearnTrainingBernoulliNaiveBayesTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingBernoulliNaiveBayesOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingBernoulliNaiveBayesOpDesc()
}

object SklearnTrainingComplementNaiveBayesTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] =
    classOf[SklearnTrainingComplementNaiveBayesOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingComplementNaiveBayesOpDesc()
}

object SklearnTrainingDummyClassifierTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingDummyClassifierOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingDummyClassifierOpDesc()
}

object SklearnTrainingExtraTreeTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingExtraTreeOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingExtraTreeOpDesc()
}

object SklearnTrainingExtraTreesTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingExtraTreesOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingExtraTreesOpDesc()
}

object SklearnTrainingGradientBoostingTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingGradientBoostingOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingGradientBoostingOpDesc()
}

object SklearnTrainingLinearRegressionTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingLinearRegressionOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingLinearRegressionOpDesc()
}

object SklearnTrainingLinearSVMTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingLinearSVMOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingLinearSVMOpDesc()
}

object SklearnTrainingLogisticRegressionCVTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] =
    classOf[SklearnTrainingLogisticRegressionCVOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingLogisticRegressionCVOpDesc()
}

object SklearnTrainingMultiLayerPerceptronTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] =
    classOf[SklearnTrainingMultiLayerPerceptronOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingMultiLayerPerceptronOpDesc()
}

object SklearnTrainingMultinomialNaiveBayesTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] =
    classOf[SklearnTrainingMultinomialNaiveBayesOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingMultinomialNaiveBayesOpDesc()
}

object SklearnTrainingNearestCentroidTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingNearestCentroidOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingNearestCentroidOpDesc()
}

object SklearnTrainingPassiveAggressiveTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingPassiveAggressiveOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingPassiveAggressiveOpDesc()
}

object SklearnTrainingPerceptronTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingPerceptronOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingPerceptronOpDesc()
}

object SklearnTrainingProbabilityCalibrationTransformHandler
    extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] =
    classOf[SklearnTrainingProbabilityCalibrationOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc =
    new SklearnTrainingProbabilityCalibrationOpDesc()
}

object SklearnTrainingRidgeTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingRidgeOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingRidgeOpDesc()
}

object SklearnTrainingRidgeCVTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingRidgeCVOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingRidgeCVOpDesc()
}

object SklearnTrainingSDGTransformHandler extends SklearnTrainingTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnTrainingSDGOpDesc]
  override protected def newDesc(): SklearnTrainingOpDesc = new SklearnTrainingSDGOpDesc()
}

/** Machine learning scorer fixture with a tiny labeled prediction table. */
object MachineLearningScorerTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[MachineLearningScorerOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("y", AttributeType.INTEGER),
      new Attribute("pred", AttributeType.INTEGER)
    )

    def tup(y: Int, pred: Int): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("y"), Int.box(y))
      builder.add(schema.getAttribute("pred"), Int.box(pred))
      builder.build()
    }

    val rows = Seq(
      tup(0, 0),
      tup(0, 0),
      tup(1, 1),
      tup(1, 0),
      tup(1, 1)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new MachineLearningScorerOpDesc()
    desc.isRegression = false
    desc.actualValueColumn = "y"
    desc.predictValueColumn = "pred"
    desc.classificationMetrics = List(classificationMetricsFnc.accuracy)

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** HeatMap visualization fixture. A 3x3 grid of categorical x/y axes with a
  * deterministic numeric z value gives go.Heatmap a stable, fully-populated
  * matrix so both the runtime and standalone Plotly payloads match. */
object HeatMapVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HeatMapOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val rows: Seq[Seq[Any]] =
      for {
        x <- Seq("a", "b", "c")
        y <- Seq("p", "q", "r")
      } yield Seq(x, y, x.head.toInt.toDouble + y.head.toInt.toDouble)

    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq(
        "x" -> AttributeType.STRING,
        "y" -> AttributeType.STRING,
        "z" -> AttributeType.DOUBLE
      ),
      rows
    )

    val desc = new HeatMapOpDesc()
    desc.x = "x"
    desc.y = "y"
    desc.value = "z"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** HierarchyChart visualization fixture. Uses a three-level hierarchy and
  * positive numeric values so both Plotly treemap paths produce a stable tree. */
object HierarchyChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HierarchyChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq(
        "continent" -> AttributeType.STRING,
        "country" -> AttributeType.STRING,
        "city" -> AttributeType.STRING,
        "sales" -> AttributeType.DOUBLE
      ),
      Seq(
        Seq("North America", "United States", "Irvine", 12.0),
        Seq("North America", "United States", "Seattle", 8.0),
        Seq("North America", "Canada", "Toronto", 6.5),
        Seq("Europe", "Germany", "Berlin", 7.5),
        Seq("Europe", "France", "Paris", 9.5),
        Seq("Europe", "France", "Lyon", 4.0)
      )
    )

    def hierarchySection(attribute: String): HierarchySection = {
      val section = new HierarchySection()
      section.attributeName = attribute
      section
    }

    val desc = new HierarchyChartOpDesc()
    desc.hierarchyChartType = HierarchyChartType.TREEMAP
    desc.hierarchy = List(
      hierarchySection("continent"),
      hierarchySection("country"),
      hierarchySection("city")
    )
    desc.value = "sales"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** HistogramChart visualization fixture. A numeric value column plus a string
  * color column give px.histogram a stable, non-degenerate distribution for
  * Plotly JSON parity between the Texera and standalone paths. */
object HistogramChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HistogramChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq("amount" -> AttributeType.DOUBLE, "group" -> AttributeType.STRING),
      Seq(
        Seq(1.0, "a"),
        Seq(2.0, "a"),
        Seq(2.0, "b"),
        Seq(3.0, "b"),
        Seq(3.0, "a"),
        Seq(4.0, "b"),
        Seq(5.0, "a"),
        Seq(5.0, "b")
      )
    )

    val desc = new HistogramChartOpDesc()
    desc.value = "amount"
    desc.color = "group"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Histogram2D visualization fixture. Two numeric columns spread across a few
  * X/Y bins so px.density_heatmap produces a stable, non-degenerate Plotly
  * payload on both paths. Default bins (10) and DENSITY normalization. */
object Histogram2DVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[Histogram2DOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq("x" -> AttributeType.DOUBLE, "y" -> AttributeType.DOUBLE),
      Seq(
        Seq(1.0, 1.0),
        Seq(1.0, 2.0),
        Seq(2.0, 1.0),
        Seq(2.0, 2.0),
        Seq(3.0, 3.0),
        Seq(3.0, 1.0),
        Seq(1.0, 3.0),
        Seq(2.0, 3.0)
      )
    )

    val desc = new Histogram2DOpDesc()
    desc.xColumn = "x"
    desc.yColumn = "y"
    desc.xBins = 10
    desc.yBins = 10
    desc.normalize = NormalizationType.DENSITY

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** LineChart visualization fixture. One configured line over a monotonic
  * x with a stable y series; LINE_WITH_DOTS mode and empty name/color exercise
  * the default name=yValue branch. Six rows give the Scatter trace enough
  * points to render deterministically on both paths. */
object LineChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[LineChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq("x" -> AttributeType.DOUBLE, "y" -> AttributeType.DOUBLE),
      Seq(
        Seq(1.0, 2.0),
        Seq(2.0, 3.5),
        Seq(3.0, 2.8),
        Seq(4.0, 5.0),
        Seq(5.0, 4.2),
        Seq(6.0, 6.1)
      )
    )

    val line = new LineConfig()
    line.xValue = "x"
    line.yValue = "y"
    line.mode = LineMode.LINE_WITH_DOTS

    val lines = new util.ArrayList[LineConfig]()
    lines.add(line)

    val desc = new LineChartOpDesc()
    desc.xLabel = "X Axis"
    desc.yLabel = "Y Axis"
    desc.lines = lines

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** NestedTable visualization fixture. Three columns across two attribute
  * groups (one renamed) exercise the depth-two MultiIndex header, the
  * group-sort, and the precision=2 numeric formatting in the styled HTML. Six
  * rows give the table stable, non-trivial body content. */

/** ParallelCoordinatesPlot visualization fixture. Three numeric dimension
  * axes plus a categorical color column, with no nulls so the operator's
  * notnull-filter keeps every row. */
object ParallelCoordinatesPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ParallelCoordinatesPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    // px.parallel_coordinates requires every dimension column AND the color
    // column to be numeric (it builds numeric Plotly axes and a continuous
    // color scale). The color column therefore must be DOUBLE, not a string —
    // pointing `color` at a string column raises a Plotly property validation
    // error on Path A. All four columns are DOUBLE so both paths build the
    // identical figure. Six fully-populated rows survive the notnull filter.
    CuratedHandlers.writeFixture(
      inputPath,
      Seq(
        "d1" -> AttributeType.DOUBLE,
        "d2" -> AttributeType.DOUBLE,
        "d3" -> AttributeType.DOUBLE,
        "cgroup" -> AttributeType.DOUBLE
      ),
      Seq(
        Seq(1.0, 4.0, 7.0, 10.0),
        Seq(2.0, 3.0, 6.0, 20.0),
        Seq(3.0, 2.0, 5.0, 10.0),
        Seq(4.0, 1.0, 8.0, 30.0),
        Seq(5.0, 5.0, 9.0, 20.0),
        Seq(6.0, 2.5, 4.0, 10.0)
      )
    )

    val desc = new ParallelCoordinatesPlotOpDesc()
    desc.dimensions = List("d1", "d2", "d3")
    desc.color = "cgroup"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** PieChart visualization fixture. Six rows with unique category names and
  * positive numeric values: the runtime path drops NA on both columns and
  * errors on duplicate names, so distinct names keep px.pie deterministic
  * across both paths. */
object PieChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[PieChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq("category" -> AttributeType.STRING, "amount" -> AttributeType.DOUBLE),
      Seq(
        Seq("alpha", 12.0),
        Seq("beta", 8.0),
        Seq("gamma", 15.0),
        Seq("delta", 5.0),
        Seq("epsilon", 20.0),
        Seq("zeta", 10.0)
      )
    )

    val desc = new PieChartOpDesc()
    desc.name = "category"
    desc.value = "amount"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** PolarChart visualization fixture. Both `r` (radial) and `theta` (angular)
  * columns must be numeric — the runtime path guards on np.issubdtype(np.number)
  * — so DOUBLE columns make the figure identical on both paths. Six points
  * spread around the circle give a non-degenerate Scatterpolargl trace. */
object PolarChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[PolarChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq("r" -> AttributeType.DOUBLE, "theta" -> AttributeType.DOUBLE),
      Seq(
        Seq(1.0, 0.0),
        Seq(2.0, 60.0),
        Seq(3.0, 120.0),
        Seq(4.0, 180.0),
        Seq(5.0, 240.0),
        Seq(6.0, 300.0)
      )
    )

    val desc = new PolarChartOpDesc()
    desc.r = "r"
    desc.theta = "theta"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** QuiverPlot visualization fixture. A 3x3 grid of arrow origins (x, y) with
  * deterministic numeric vector components (u, v) gives plotly.figure_factory
  * create_quiver a stable, non-degenerate vector field for Plotly JSON parity. */
object QuiverPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[QuiverPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq(
        "x" -> AttributeType.DOUBLE,
        "y" -> AttributeType.DOUBLE,
        "u" -> AttributeType.DOUBLE,
        "v" -> AttributeType.DOUBLE
      ),
      Seq(
        Seq(0.0, 0.0, 1.0, 0.5),
        Seq(1.0, 0.0, 0.8, 0.6),
        Seq(2.0, 0.0, 0.6, 0.8),
        Seq(0.0, 1.0, 0.5, 1.0),
        Seq(1.0, 1.0, 0.7, 0.7),
        Seq(2.0, 1.0, 0.9, 0.4),
        Seq(0.0, 2.0, 0.4, 0.9),
        Seq(1.0, 2.0, 0.6, 0.6),
        Seq(2.0, 2.0, 1.0, 0.3)
      )
    )

    val desc = new QuiverPlotOpDesc()
    desc.x = "x"
    desc.y = "y"
    desc.u = "u"
    desc.v = "v"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** RadarChart visualization fixture. One string name column identifies each
  * radar trace; three numeric axis columns give Scatterpolar stable,
  * non-degenerate vertices. Six rows so the chart renders multiple traces. */
object RadarChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[RadarChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq(
        "entity" -> AttributeType.STRING,
        "speed" -> AttributeType.DOUBLE,
        "power" -> AttributeType.DOUBLE,
        "range" -> AttributeType.DOUBLE
      ),
      Seq(
        Seq("Alpha", 80.0, 60.0, 70.0),
        Seq("Beta", 50.0, 90.0, 40.0),
        Seq("Gamma", 65.0, 55.0, 85.0),
        Seq("Delta", 75.0, 45.0, 60.0),
        Seq("Epsilon", 40.0, 80.0, 95.0),
        Seq("Zeta", 90.0, 70.0, 50.0)
      )
    )

    val desc = new RadarChartOpDesc()
    desc.nameColumn = "entity"
    desc.valueColumns = List("speed", "power", "range")
    desc.fillOpacity = 0.5

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** RadarPlot visualization fixture. Three numeric axis columns plus a string
  * trace-name column and a string trace-color column, exercising both the
  * normalize path and the per-trace coloring path. Six rows give the radar
  * several distinct traces. */
object RadarPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[RadarPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq(
        "speed" -> AttributeType.DOUBLE,
        "power" -> AttributeType.DOUBLE,
        "range" -> AttributeType.DOUBLE,
        "model" -> AttributeType.STRING,
        "brand" -> AttributeType.STRING
      ),
      Seq(
        Seq(8.0, 6.0, 7.0, "alpha", "x"),
        Seq(5.0, 9.0, 4.0, "beta", "y"),
        Seq(7.0, 7.0, 8.0, "gamma", "x"),
        Seq(6.0, 5.0, 9.0, "delta", "y"),
        Seq(9.0, 4.0, 6.0, "epsilon", "z"),
        Seq(4.0, 8.0, 5.0, "zeta", "z")
      )
    )

    val desc = new RadarPlotOpDesc()
    desc.selectedAttributes = List("speed", "power", "range")
    desc.traceNameAttribute = "model"
    desc.traceColorAttribute = "brand"
    desc.linePattern = RadarPlotLinePattern.SOLID
    desc.maxNormalize = true
    desc.fillTrace = true
    desc.showMarkers = true
    desc.showLegend = true

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** RangeSlider visualization fixture: a numeric y-series over a categorical
  * x-axis. duplicateType defaults to NOTHING, so no groupby is applied and the
  * scatter trace plots every row directly. */
object RangeSliderVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[RangeSliderOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq("date" -> AttributeType.STRING, "price" -> AttributeType.DOUBLE),
      Seq(
        Seq("2024-01-01", 10.0),
        Seq("2024-01-02", 12.5),
        Seq("2024-01-03", 9.0),
        Seq("2024-01-04", 15.0),
        Seq("2024-01-05", 11.0),
        Seq("2024-01-06", 14.0),
        Seq("2024-01-07", 13.5)
      )
    )

    val desc = new RangeSliderOpDesc()
    desc.xAxis = "date"
    desc.yAxis = "price"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** SankeyDiagram visualization fixture. Source/target string columns plus a
  * numeric flow value. Rows share repeated source/target pairs so the
  * operator's groupby+sum produces deterministic aggregated flows for the
  * Plotly Sankey payload. */
object SankeyDiagramVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SankeyDiagramOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq(
        "source" -> AttributeType.STRING,
        "target" -> AttributeType.STRING,
        "value" -> AttributeType.DOUBLE
      ),
      Seq(
        Seq("A", "X", 5.0),
        Seq("A", "Y", 3.0),
        Seq("B", "X", 2.0),
        Seq("B", "Y", 4.0),
        Seq("A", "X", 1.0),
        Seq("C", "Y", 6.0)
      )
    )

    val desc = new SankeyDiagramOpDesc()
    desc.sourceAttribute = "source"
    desc.targetAttribute = "target"
    desc.valueAttribute = "value"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Scatter3D chart visualization fixture. Three numeric axes with a stable,
  * non-degenerate point cloud so go.Scatter3d renders identical Plotly JSON on
  * both the runtime and standalone paths. */
object Scatter3dChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[Scatter3dChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq(
        "x" -> AttributeType.DOUBLE,
        "y" -> AttributeType.DOUBLE,
        "z" -> AttributeType.DOUBLE
      ),
      Seq(
        Seq(1.0, 4.0, 7.0),
        Seq(2.0, 3.0, 6.0),
        Seq(3.0, 2.0, 5.0),
        Seq(4.0, 1.0, 8.0),
        Seq(5.0, 5.0, 9.0),
        Seq(6.0, 2.5, 4.5)
      )
    )

    val desc = new Scatter3dChartOpDesc()
    desc.x = "x"
    desc.y = "y"
    desc.z = "z"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Scatterplot visualization fixture. Numeric x/y, a categorical color column
  * and a string hover column exercise every optional argument branch in the
  * standalone codegen; all rows are complete so the dropna is a no-op. */
object ScatterplotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ScatterplotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq(
        "x" -> AttributeType.DOUBLE,
        "y" -> AttributeType.DOUBLE,
        "category" -> AttributeType.STRING,
        "label" -> AttributeType.STRING
      ),
      Seq(
        Seq(1.0, 2.0, "alpha", "p1"),
        Seq(2.0, 3.5, "beta", "p2"),
        Seq(3.0, 1.5, "alpha", "p3"),
        Seq(4.0, 4.0, "gamma", "p4"),
        Seq(5.0, 2.5, "beta", "p5"),
        Seq(6.0, 5.0, "gamma", "p6")
      )
    )

    val desc = new ScatterplotOpDesc()
    desc.getClass.getDeclaredFields
      .find(_.getName == "xColumn")
      .foreach { f => f.setAccessible(true); f.set(desc, "x") }
    desc.getClass.getDeclaredFields
      .find(_.getName == "yColumn")
      .foreach { f => f.setAccessible(true); f.set(desc, "y") }
    desc.getClass.getDeclaredFields
      .find(_.getName == "colorColumn")
      .foreach { f => f.setAccessible(true); f.set(desc, "category") }
    desc.hoverName = "label"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** StripChart visualization fixture. Numeric x against a categorical y, with a
  * repeated color category so the optional colorBy branch is exercised and the
  * px.strip jitter positions stay stable across both paths. */
object StripChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[StripChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq(
        "value" -> AttributeType.DOUBLE,
        "group" -> AttributeType.STRING,
        "cohort" -> AttributeType.STRING
      ),
      Seq(
        Seq(1.0, "A", "x"),
        Seq(2.0, "A", "y"),
        Seq(3.0, "B", "x"),
        Seq(4.0, "B", "y"),
        Seq(5.0, "A", "x"),
        Seq(6.0, "B", "y")
      )
    )

    val desc = new StripChartOpDesc()
    desc.x = "value"
    desc.y = "group"
    desc.colorBy = "cohort"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** TablesPlot visualization fixture. Selects two columns (a string label and
  * an integer measure) from a 6-row, null-free table into a Plotly go.Table.
  * The fixture has no missing values so the operator's dropna(subset=...) is a
  * no-op and both paths render identical header/cell arrays. */
object TablesPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[TablesPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq("name" -> AttributeType.STRING, "score" -> AttributeType.INTEGER),
      Seq(
        Seq("alpha", 10),
        Seq("beta", 20),
        Seq("gamma", 30),
        Seq("delta", 40),
        Seq("epsilon", 50),
        Seq("zeta", 60)
      )
    )

    val nameCol = new TablesConfig()
    nameCol.attributeName = "name"
    val scoreCol = new TablesConfig()
    scoreCol.attributeName = "score"

    val desc = new TablesPlotOpDesc()
    desc.includedColumns = List(nameCol, scoreCol)

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** TernaryContour visualization fixture. Provides four numeric columns: three
  * mixture components (a, b, c) spread across the simplex with strictly
  * positive sums and non-negative values, plus a measured value z. Enough
  * distinct compositions for ff.create_ternary_contour's cartesian
  * interpolation to build a stable surface. */
object TernaryContourVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[TernaryContourOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("a", AttributeType.DOUBLE),
      new Attribute("b", AttributeType.DOUBLE),
      new Attribute("c", AttributeType.DOUBLE),
      new Attribute("value", AttributeType.DOUBLE)
    )

    def tup(a: Double, b: Double, c: Double, value: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("a"), a)
      builder.add(schema.getAttribute("b"), b)
      builder.add(schema.getAttribute("c"), c)
      builder.add(schema.getAttribute("value"), value)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 0.0, 0.0, 1.0),
      tup(0.0, 1.0, 0.0, 2.0),
      tup(0.0, 0.0, 1.0, 3.0),
      tup(0.5, 0.5, 0.0, 1.5),
      tup(0.5, 0.0, 0.5, 2.0),
      tup(0.0, 0.5, 0.5, 2.5),
      tup(0.34, 0.33, 0.33, 2.0),
      tup(0.6, 0.2, 0.2, 1.8)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new TernaryContourOpDesc()
    desc.firstVariable = "a"
    desc.secondVariable = "b"
    desc.thirdVariable = "c"
    desc.fourthVariable = "value"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** TernaryPlot visualization fixture: three numeric composition columns
  * (a + b + c proportions) with color disabled, matching the operator's
  * px.scatter_ternary(table, a, b, c) path. */
object TernaryPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[TernaryPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq(
        "compA" -> AttributeType.DOUBLE,
        "compB" -> AttributeType.DOUBLE,
        "compC" -> AttributeType.DOUBLE
      ),
      Seq(
        Seq(0.6, 0.3, 0.1),
        Seq(0.2, 0.5, 0.3),
        Seq(0.1, 0.1, 0.8),
        Seq(0.4, 0.4, 0.2),
        Seq(0.33, 0.33, 0.34),
        Seq(0.5, 0.25, 0.25),
        Seq(0.7, 0.2, 0.1)
      )
    )

    val desc = new TernaryPlotOpDesc()
    desc.firstVariable = "compA"
    desc.secondVariable = "compB"
    desc.thirdVariable = "compC"
    desc.colorEnabled = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** TimeSeries visualization fixture. A single ISO-date time column and a
  * numeric value column over six chronological rows; pd.to_datetime parses
  * the dates and px.line renders a stable single-trace figure. Category and
  * facet columns are left at their "No Selection" defaults so neither the
  * color nor facet branch is exercised. */
object TimeSeriesVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[TimeSeriesOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq("date" -> AttributeType.STRING, "value" -> AttributeType.DOUBLE),
      Seq(
        Seq("2024-01-01", 10.0),
        Seq("2024-01-02", 12.5),
        Seq("2024-01-03", 9.0),
        Seq("2024-01-04", 15.0),
        Seq("2024-01-05", 13.5),
        Seq("2024-01-06", 17.0)
      )
    )

    val desc = new TimeSeriesOpDesc()
    desc.timeColumn = "date"
    desc.valueColumn = "value"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** TreePlot visualization fixture. A single STRING column of "[parent, child]"
  * string literals (parsed by ast.literal_eval) describing a small, fully
  * connected tree: one root, two interior nodes, and leaves. Both paths feed
  * the same edge list into igraph's Reingold-Tilford ('rt') layout, so the node
  * coordinates — and thus the Plotly JSON — are identical. */
object TreePlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[TreePlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq("edge" -> AttributeType.STRING),
      Seq(
        Seq("['root', 'a']"),
        Seq("['root', 'b']"),
        Seq("['a', 'a1']"),
        Seq("['a', 'a2']"),
        Seq("['b', 'b1']"),
        Seq("['b', 'b2']")
      )
    )

    val desc = new TreePlotOpDesc()
    desc.edgeListColumn = "edge"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** VolcanoPlot visualization fixture. A leading STRING `gene` column serves as
  * `table.columns[0]` (the hover_name), with a numeric log2 fold-change
  * (effectColumn, also driving the continuous color scale) and strictly
  * positive p-values (pvalueColumn, transformed via -log10). All p-values are
  * > 0 so the operator's filter keeps every row and both paths render the same
  * scatter. */
object VolcanoPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[VolcanoPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq(
        "gene" -> AttributeType.STRING,
        "log2fc" -> AttributeType.DOUBLE,
        "pvalue" -> AttributeType.DOUBLE
      ),
      Seq(
        Seq("GENE_A", -2.5, 0.001),
        Seq("GENE_B", 1.8, 0.02),
        Seq("GENE_C", 0.3, 0.45),
        Seq("GENE_D", 3.1, 0.0005),
        Seq("GENE_E", -1.2, 0.08),
        Seq("GENE_F", 2.4, 0.005),
        Seq("GENE_G", -0.6, 0.3)
      )
    )

    val desc = new VolcanoPlotOpDesc()
    desc.effectColumn = "log2fc"
    desc.pvalueColumn = "pvalue"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** WaterfallChart visualization fixture: categorical stages with relative
  * numeric deltas; the final row is rendered as the cumulative total. */
object WaterfallChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[WaterfallChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    CuratedHandlers.writeFixture(
      inputPath,
      Seq("stage" -> AttributeType.STRING, "amount" -> AttributeType.DOUBLE),
      Seq(
        Seq("Sales", 60.0),
        Seq("Consulting", 20.0),
        Seq("Net revenue", -15.0),
        Seq("Purchases", -10.0),
        Seq("Other", 5.0),
        Seq("Adjustments", 8.0),
        Seq("Total", 68.0)
      )
    )

    val desc = new WaterfallChartOpDesc()
    desc.xColumn = "stage"
    desc.yColumn = "amount"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** WindRoseChart visualization fixture. Provides a numeric radial column, a
  * categorical angular (direction) column, and a categorical color group so
  * both the optional color branch and the px.bar_polar call render
  * deterministically. */
object WindRoseChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[WindRoseChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_0.jsonl"),
      Seq(
        "frequency" -> AttributeType.DOUBLE,
        "direction" -> AttributeType.STRING,
        "strength" -> AttributeType.STRING
      ),
      Seq(
        Seq(5.0, "N", "weak"),
        Seq(8.0, "NE", "strong"),
        Seq(3.0, "E", "weak"),
        Seq(6.0, "SE", "strong"),
        Seq(4.0, "S", "weak"),
        Seq(9.0, "SW", "strong"),
        Seq(7.0, "W", "weak"),
        Seq(2.0, "NW", "strong")
      )
    )

    val desc = new WindRoseChartOpDesc()
    desc.rColumn = "frequency"
    desc.thetaColumn = "direction"
    desc.colorColumn = "strength"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Shared two-input fixture for the Sklearn classifier operators (training
  * port 0 + testing port 1). The fitted model lands in a BINARY column; the
  * comparator unpickles both paths' models and compares their predictions on
  * the training features, verifying behavior, not bytes. Each concrete handler
  * supplies its estimator-specific OpDesc. */
abstract class SklearnClassifierTransformHandler extends TransformHandler {
  protected def newDesc(): SklearnClassifierOpDesc

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val (train, test) = CuratedHandlers.writeClassification2Input(testRoot)
    val desc = newDesc()
    desc.target = "y"
    desc.countVectorizer = false
    (desc, Map(PortIdentity(0) -> train, PortIdentity(1) -> test))
  }
}

object SklearnAdaptiveBoostingTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnAdaptiveBoostingOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnAdaptiveBoostingOpDesc()
}

object SklearnBaggingTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnBaggingOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnBaggingOpDesc()
}

object SklearnBernoulliNaiveBayesTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnBernoulliNaiveBayesOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnBernoulliNaiveBayesOpDesc()
}

object SklearnComplementNaiveBayesTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnComplementNaiveBayesOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnComplementNaiveBayesOpDesc()
}

object SklearnDecisionTreeTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnDecisionTreeOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnDecisionTreeOpDesc()
}

object SklearnDummyClassifierTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnDummyClassifierOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnDummyClassifierOpDesc()
}

object SklearnExtraTreeTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnExtraTreeOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnExtraTreeOpDesc()
}

object SklearnExtraTreesTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnExtraTreesOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnExtraTreesOpDesc()
}

object SklearnGaussianNaiveBayesTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnGaussianNaiveBayesOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnGaussianNaiveBayesOpDesc()
}

object SklearnGradientBoostingTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnGradientBoostingOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnGradientBoostingOpDesc()
}

object SklearnKNNTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnKNNOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnKNNOpDesc()
}

object SklearnLinearSVMTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnLinearSVMOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnLinearSVMOpDesc()
}

object SklearnLogisticRegressionTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnLogisticRegressionOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnLogisticRegressionOpDesc()
}

object SklearnLogisticRegressionCVTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnLogisticRegressionCVOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnLogisticRegressionCVOpDesc()
}

object SklearnMultiLayerPerceptronTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnMultiLayerPerceptronOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnMultiLayerPerceptronOpDesc()
}

object SklearnMultinomialNaiveBayesTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnMultinomialNaiveBayesOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnMultinomialNaiveBayesOpDesc()
}

object SklearnNearestCentroidTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnNearestCentroidOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnNearestCentroidOpDesc()
}

object SklearnPassiveAggressiveTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnPassiveAggressiveOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnPassiveAggressiveOpDesc()
}

object SklearnPerceptronTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnPerceptronOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnPerceptronOpDesc()
}

object SklearnProbabilityCalibrationTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnProbabilityCalibrationOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnProbabilityCalibrationOpDesc()
}

object SklearnRandomForestTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnRandomForestOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnRandomForestOpDesc()
}

object SklearnRidgeTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnRidgeOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnRidgeOpDesc()
}

object SklearnRidgeCVTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnRidgeCVOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnRidgeCVOpDesc()
}

object SklearnSDGTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnSDGOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnSDGOpDesc()
}

object SklearnSVMTransformHandler extends SklearnClassifierTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnSVMOpDesc]
  override protected def newDesc(): SklearnClassifierOpDesc = new SklearnSVMOpDesc()
}

object SklearnLinearRegressionTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnLinearRegressionOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val (train, test) = CuratedHandlers.writeClassification2Input(testRoot)
    val desc = new SklearnLinearRegressionOpDesc()
    desc.target = "y"
    (desc, Map(PortIdentity(0) -> train, PortIdentity(1) -> test))
  }
}

/** Advanced (hyperparameter-sweep) trainers: train on port 0, parameter table
  * on port 1. With an empty paraList the estimator uses default hyperparameters
  * (one model, no sweep). The model lands in a BINARY column compared by
  * prediction behavior; Parameters is an empty string on both paths. */
abstract class SklearnAdvancedTrainerTransformHandler extends TransformHandler {
  protected def newDesc(): SklearnMLOperatorDescriptor[_]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val (train, param) = CuratedHandlers.writeClassification2Input(testRoot)
    val desc = newDesc()
    desc.groundTruthAttribute = "y"
    desc.selectedFeatures = List("x1", "x2")
    (desc, Map(PortIdentity(0) -> train, PortIdentity(1) -> param))
  }
}

object SklearnAdvancedSVCTrainerTransformHandler extends SklearnAdvancedTrainerTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnAdvancedSVCTrainerOpDesc]
  override protected def newDesc(): SklearnMLOperatorDescriptor[_] = new SklearnAdvancedSVCTrainerOpDesc()
}

object SklearnAdvancedSVRTrainerTransformHandler extends SklearnAdvancedTrainerTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnAdvancedSVRTrainerOpDesc]
  override protected def newDesc(): SklearnMLOperatorDescriptor[_] = new SklearnAdvancedSVRTrainerOpDesc()
}

object SklearnAdvancedKNNClassifierTrainerTransformHandler extends SklearnAdvancedTrainerTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnAdvancedKNNClassifierTrainerOpDesc]
  override protected def newDesc(): SklearnMLOperatorDescriptor[_] = new SklearnAdvancedKNNClassifierTrainerOpDesc()
}

object SklearnAdvancedKNNRegressorTrainerTransformHandler extends SklearnAdvancedTrainerTransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnAdvancedKNNRegressorTrainerOpDesc]
  override protected def newDesc(): SklearnMLOperatorDescriptor[_] = new SklearnAdvancedKNNRegressorTrainerOpDesc()
}
