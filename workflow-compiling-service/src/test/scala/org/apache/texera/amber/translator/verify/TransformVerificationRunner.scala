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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{BooleanNode, IntNode, TextNode}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.operator.{
  LogicalOp,
  PythonOperatorDescriptor,
  StandaloneCodeGenerator
}
import org.apache.texera.amber.operator.aggregate.AggregateOpDesc
import org.apache.texera.amber.operator.dummy.DummyOpDesc
import org.apache.texera.amber.operator.filter.SpecializedFilterOpDesc
import org.apache.texera.amber.operator.sleep.SleepOpDesc
import org.apache.texera.amber.operator.split.SplitOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnPredictionOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnClassifierOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnGaussianNaiveBayesOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnLinearRegressionOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor
import org.apache.texera.amber.operator.machineLearning.Scorer.MachineLearningScorerOpDesc
import org.apache.texera.amber.operator.huggingFace.HuggingFaceSpamSMSDetectionOpDesc
import org.apache.texera.amber.operator.sklearn.training.SklearnTrainingOpDesc
import org.apache.texera.amber.operator.sklearn.training.SklearnTrainingGaussianNaiveBayesOpDesc
import org.apache.texera.amber.operator.sklearn.testing.SklearnTestingOpDesc
import org.apache.texera.amber.operator.typecasting.TypeCastingOpDesc
import org.apache.texera.amber.operator.visualization.wordCloud.WordCloudOpDesc
import org.apache.texera.amber.operator.visualization.DotPlot.DotPlotOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotOpDesc
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc
import org.apache.texera.amber.operator.visualization.IcicleChart.IcicleChartOpDesc
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

import org.apache.texera.amber.operator.visualization.heatMap.HeatMapOpDesc
import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchyChartOpDesc
import org.apache.texera.amber.operator.visualization.histogram2d.Histogram2DOpDesc
import org.apache.texera.amber.operator.visualization.histogram.HistogramChartOpDesc
import org.apache.texera.amber.operator.visualization.lineChart.LineChartOpDesc
import org.apache.texera.amber.operator.visualization.nestedTable.NestedTableOpDesc
import org.apache.texera.amber.operator.visualization.networkGraph.NetworkGraphOpDesc
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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
  * Unified verification runner for non-source operators implementing
  * [[StandaloneCodeGenerator]]. Resolves, per operator:
  *   - Path A engine: [[PyOpExecHarness]] for PythonOperatorDescriptor,
  *     [[OpExecHarness]] otherwise; Path B is always [[StandaloneRunner]].
  *   - Config + fixture: curated handler ([[CuratedHandlers]]) if registered,
  *     else [[ConfigGenerator]] against the [[CanonicalFixture]] schemas.
  *   - Comparison: order-insensitive by default (parallel output order isn't a
  *     contract); strict positional only when the operator declares
  *     `orderSensitive = true` (the sort family). All output ports are compared.
  * Operators that can't be run are Flagged with a reason — never silently
  * skipped.
  */
object TransformVerificationRunner {

  /**
    * Per-operator knob handling: a value this operator's variants must carry,
    * and where it applies. Two needs, one table, because both answer the same
    * question — what does the generator have to be told about this operator's
    * knobs that its metadata does not say.
    *
    * `Pinned` holds a knob at one value and keeps it out of the sweep, for a
    * knob whose other value selects non-determinism rather than another
    * behaviour to check. Split's "Auto-Generate Seed" is the case: with it on
    * the executor seeds from the clock, so that run agrees with nothing, its own
    * previous run included. The value reaches the test name via
    * [[pinnedTierNote]], so the run does not read as full coverage.
    *
    * `WithOptionals` sets a knob inside the `optionals` variant, for a branch
    * that needs a switch and the field it governs together. Ternary Plot colours
    * its points only when both are set, and the sweep and the optional fill each
    * supply one, so neither variant reached the coloured branch.
    *
    * Named per operator rather than applied wholesale, because switches are not
    * generally independent: turning every Boolean on at once paired Sklearn's
    * two mutually exclusive text pipelines, among others.
    */
  sealed trait KnobScope
  object KnobScope {
    case object Pinned extends KnobScope
    case object WithOptionals extends KnobScope
  }

  final case class Knob(field: String, value: JsonNode, scope: KnobScope)

  val knobOverrides: Map[Class[_], Seq[Knob]] = Map(
    classOf[SplitOpDesc] -> Seq(Knob("random", BooleanNode.FALSE, KnobScope.Pinned)),
    // `SleepOpExec` sleeps this many seconds per tuple, and the generator fills a
    // required Int with half the row count, so the fixture would spend tens of
    // seconds asleep for nothing: the delay never reaches the output being
    // compared, and the standalone translation is a passthrough by design.
    classOf[SleepOpDesc] -> Seq(Knob("sleepTime", IntNode.valueOf(0), KnobScope.Pinned)),
    classOf[TernaryPlotOpDesc] -> Seq(
      Knob("colorEnabled", BooleanNode.TRUE, KnobScope.WithOptionals)
    ),
    // Both text switches are pinned off on the numeric table: the pipeline they
    // build reads a column that table does not have, and `tfidfTransformer` has
    // no meaning at all outside a CountVectorizer pipeline (the schema hides it
    // when the vectorizer is off). Their branches are generated against the text
    // table by the [[AltScenario]]s instead.
    classOf[SklearnClassifierOpDesc] -> Seq(
      Knob("countVectorizer", BooleanNode.FALSE, KnobScope.Pinned),
      Knob("tfidfTransformer", BooleanNode.FALSE, KnobScope.Pinned)
    ),
    classOf[SklearnTrainingOpDesc] -> Seq(
      Knob("countVectorizer", BooleanNode.FALSE, KnobScope.Pinned),
      Knob("tfidfTransformer", BooleanNode.FALSE, KnobScope.Pinned)
    )
  )

  /** This operator's overrides for one scope, as the generator takes them.
    * Exact class first, then base class, so a family can be named once instead
    * of per estimator.
    */
  private def knobsFor(opClass: Class[_ <: LogicalOp], scope: KnobScope): Map[String, JsonNode] =
    knobOverrides
      .get(opClass)
      .orElse(knobOverrides.collectFirst {
        case (base, knobs) if base.isAssignableFrom(opClass) => knobs
      })
      .getOrElse(Seq.empty)
      .filter(_.scope == scope)
      .map(k => k.field -> k.value)
      .toMap

  /** A second generation pass for a branch the base config cannot reach. Usually
    * that is a branch needing a DIFFERENT table: a swept variant cannot switch
    * tables, since [[ConfigGenerator]] resolves every column picker against ONE
    * schema, so the branch is generated separately against the table it needs
    * with its switch pinned on.
    *
    * It also reaches a branch whose own knobs the base config leaves empty. The
    * sweep offers the values a config already holds, so a list filled only on the
    * far side of a switch has nothing to offer until the switch is pinned — and
    * then the second pass may well take the same table as the first.
    *
    * The auto-tier twin of [[TransformHandler.extraScenarios]]: it names only the
    * table and the pins, and the generator writes the config.
    */
  final case class AltScenario(
      label: String,
      fixture: SharedFixture,
      pinned: Map[String, JsonNode]
  )

  /** Every kind of run a [[variantsNotRun]] row can name: the derived variants,
    * plus the [[AltScenario]] labels (an alt scenario IS one kind of run). Named
    * so a row cannot misspell one and silently stop suppressing anything.
    */
  object RunKind {
    val Nulls = "nulls"
    val EnumSweep = "enumSweep"
    val HostileText = "hostileText"
    val CountVectorizerText = "countVectorizer_text"
    val TfidfText = "tfidf_text"
    val NonFeatureColumn = "nonFeatureColumn"
    val TextLabels = "textLabels"
    val RegressionBranch = "regressionBranch"

    /** One swept hyperparameter of an advanced trainer, which the sweep labels by the
      * pointer it flips. Built here rather than spelled out at each row, since a row
      * that misspelled the pointer would suppress nothing and say nothing.
      */
    def hyperParameter(name: String): String = s"paraList/0/parameter=$name"
  }

  /** The [[RunKind]] a generated variant's label names. A `merged` variant labels
    * itself `kind(fields…)`, and the fields it happened to move are not part of
    * what is being withheld.
    */
  private def kindOf(label: String): String = label.takeWhile(_ != '(')

  /** Keyed by BASE class, not by concrete operator: a newly registered sklearn
    * estimator is covered with no entry of its own, matching how the families
    * themselves are discovered.
    */
  val altFixtureScenarios: Map[Class[_], Seq[AltScenario]] = {
    // One scenario per text pipeline rather than a sweep inside one: which
    // pipeline is built is the branch under test, and the two are alternatives,
    // not a knob crossed with everything else.
    val countVectorizerText = AltScenario(
      label = RunKind.CountVectorizerText,
      fixture = CanonicalFixture.sklearnText,
      pinned = Map(
        "countVectorizer" -> BooleanNode.TRUE,
        "tfidfTransformer" -> BooleanNode.FALSE
      )
    )
    val tfidfText = countVectorizerText.copy(
      label = RunKind.TfidfText,
      pinned = Map(
        "countVectorizer" -> BooleanNode.TRUE,
        "tfidfTransformer" -> BooleanNode.TRUE
      )
    )
    // The vectorizer stays off: what this scenario covers is the branch that
    // narrows `X` for an estimator, and Count Vectorizer replaces it rather than
    // feeding it, naming the text columns the narrowing would otherwise drop.
    val nonFeatureColumn = AltScenario(
      label = RunKind.NonFeatureColumn,
      fixture = CanonicalFixture.sklearnNumericWithText,
      pinned = Map(
        "countVectorizer" -> BooleanNode.FALSE,
        "tfidfTransformer" -> BooleanNode.FALSE
      )
    )
    // The advanced trainers are the ones left out: they name the feature columns
    // themselves rather than taking every column but the target, so a column an
    // estimator cannot fit is not reachable for them.
    Map(
      classOf[SklearnClassifierOpDesc] -> Seq(countVectorizerText, tfidfText, nonFeatureColumn),
      classOf[SklearnTrainingOpDesc] -> Seq(countVectorizerText, tfidfText, nonFeatureColumn),
      // No text scenarios, and nothing pinned: this operator declares neither
      // switch, and a pin is set on the config whether or not the field exists,
      // so pinning one here would hand it a property it cannot read back.
      classOf[SklearnLinearRegressionOpDesc] -> Seq(nonFeatureColumn.copy(pinned = Map.empty)),
      // A scorer reads a text label as readily as a numeric one, and names the
      // class after the label rather than after its position. Regression is
      // pinned off rather than swept: a regression metric puts both columns
      // through `float()`, so on this table the sweep would generate the one
      // configuration the operator is right to refuse. The two columns are
      // pinned because the operator's `@SampleColumn`s name the numeric pair,
      // and an annotation naming a column the table does not hold ends the run
      // rather than falling back — which is what catches a misspelling.
      classOf[MachineLearningScorerOpDesc] -> Seq(
        AltScenario(
          label = RunKind.TextLabels,
          fixture = CanonicalFixture.scorerTextLabels,
          pinned = Map(
            "isRegression" -> BooleanNode.FALSE,
            "actualValueColumn" -> TextNode.valueOf("species_name"),
            "predictValueColumn" -> TextNode.valueOf("species_name_pred")
          )
        ),
        // The regression metrics are unreachable from the base config: the sweep
        // reads the sites the config already holds, and on the classification
        // branch the regression list is empty, so it offers none. Pinned on, the
        // list is filled before the sweep looks, and the other three metrics
        // become variants like any other enum. Same table as the default runs —
        // this scenario is here for the branch, not for a different set of rows.
        AltScenario(
          label = RunKind.RegressionBranch,
          fixture = CanonicalFixture,
          pinned = Map("isRegression" -> BooleanNode.TRUE)
        )
      )
    )
  }

  /** The alternate-table scenarios this operator takes, resolved by family and
    * minus any [[variantsNotRun]] names.
    */
  private def altScenariosFor(opClass: Class[_ <: LogicalOp]): Seq[AltScenario] =
    altFixtureScenarios
      .collectFirst { case (base, scenarios) if base.isAssignableFrom(opClass) => scenarios }
      .getOrElse(Seq.empty)
      .filterNot(alt => notRun(opClass, alt.label))

  /** How a pinned operator's tier reads in the report, e.g. `auto, random=false`. */
  private def pinnedTierNote(opClass: Class[_ <: LogicalOp]): String = {
    val pinned = knobsFor(opClass, KnobScope.Pinned)
    if (pinned.isEmpty) ""
    else pinned.map { case (field, value) => s"$field=${value.asText}" }.mkString(", ", ", ", "")
  }

  /** Why one kind of run is left out for an operator. The distinction is whether
    * anyone should be waiting for it: [[PendingFix]] is a debt someone closes,
    * [[ByDesign]] is an answer that will not change.
    */
  sealed trait NotRunReason
  final case class PendingFix(issue: String) extends NotRunReason
  final case class ByDesign(why: String) extends NotRunReason

  final case class NotRun(op: Class[_], kind: String, reason: NotRunReason)

  /** The runs an operator does not get, and why.
    *
    * One table rather than one per kind. Every row makes the same statement, so
    * the coverage report can print them together, and the next exemption has an
    * obvious home instead of arriving as another set somewhere else.
    *
    * `op` matches its subclasses, so one row covers a family. `kind` is a
    * [[RunKind]].
    *
    * A curated handler's own [[TransformHandler.unfillableVariants]] stays where
    * it is: those describe the table that handler wrote rather than the operator,
    * and change when the fixture is rewritten.
    */
  val variantsNotRun: Seq[NotRun] = {
    // The operator refuses the text pipeline in `getOutputSchemas`, so there is no
    // configuration to compare: neither path is generated. An invalid configuration
    // rather than a translation gap.
    //
    // Not sklearn raising, which is what the estimator's own limitation would look
    // like. This fixture's word counts repeat enough that `ColumnTransformer` stays
    // above its 0.3 sparse threshold and hands over a dense array, which GaussianNB
    // fits without complaint. Only a wider vocabulary would reach the limitation
    // the operator is guarding against.
    val dense = ByDesign("the operator refuses Count Vectorizer at compile time")
    val denseOnly = for {
      op <- Seq(
        classOf[SklearnGaussianNaiveBayesOpDesc],
        classOf[SklearnTrainingGaussianNaiveBayesOpDesc]
      )
      label <- Seq(RunKind.CountVectorizerText, RunKind.TfidfText)
    } yield NotRun(op, label, dense)

    Seq(
      // An enum whose legal values depend on a sibling field: flipping it alone
      // builds a config the curated fixture already covers properly.
      NotRun(
        classOf[TypeCastingOpDesc],
        RunKind.EnumSweep,
        ByDesign(
          "resultType is legal only for certain source column types, and the native " +
            "executor throws on an illegal cast; the fixture pairs each type with a " +
            "compatible column already"
        )
      ),
      NotRun(
        classOf[AggregateOpDesc],
        RunKind.EnumSweep,
        ByDesign(
          "aggFunction is cross-constrained with its attribute's type and with " +
            "COUNT(*)'s empty attribute; the fixture pairs each function with a " +
            "compatible column already"
        )
      ),
      // Stated about the operator's own fixture rather than about the operator: a
      // predicate over a string column takes the hostile value fine, so this row goes
      // the day that fixture filters on one.
      NotRun(
        classOf[SpecializedFilterOpDesc],
        RunKind.HostileText,
        ByDesign(
          "`id > 8` compares against an INTEGER column, and the platform parses the " +
            "predicate value as that column's type, so the number parser refuses the " +
            "hostile string before any escaping could matter"
        )
      ),
      NotRun(
        classOf[SklearnMLOperatorDescriptor[_]],
        RunKind.HostileText,
        ByDesign(
          "what a hyperparameter's value may hold is decided by the parameter beside " +
            "it, and every one of those is a number or a word from a fixed set, so a " +
            "spliced a\"b fails at the conversion rather than at any escaping"
        )
      )
    ) ++ denseOnly
  }

  /** Every kind of run withheld from this operator, with why. This is the whole of
    * what the coverage report needs, so it never walks the table itself. One entry
    * per kind: a family row and an operator row for the same kind are the same
    * statement twice, and the first one wins.
    */
  def withheldRunsFor(opClass: Class[_ <: LogicalOp]): Seq[(String, NotRunReason)] =
    variantsNotRun
      .collect { case NotRun(op, kind, reason) if op.isAssignableFrom(opClass) => kind -> reason }
      .distinctBy(_._1)

  private def notRun(opClass: Class[_ <: LogicalOp], kind: String): Boolean =
    withheldRunsFor(opClass).exists(_._1 == kind)

  /** Visualization operators with deterministic Plotly JSON validation. */
  val visualizationJsonOps: Set[Class[_]] = Set(
    // Its layout is seeded, so both paths place the nodes identically and the two
    // figures can be compared number by number.
    classOf[NetworkGraphOpDesc],
    classOf[RangeSliderOpDesc],
    classOf[HeatMapOpDesc],
    classOf[HierarchyChartOpDesc],
    classOf[HistogramChartOpDesc],
    classOf[Histogram2DOpDesc],
    classOf[LineChartOpDesc],
    classOf[ParallelCoordinatesPlotOpDesc],
    classOf[PieChartOpDesc],
    classOf[PolarChartOpDesc],
    classOf[QuiverPlotOpDesc],
    classOf[RadarChartOpDesc],
    classOf[RadarPlotOpDesc],
    classOf[SankeyDiagramOpDesc],
    classOf[Scatter3dChartOpDesc],
    classOf[ScatterplotOpDesc],
    classOf[StripChartOpDesc],
    classOf[TablesPlotOpDesc],
    classOf[TernaryContourOpDesc],
    classOf[TernaryPlotOpDesc],
    classOf[TimeSeriesOpDesc],
    classOf[TreePlotOpDesc],
    classOf[VolcanoPlotOpDesc],
    classOf[WaterfallChartOpDesc],
    classOf[WindRoseChartOpDesc],
    classOf[BarChartOpDesc],
    classOf[BulletChartOpDesc],
    classOf[CandlestickChartOpDesc],
    classOf[CarpetPlotOpDesc],
    classOf[ChoroplethMapOpDesc],
    classOf[ContinuousErrorBandsOpDesc],
    classOf[ContourPlotOpDesc],
    classOf[DendrogramOpDesc],
    classOf[DumbbellPlotOpDesc],
    classOf[ECDFPlotOpDesc],
    classOf[FigureFactoryTableOpDesc],
    classOf[FilledAreaPlotOpDesc],
    classOf[FunnelPlotOpDesc],
    classOf[GanttChartOpDesc],
    classOf[GaugeChartOpDesc],
    classOf[DotPlotOpDesc],
    classOf[IcicleChartOpDesc],
    classOf[BubbleChartOpDesc],
    classOf[ScatterMatrixChartOpDesc],
    classOf[BoxViolinPlotOpDesc]
  )

  /** Visualization operators with deterministic HTML validation. */
  val visualizationHtmlOps: Set[Class[_]] = Set(
    classOf[ImageVisualizerOpDesc],
    // A word cloud is a picture, not a figure with values to read, so the two
    // paths are compared as the HTML they emit. Its placement is seeded, which
    // is what makes that comparison mean anything.
    classOf[WordCloudOpDesc],
    classOf[NestedTableOpDesc]
  )

  /** Triaged, explicitly-not-run operators: class → honest reason, shown in
    * the test report and coverage table.
    */
  val knownIssues: Map[Class[_], String] = Map(
    classOf[DummyOpDesc] ->
      ("harness gap: placeholder operator with no physical execution — " +
        "LogicalOp.getPhysicalOp throws NotImplementedError"),
    classOf[SklearnPredictionOpDesc] ->
      ("trained-model input: the operator consumes a fitted sklearn model on " +
        "its model port; a JSONL fixture written from the JVM cannot carry a " +
        "live model object, so the operator cannot be run in isolation here"),
    classOf[SklearnTestingOpDesc] ->
      ("trained-model input: scores a fitted sklearn model read from its model " +
        "port; a JVM-written JSONL fixture cannot carry a live model object, so " +
        "the operator cannot be run in isolation here")
  )

  sealed trait Disposition
  final case class Runnable(tier: String) extends Disposition // "auto" | "curated"
  final case class Flagged(reason: String) extends Disposition

  /** When `VERIFY_FORCE_AUTO=1`, ignore CuratedHandlers so every operator is
    * exercised through the shared-CSV auto path instead. Lets us measure how
    * much of the hand-written curated set the auto tier can now replace: an op
    * that stays RUNNABLE/passes under force-auto no longer needs its curated
    * handler.
    */
  private def forceAuto: Boolean = sys.env.get("VERIFY_FORCE_AUTO").contains("1")

  /** The shared table an operator runs on in the AUTO tier. Which table an
    * operator takes is its own axis (see [[SharedFixture]]); the auto tier used
    * to be pinned to the whole of [[CanonicalFixture]], which is why an operator
    * needing a narrower table had to be curated just to name one. sklearn cannot
    * fit canonical's string columns — `X = table.drop(target)` feeds every
    * remaining column to `fit` — so its families take the petal-and-label view
    * of that same table.
    */
  private[verify] def fixtureFor(opClass: Class[_ <: LogicalOp]): SharedFixture =
    if (CuratedHandlers.sklearnNumericClasses.contains(opClass)) CanonicalFixture.sklearnNumeric
    else if (opClass == classOf[HuggingFaceSpamSMSDetectionOpDesc]) CanonicalFixture.withoutScore
    else CanonicalFixture

  /** Static classification — cheap (reflection only, no subprocesses), called
    * at spec construction time to decide test-vs-ignore.
    */
  def disposition(opClass: Class[_ <: LogicalOp]): Disposition =
    knownIssues.get(opClass) match {
      case Some(reason) => Flagged(s"known issue: $reason")
      case None =>
        Try(opClass.getDeclaredConstructor().newInstance()) match {
          case Failure(e) => Flagged(s"cannot instantiate: ${e.getMessage}")
          case Success(op: StandaloneCodeGenerator) =>
            if (!op.producesDataFrame())
              if (visualizationJsonOps.contains(opClass) || visualizationHtmlOps.contains(opClass))
                Runnable("visualization")
              else Flagged("visualization: no DataFrame output to compare")
            else if (!forceAuto && CuratedHandlers.byClass.contains(opClass))
              Runnable("curated")
            else
              ConfigGenerator.generate(opClass, fixtureFor(opClass).schemasByPort) match {
                case Left(reason) => Flagged(s"cannot auto-configure: $reason")
                case Right(configured) =>
                  Try(configured.operatorInfo.inputPorts.size) match {
                    case Failure(e) =>
                      Flagged(s"operatorInfo failed on generated config: ${e.getMessage}")
                    case Success(n) if n < 1 || n > 2 =>
                      Flagged(s"unsupported input port count: $n")
                    case Success(_)
                        if outputHasBinaryColumn(configured, fixtureFor(opClass)) &&
                          fixtureFor(opClass) == CanonicalFixture =>
                      // A trained-model (BINARY) output cannot be fit on the
                      // canonical table, whose string columns reach `fit`. The
                      // model itself is not byte-comparable either, but that is
                      // handled for every tier alike (see modelColumns in run).
                      // An op that names a numeric fixture is fine here.
                      Flagged(
                        "model output: emits a BINARY (trained-model) column; " +
                          "requires a numeric fixture, not the canonical table"
                      )
                    case Success(_) => Runnable(s"auto${pinnedTierNote(opClass)}")
                  }
              }
          case Success(_) =>
            Flagged("does not implement StandaloneCodeGenerator")
        }
    }

  /** True if the configured operator declares a BINARY output column (e.g. a
    * serialized trained model). Best-effort: only Python descriptors expose
    * getOutputSchemas, and a throw (schema needs real inputs) reads as "no
    * detectable BINARY column" so the op falls through to its normal tier.
    */
  private def outputHasBinaryColumn(configured: LogicalOp, fixture: SharedFixture): Boolean =
    configured match {
      case p: PythonOperatorDescriptor =>
        val inputSchemas = fixture.schemasByPort.map {
          case (port, schema) => PortIdentity(port) -> schema
        }
        Try(p.getOutputSchemas(inputSchemas)).toOption
          .exists(_.values.exists(_.getAttributes.exists(_.getType == AttributeType.BINARY)))
      case _ => false
    }

  /** Execute both paths and assert parity on every declared output port.
    * Precondition: disposition(opClass) returned Runnable.
    */
  def run(opClass: Class[_ <: LogicalOp]): Unit = {
    val testRoot = Files.createTempDirectory(s"verify-${opClass.getSimpleName}-")

    // Resolve the run list: each entry is (label, configured op, its inputs).
    // Both tiers yield the base config PLUS one variant per enum value, so each
    // enum branch (e.g. a line chart's mode = line / dots / line+dots) is
    // exercised, not just the default, PLUS the `optionals` and `hostileText`
    // variants. Variants of one fixture share input files; a curated handler's
    // extraScenarios carry their own (structurally different) inputs.
    val runs: Seq[(String, LogicalOp, Map[PortIdentity, Path])] =
      (if (forceAuto) None else CuratedHandlers.byClass.get(opClass)) match {
        case Some(handler) =>
          val (op, in) = handler.fixture(testRoot)
          // The variants are derived against the handler's OWN fixture, not the
          // canonical one — a curated handler writes the table its operator needs,
          // so that is what an optional column knob has to resolve against.
          //
          // An enum-sweep-exempt op still gets the fills: what is cross-constrained
          // with a sibling field is its ENUM values, so a blind sweep produces invalid
          // configs — filling an optional knob or splicing a quote does not.
          //
          // Fall back to the single curated config if it can't be varied at all.
          val primary =
            ConfigGenerator
              .fullVariantsOf(
                op,
                schemasOf(in),
                rowCountOf(in),
                sweepEnums = !notRun(opClass, RunKind.EnumSweep)
              )
              .fold(_ => Seq("default" -> op), identity)
              // A variant this operator does not get, named in [[variantsNotRun]].
              .filterNot { case (label, _) => notRun(opClass, kindOf(label)) }
          primary.map { case (label, o) => (label, o, in) } ++
            handler.extraScenarios(testRoot) ++
            handler.nullsKeepFilled.toSeq.flatMap(curatedNullsCase(opClass, op, in, testRoot, _))
        case None =>
          val fixture = fixtureFor(opClass)
          val vs = ConfigGenerator
            .generateVariants(
              opClass,
              fixture.schemasByPort,
              fixture.port0RowCount,
              knobsFor(opClass, KnobScope.Pinned),
              knobsFor(opClass, KnobScope.WithOptionals)
            )
            .fold(
              reason => throw new IllegalStateException(s"cannot auto-configure: $reason"),
              identity
            )
          val inputPortCount = vs.head._2.operatorInfo.inputPorts.size
          val in = fixture.writeInputs(testRoot, inputPortCount)
          // A variant the operator itself cannot take, named in [[variantsNotRun]]:
          // for a swept hyperparameter that is one row per parameter, so the sweep
          // keeps covering the rest.
          vs.filterNot { case (label, _) => notRun(opClass, kindOf(label)) }
            .map { case (label, o) => (label, o, in) } ++
            nullsCase(opClass, vs.head._2, testRoot, fixture) ++
            altScenariosFor(opClass).flatMap { alt =>
              // Each scenario writes under its own directory: two tables in one
              // testRoot would otherwise both claim input_port_0.jsonl.
              val dir = testRoot.resolve(alt.label)
              Files.createDirectories(dir)
              ConfigGenerator
                .generateVariants(
                  opClass,
                  alt.fixture.schemasByPort,
                  alt.fixture.port0RowCount,
                  pinned = alt.pinned,
                  switches = knobsFor(opClass, KnobScope.WithOptionals)
                )
                .fold(
                  reason =>
                    throw new IllegalStateException(
                      s"cannot auto-configure ${alt.label}: $reason"
                    ),
                  identity
                )
                // The base variant carries the branch's own column knob: the pins
                // are visible while the config is built, so the knob the schema
                // requires under them is filled like any other required field.
                .map {
                  case (label, o) =>
                    (s"${alt.label}/$label", o, alt.fixture.writeInputs(dir, inputPortCount))
                }
            }
      }

    runs.foreach {
      case (label, opDesc, inputs) =>
        val workDir =
          if (runs.size == 1) testRoot
          else testRoot.resolve(label.replaceAll("[^A-Za-z0-9]+", "_"))
        Files.createDirectories(workDir)
        try runVariant(opClass, opDesc, inputs, workDir)
        catch {
          case e: Throwable =>
            throw new AssertionError(s"[variant: $label] ${e.getMessage}", e)
        }
    }
  }

  /** One extra run per operator, on `fixture` with one empty cell per column (see
    * [[SharedFixture.emptyOneCellPerColumn]]). It takes the base config rather than
    * crossing with the other variants: what an operator does with a null is a
    * property of the operator, and multiplying it across every knob would buy more
    * runtime than signal.
    *
    * The auto tier's form: the table is the shared one [[fixtureFor]] resolves, so
    * the holes come from the fixture itself. See [[curatedNullsCase]] for the other
    * tier, which has no fixture object to ask.
    */
  private def nullsCase(
      opClass: Class[_ <: LogicalOp],
      base: LogicalOp,
      testRoot: Path,
      fixture: SharedFixture
  ): Seq[(String, LogicalOp, Map[PortIdentity, Path])] =
    if (notRun(opClass, RunKind.Nulls)) Seq.empty
    else {
      val dir = testRoot.resolve("nulls-input")
      Files.createDirectories(dir)
      val in = fixture.write(dir, base.operatorInfo.inputPorts.size, withGaps = true)
      Seq(("nulls", base, in))
    }

  /** [[nullsCase]] for the curated tier, where there is no fixture object to write
    * a second time: the handler's own files are read back, holed, and rewritten.
    * So a handler opts in by naming its load-bearing columns and nothing else, and
    * [[TransformHandler.fixture]] keeps returning paths.
    */
  private def curatedNullsCase(
      opClass: Class[_ <: LogicalOp],
      base: LogicalOp,
      inputs: Map[PortIdentity, Path],
      testRoot: Path,
      keepFilled: Set[String]
  ): Seq[(String, LogicalOp, Map[PortIdentity, Path])] =
    if (notRun(opClass, RunKind.Nulls)) Seq.empty
    else {
      val dir = testRoot.resolve("nulls-input")
      Files.createDirectories(dir)
      val holed = inputs.map {
        case (portId, path) =>
          val schema = TupleIO.readSchemaSidecar(path)
          val rows = TupleIO.readTuples(path, schema).toSeq
          val out = dir.resolve(path.getFileName.toString)
          TupleIO.writeTuples(
            out,
            SharedFixture.emptyOneCellPerColumn(rows, schema, keepFilled).iterator,
            schema
          )
          portId -> out
      }
      Seq(("nulls", base, holed))
    }

  /** The schema of each input file, keyed by port index — what a curated handler
    * actually wrote, read back off the sidecar its writer drops. A file without one
    * contributes no schema, so a column knob resolved against that port simply finds
    * nothing to fill and the variant is skipped rather than built on a guess.
    */
  private def schemasOf(inputs: Map[PortIdentity, Path]): Map[Int, Schema] =
    inputs.flatMap {
      case (portId, path) => Try(TupleIO.readSchemaSidecar(path)).toOption.map(portId.id -> _)
    }

  /** How many rows port 0 holds — the hint a numeric knob's fill is scaled against
    * (a `limit` worth running is one that keeps some rows and drops some).
    */
  private def rowCountOf(inputs: Map[PortIdentity, Path]): Int =
    inputs
      .get(PortIdentity(0))
      .flatMap(path => Try(Files.readAllLines(path).asScala.count(_.trim.nonEmpty)).toOption)
      .filter(_ > 0)
      .getOrElse(ConfigGenerator.DefaultRowCount)

  /** Run one configured variant of `opDesc` through both paths against `inputs`,
    * writing all intermediate/output files under `workDir`, and assert parity on
    * every declared output port.
    */
  private def runVariant(
      opClass: Class[_ <: LogicalOp],
      opDesc: LogicalOp,
      inputs: Map[PortIdentity, Path],
      workDir: Path
  ): Unit = {
    val outputPortCount = opDesc.operatorInfo.outputPorts.size
    val actualDir = workDir.resolve("actual")
    Files.createDirectories(actualDir)

    if (!opDesc.asInstanceOf[StandaloneCodeGenerator].producesDataFrame()) {
      runVisualization(opClass, opDesc, inputs, outputPortCount, actualDir, workDir)
      return
    }

    // Path A's getPhysicalPlan/getPhysicalOp may mutate the OpDesc in place —
    // AggregateOpDesc rewrites its `aggregations` to the final stage (COUNT→SUM)
    // via getFinal. Run Path A on an isolated deep copy (same JSON round-trip the
    // executor itself uses) so the shared instance stays pristine for Path B,
    // whose generateStandaloneCode reads the original fields directly.
    val opDescForPathA =
      objectMapper
        .readValue(objectMapper.writeValueAsString(opDesc), opClass)
        .asInstanceOf[LogicalOp]
    val (pathAOutputs, pathAOutputSchemas): (Map[PortIdentity, Path], Map[PortIdentity, Schema]) =
      if (classOf[PythonOperatorDescriptor].isAssignableFrom(opClass)) {
        val r = PyOpExecHarness.execute(opDescForPathA, inputs = inputs, outputDir = actualDir)
        (r.outputs, r.outputSchemas)
      } else {
        val r = OpExecHarness.execute(opDescForPathA, inputs = inputs, outputDir = actualDir)
        (r.outputs, r.outputSchemas)
      }

    // StandaloneRunner keys inputs by 1-based port index (the inNdf convention).
    val standaloneInputs: Map[Int, Path] =
      inputs.toSeq
        .sortBy(_._1.id)
        .zipWithIndex
        .map {
          case ((_, path), idx) => (idx + 1) -> path
        }
        .toMap

    val pathB = StandaloneRunner.run(
      opDesc = opDesc,
      inputs = standaloneInputs,
      outputPortCount = outputPortCount,
      workDir = workDir
    )

    // The operator declares whether its output row order is meaningful via
    // LogicalOp.orderSensitive (true only for the sort family); default unordered.
    val orderSensitive = opDesc.orderSensitive
    (0 until outputPortCount).foreach { port =>
      val actual = pathAOutputs.getOrElse(
        PortIdentity(port),
        throw new AssertionError(s"Texera path produced no output for port $port")
      )
      val expected = pathB.outputs.getOrElse(
        port + 1,
        throw new AssertionError(s"standalone path produced no output for port $port")
      )
      // A BINARY column holds a trained model: the two paths produce
      // behaviorally-equivalent but not bit-identical models, so the comparator
      // unpickles both and asserts their predictions on the training features
      // (the probe) match — verifying behavior, not just completion.
      val modelColumns: Seq[String] = pathAOutputSchemas
        .get(PortIdentity(port))
        .map(_.getAttributes.filter(_.getType == AttributeType.BINARY).map(_.getName))
        .getOrElse(Seq.empty)
      val probePath: Option[Path] =
        if (modelColumns.nonEmpty) inputs.toSeq.sortBy(_._1.id).headOption.map(_._2) else None
      Comparator.assertEqual(
        actual,
        expected,
        orderSensitive = orderSensitive,
        modelColumns = modelColumns,
        probePath = probePath
      )
    }
  }

  private def runVisualization(
      opClass: Class[_ <: LogicalOp],
      opDesc: LogicalOp,
      inputs: Map[PortIdentity, Path],
      outputPortCount: Int,
      actualDir: Path,
      testRoot: Path
  ): Unit = {
    require(
      visualizationJsonOps.contains(opClass) || visualizationHtmlOps.contains(opClass),
      s"${opClass.getSimpleName} is not registered for visualization validation"
    )
    require(
      outputPortCount == 1,
      "visualization JSON validation currently supports one output port"
    )
    require(
      classOf[PythonOperatorDescriptor].isAssignableFrom(opClass),
      "visualization JSON validation currently supports Python visualization operators"
    )

    val actual = PyOpExecHarness
      .execute(opDesc, inputs = inputs, outputDir = actualDir)
      .outputs
      .getOrElse(
        PortIdentity(0),
        throw new AssertionError("Texera path produced no visualization output for port 0")
      )

    val standaloneInputs: Map[Int, Path] =
      inputs.toSeq
        .sortBy(_._1.id)
        .zipWithIndex
        .map {
          case ((_, path), idx) => (idx + 1) -> path
        }
        .toMap

    StandaloneRunner.run(
      opDesc = opDesc,
      inputs = standaloneInputs,
      outputPortCount = outputPortCount,
      workDir = testRoot
    )

    // A JSON-compared operator can still legitimately render its own error page
    // instead of a figure (a non-numeric threshold, no non-null rows). There is
    // then no Plotly payload to compare on either path, so compare what the user
    // actually sees — the HTML.
    if (visualizationJsonOps.contains(opClass) && hasPlotlyFigure(actual)) {
      val expected = testRoot.resolve("output.json")
      if (!Files.exists(expected)) {
        throw new AssertionError(s"standalone visualization path did not produce $expected")
      }
      VisualizationJsonComparator.assertEqual(actual, expected)
    } else {
      val expected = testRoot.resolve("output.html")
      if (!Files.exists(expected)) {
        throw new AssertionError(s"standalone visualization path did not produce $expected")
      }
      VisualizationHtmlComparator.assertEqual(actual, expected)
    }
  }

  /** True if the runtime path's visualization output carries a Plotly figure —
    * either a `json-content` payload or an `html-content` holding a
    * `Plotly.newPlot(...)` call. False for an operator's own error page.
    */
  private def hasPlotlyFigure(visualizationJsonl: Path): Boolean = {
    val line = Files
      .readAllLines(visualizationJsonl, StandardCharsets.UTF_8)
      .asScala
      .find(_.trim.nonEmpty)
      .getOrElse(throw new AssertionError(s"$visualizationJsonl is empty"))
    val node = objectMapper.readTree(line)
    val json = node.get("json-content")
    if (json != null && !json.isNull && json.asText().nonEmpty) true
    else {
      val html = node.get("html-content")
      html != null && !html.isNull && html.asText().contains("Plotly.newPlot(")
    }
  }
}
