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
import com.fasterxml.jackson.databind.node.BooleanNode
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
import org.apache.texera.amber.operator.split.SplitOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnPredictionOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnClassifierOpDesc
import org.apache.texera.amber.operator.sklearn.training.SklearnTrainingOpDesc
import org.apache.texera.amber.operator.sklearn.testing.SklearnTestingOpDesc
import org.apache.texera.amber.operator.typecasting.TypeCastingOpDesc
import org.apache.texera.amber.operator.visualization.wordCloud.WordCloudOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
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

  /** Curated ops whose enum sweep must be suppressed: an enum value is only
    * valid in combination with a sibling field, so flipping it in isolation
    * yields an invalid config. TypeCasting's `resultType` is legal only for
    * certain source column types (attributeTypeRules), and the native executor
    * throws on an illegal cast (e.g. INTEGER → Timestamp); a blind sweep re-pairs
    * each curated column with every target type. The curated fixture already
    * covers each branch with a type-compatible column, so no sweep is needed.
    */
  /**
    * Knobs held at one value instead of being swept, because the other value
    * selects non-determinism rather than a different behavior to check.
    *
    * Split's "Auto-Generate Seed" is the case: with it on, the executor seeds
    * from the clock, so that run agrees with nothing — its own previous run
    * included — and there is no output for a script to reproduce. Everything
    * else about the operator is deterministic, so pinning the knob covers the
    * partition itself rather than abandoning the operator over one switch. The
    * pinned value is the one under test, and [[pinnedTierNote]] states it in the
    * test name so the run does not read as full coverage.
    *
    * Distinct from [[enumSweepExemptOps]], where the values are all legitimate
    * and only the blind RE-PAIRING with a sibling field is wrong.
    */
  val pinnedKnobs: Map[Class[_], Map[String, JsonNode]] = Map(
    classOf[SplitOpDesc] -> Map("random" -> BooleanNode.FALSE)
  )

  /** How a pinned operator's tier reads in the report, e.g. `auto, random=false`. */
  private def pinnedTierNote(opClass: Class[_ <: LogicalOp]): String =
    pinnedKnobs
      .get(opClass)
      .map(_.map { case (field, value) => s"$field=${value.asText}" }.mkString(", ", ", ", ""))
      .getOrElse("")

  val enumSweepExemptOps: Set[Class[_]] = Set(
    classOf[TypeCastingOpDesc],
    // Aggregate's sweepable enum is each aggregation's `aggFunction`, but the
    // function is cross-constrained with its `attribute` column type (concat→string,
    // sum/avg/min/max→numeric) and with COUNT(*)'s empty attribute. A blind sweep
    // re-pairs functions with the wrong column. The curated fixture already covers
    // every function with a type-compatible column, so no sweep is needed.
    classOf[AggregateOpDesc]
  )

  /** Whether the enum sweep is suppressed for `opClass`. True for
    * [[enumSweepExemptOps]] and for the whole sklearn classifier/training
    * families: their only sweepable enums are the `countVectorizer` /
    * `tfidfTransformer` booleans, which flip the feature source between the
    * numeric default and a text column — structurally incompatible with one
    * fixture. That branch is covered by a dedicated `extraScenarios` text
    * fixture instead, so the sweep would only regenerate invalid numeric+text
    * combinations.
    */
  private def enumSweepExempt(opClass: Class[_ <: LogicalOp]): Boolean =
    enumSweepExemptOps.contains(opClass) ||
      classOf[SklearnClassifierOpDesc].isAssignableFrom(opClass) ||
      classOf[SklearnTrainingOpDesc].isAssignableFrom(opClass)

  /** Visualization operators with deterministic Plotly JSON validation. */
  val visualizationJsonOps: Set[Class[_]] = Set(
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
    classOf[NestedTableOpDesc]
  )

  /** Triaged, explicitly-not-run operators: class → honest reason, shown in
    * the test report and coverage table.
    */
  val knownIssues: Map[Class[_], String] = Map(
    classOf[UnionOpDesc] ->
      ("variadic input port: generateStandaloneCode assumes exactly 2 " +
        "upstream links but operatorInfo declares a single multi-link port"),
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
        "the operator cannot be run in isolation here"),
    classOf[WordCloudOpDesc] ->
      ("non-deterministic image: emits a base64 PNG from the wordcloud library " +
        "whose word placement is randomized (no seed), so the two paths' images " +
        "never match byte-for-byte"),
    classOf[NetworkGraphOpDesc] ->
      ("non-deterministic layout: the native path calls nx.spring_layout with no " +
        "seed, so node coordinates are random per run (and differ from the " +
        "seeded standalone path); it also builds its node set via a hash-ordered " +
        "set() over element-wise-concatenated columns, so the two paths' Plotly " +
        "figures never match numerically")
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
              // The sklearn family is a systematic, auto-discovered tier with a
              // shared fixture + predict-compare — label it "ml-auto" so
              // "curated" is reserved for genuine one-off fixtures (joins, etc.).
              if (CuratedHandlers.sklearnAutoClasses.contains(opClass)) Runnable("ml-auto")
              else Runnable("curated")
            else
              ConfigGenerator.generate(opClass, CanonicalFixture.schemasByPort) match {
                case Left(reason) => Flagged(s"cannot auto-configure: $reason")
                case Right(configured) =>
                  Try(configured.operatorInfo.inputPorts.size) match {
                    case Failure(e) =>
                      Flagged(s"operatorInfo failed on generated config: ${e.getMessage}")
                    case Success(n) if n < 1 || n > 2 =>
                      Flagged(s"unsupported input port count: $n")
                    case Success(_) if outputHasBinaryColumn(configured) =>
                      // A trained-model (BINARY) output can only be exercised
                      // with a curated numeric fixture — the canonical auto
                      // fixture has a string column sklearn can't fit, and the
                      // model itself isn't byte-comparable across paths. Such
                      // ops must be registered in CuratedHandlers to run.
                      Flagged(
                        "model output: emits a BINARY (trained-model) column; " +
                          "requires a curated numeric fixture, not the auto tier"
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
  private def outputHasBinaryColumn(configured: LogicalOp): Boolean =
    configured match {
      case p: PythonOperatorDescriptor =>
        val inputSchemas = CanonicalFixture.schemasByPort.map {
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
                sweepEnums = !enumSweepExempt(opClass)
              )
              .fold(_ => Seq("default" -> op), identity)
              // A variant kind the fixture states it cannot take (see
              // [[TransformHandler.unfillableVariants]]); `merged` labels each one
              // `kind(fields…)`.
              .filterNot {
                case (label, _) =>
                  handler.unfillableVariants.exists(kind => label.startsWith(s"$kind("))
              }
          primary.map { case (label, o) => (label, o, in) } ++ handler.extraScenarios(testRoot)
        case None =>
          val vs = ConfigGenerator
            .generateVariants(
              opClass,
              CanonicalFixture.schemasByPort,
              CanonicalFixture.port0Rows.size,
              pinnedKnobs.getOrElse(opClass, Map.empty)
            )
            .fold(
              reason => throw new IllegalStateException(s"cannot auto-configure: $reason"),
              identity
            )
          val inputPortCount = vs.head._2.operatorInfo.inputPorts.size
          val in = CanonicalFixture.writeInputs(testRoot, inputPortCount)
          vs.map { case (label, o) => (label, o, in) }
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
