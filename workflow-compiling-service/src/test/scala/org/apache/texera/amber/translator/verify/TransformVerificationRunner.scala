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

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{
  LogicalOp,
  PythonOperatorDescriptor,
  StandaloneCodeGenerator
}
import org.apache.texera.amber.operator.aggregate.AggregateOpDesc
import org.apache.texera.amber.operator.cartesianProduct.CartesianProductOpDesc
import org.apache.texera.amber.operator.difference.DifferenceOpDesc
import org.apache.texera.amber.operator.dummy.DummyOpDesc
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc
import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.intervalJoin.IntervalJoinOpDesc
import org.apache.texera.amber.operator.randomksampling.RandomKSamplingOpDesc
import org.apache.texera.amber.operator.reservoirsampling.ReservoirSamplingOpDesc
import org.apache.texera.amber.operator.split.SplitOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnPredictionOpDesc
import org.apache.texera.amber.operator.sklearn.testing.SklearnTestingOpDesc
import org.apache.texera.amber.operator.symmetricDifference.SymmetricDifferenceOpDesc
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
import java.nio.file.{Files, Path}
import scala.util.{Failure, Success, Try}

/**
  * Unified verification runner for non-source operators implementing
  * [[StandaloneCodeGenerator]]. Resolves, per operator:
  *   - Path A engine: [[PyOpExecHarness]] for PythonOperatorDescriptor,
  *     [[OpExecHarness]] otherwise; Path B is always [[StandaloneRunner]].
  *   - Config + fixture: curated handler ([[CuratedHandlers]]) if registered,
  *     else [[ConfigGenerator]] against the [[CanonicalFixture]] schemas.
  *   - Comparison: strict positional unless the class is in
  *     [[orderInsensitiveOps]]; all declared output ports are compared.
  * Operators that can't be run are Flagged with a reason — never silently
  * skipped.
  */
object TransformVerificationRunner {

  /** Ops whose two paths legitimately emit the same rows in different orders
    * (JVM hash-bucket iteration vs pandas order). Comparator lex-sorts both
    * sides — a deliberate weakening; only add a class here with a justifying
    * comment. */
  val orderInsensitiveOps: Set[Class[_]] = Set(
    classOf[IntersectOpDesc],           // mutable.HashSet emit order
    classOf[DifferenceOpDesc],          // leftHashSet.diff iterator order
    classOf[SymmetricDifferenceOpDesc], // union of two hash-set diffs
    classOf[HashJoinOpDesc[_]],         // build-map bucket order vs pd.merge
    classOf[CartesianProductOpDesc],    // JVM emits per arriving right tuple × stored left (right-major) vs pandas cross-merge left-major
    classOf[IntervalJoinOpDesc],        // streaming emit per arriving tuple against opposite-side buffer (port-interleaving order) vs pandas batch left-major
    classOf[AggregateOpDesc]            // hash-partitioned group emit order vs groupby(sort=False) first-occurrence order
  )

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
    classOf[ImageVisualizerOpDesc]
  )

  /** Triaged, explicitly-not-run operators: class → honest reason, shown in
    * the test report and coverage table. */
  val knownIssues: Map[Class[_], String] = Map(
    classOf[UnionOpDesc] ->
      ("variadic input port: generateStandaloneCode assumes exactly 2 " +
        "upstream links but operatorInfo declares a single multi-link port"),
    classOf[DummyOpDesc] ->
      ("harness gap: placeholder operator with no physical execution — " +
        "LogicalOp.getPhysicalOp throws NotImplementedError"),
    classOf[RandomKSamplingOpDesc] ->
      ("non-deterministic: per-row keep decisions from JVM java.util.Random " +
        "(LCG) vs Python's Mersenne Twister select different rows even with " +
        "equal seeds"),
    classOf[ReservoirSamplingOpDesc] ->
      ("non-deterministic: reservoir replacement indices from JVM " +
        "java.util.Random (LCG) vs Python's Mersenne Twister diverge even " +
        "with equal seeds"),
    classOf[SplitOpDesc] ->
      ("non-deterministic: random partition mask from scala.util.Random " +
        "(LCG) vs numpy RandomState (Mersenne Twister) diverges even with " +
        "equal seeds"),
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
    classOf[NestedTableOpDesc] ->
      ("non-deterministic HTML: emits a pandas Styler table whose element ids/" +
        "classes embed a random per-process uuid, so the two paths' HTML never " +
        "matches; it builds no Plotly figure, so there is no JSON to compare"),
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
    * handler. */
  private def forceAuto: Boolean = sys.env.get("VERIFY_FORCE_AUTO").contains("1")

  /** Static classification — cheap (reflection only, no subprocesses), called
    * at spec construction time to decide test-vs-ignore. */
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
                    case Success(_) => Runnable("auto")
                  }
              }
          case Success(_) =>
            Flagged("does not implement StandaloneCodeGenerator")
        }
    }

  /** True if the configured operator declares a BINARY output column (e.g. a
    * serialized trained model). Best-effort: only Python descriptors expose
    * getOutputSchemas, and a throw (schema needs real inputs) reads as "no
    * detectable BINARY column" so the op falls through to its normal tier. */
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
    * Precondition: disposition(opClass) returned Runnable. */
  def run(opClass: Class[_ <: LogicalOp]): Unit = {
    val testRoot = Files.createTempDirectory(s"verify-${opClass.getSimpleName}-")

    // Resolve the fixture. Curated ops yield a single hand-written config; auto
    // ops yield the base config PLUS one variant per enum value, so each enum
    // branch (e.g. a line chart's mode = line / dots / line+dots) is exercised,
    // not just the default. All variants share the same input files.
    val (variants, inputs): (Seq[(String, LogicalOp)], Map[PortIdentity, Path]) =
      (if (forceAuto) None else CuratedHandlers.byClass.get(opClass)) match {
        case Some(handler) =>
          val (op, in) = handler.fixture(testRoot)
          // Sweep the curated op's enums too (e.g. Aggregate's sum/min/max/…);
          // fall back to the single curated config if it can't be swept.
          val vs = ConfigGenerator.variantsOf(op).fold(_ => Seq("default" -> op), identity)
          (vs, in)
        case None =>
          val vs = ConfigGenerator
            .generateVariants(opClass, CanonicalFixture.schemasByPort, CanonicalFixture.port0Rows.size)
            .fold(
              reason => throw new IllegalStateException(s"cannot auto-configure: $reason"),
              identity
            )
          val inputPortCount = vs.head._2.operatorInfo.inputPorts.size
          (vs, CanonicalFixture.writeInputs(testRoot, inputPortCount))
      }

    variants.foreach {
      case (label, opDesc) =>
        val workDir =
          if (variants.size == 1) testRoot
          else testRoot.resolve(label.replaceAll("[^A-Za-z0-9]+", "_"))
        Files.createDirectories(workDir)
        try runVariant(opClass, opDesc, inputs, workDir)
        catch {
          case e: Throwable =>
            throw new AssertionError(s"[variant: $label] ${e.getMessage}", e)
        }
    }
  }

  /** Run one configured variant of `opDesc` through both paths against `inputs`,
    * writing all intermediate/output files under `workDir`, and assert parity on
    * every declared output port. */
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

    val (pathAOutputs, pathAOutputSchemas): (Map[PortIdentity, Path], Map[PortIdentity, Schema]) =
      if (classOf[PythonOperatorDescriptor].isAssignableFrom(opClass)) {
        val r = PyOpExecHarness.execute(opDesc, inputs = inputs, outputDir = actualDir)
        (r.outputs, r.outputSchemas)
      } else {
        val r = OpExecHarness.execute(opDesc, inputs = inputs, outputDir = actualDir)
        (r.outputs, r.outputSchemas)
      }

    // StandaloneRunner keys inputs by 1-based port index (the inNdf convention).
    val standaloneInputs: Map[Int, Path] =
      inputs.toSeq.sortBy(_._1.id).zipWithIndex.map {
        case ((_, path), idx) => (idx + 1) -> path
      }.toMap

    val pathB = StandaloneRunner.run(
      opDesc = opDesc,
      inputs = standaloneInputs,
      outputPortCount = outputPortCount,
      workDir = workDir
    )

    val orderSensitive = !orderInsensitiveOps.contains(opClass)
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
    require(outputPortCount == 1, "visualization JSON validation currently supports one output port")
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
      inputs.toSeq.sortBy(_._1.id).zipWithIndex.map {
        case ((_, path), idx) => (idx + 1) -> path
      }.toMap

    StandaloneRunner.run(
      opDesc = opDesc,
      inputs = standaloneInputs,
      outputPortCount = outputPortCount,
      workDir = testRoot
    )

    if (visualizationJsonOps.contains(opClass)) {
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
}
