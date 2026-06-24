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
import org.apache.texera.amber.operator.ifStatement.IfOpDesc
import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.intervalJoin.IntervalJoinOpDesc
import org.apache.texera.amber.operator.randomksampling.RandomKSamplingOpDesc
import org.apache.texera.amber.operator.reservoirsampling.ReservoirSamplingOpDesc
import org.apache.texera.amber.operator.split.SplitOpDesc
import org.apache.texera.amber.operator.symmetricDifference.SymmetricDifferenceOpDesc
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
import org.apache.texera.amber.operator.visualization.ScatterMatrixChart.ScatterMatrixChartOpDesc

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
    classOf[BarChartOpDesc],
    classOf[BulletChartOpDesc],
    classOf[CandlestickChartOpDesc],
    classOf[CarpetPlotOpDesc],
    classOf[ChoroplethMapOpDesc],
    classOf[ContinuousErrorBandsOpDesc],
    classOf[ContourPlotOpDesc],
    classOf[DendrogramOpDesc],
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
    classOf[IfOpDesc] ->
      ("harness gap: the Condition port carries State in live Texera; the " +
        "harness can only feed tuple tables, which IfOpExec forwards to the " +
        "active output (condition rows + data rows) while the standalone " +
        "translation deliberately ignores condition-port data"),
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
        "equal seeds")
  )

  sealed trait Disposition
  final case class Runnable(tier: String) extends Disposition // "auto" | "curated"
  final case class Flagged(reason: String) extends Disposition

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
            else if (CuratedHandlers.byClass.contains(opClass))
              Runnable("curated")
            else
              ConfigGenerator.generate(opClass, CanonicalFixture.schemasByPort) match {
                case Left(reason) => Flagged(s"cannot auto-configure: $reason")
                case Right(configured) =>
                  Try(configured.operatorInfo.inputPorts.size) match {
                    case Failure(e) =>
                      Flagged(s"operatorInfo failed on generated config: ${e.getMessage}")
                    case Success(n) if n < 1 || n > 2 =>
                      Flagged(s"unsupported input port count: $n")
                    case Success(_) => Runnable("auto")
                  }
              }
          case Success(_) =>
            Flagged("does not implement StandaloneCodeGenerator")
        }
    }

  /** Execute both paths and assert parity on every declared output port.
    * Precondition: disposition(opClass) returned Runnable. */
  def run(opClass: Class[_ <: LogicalOp]): Unit = {
    val testRoot = Files.createTempDirectory(s"verify-${opClass.getSimpleName}-")

    val (opDesc, inputs) = CuratedHandlers.byClass.get(opClass) match {
      case Some(handler) => handler.fixture(testRoot)
      case None =>
        val configured = ConfigGenerator
          .generate(opClass, CanonicalFixture.schemasByPort)
          .fold(
            reason => throw new IllegalStateException(s"cannot auto-configure: $reason"),
            identity
          )
        val inputPortCount = configured.operatorInfo.inputPorts.size
        (configured, CanonicalFixture.writeInputs(testRoot, inputPortCount))
    }

    val outputPortCount = opDesc.operatorInfo.outputPorts.size
    val actualDir = testRoot.resolve("actual")
    Files.createDirectories(actualDir)

    if (!opDesc.asInstanceOf[StandaloneCodeGenerator].producesDataFrame()) {
      runVisualization(opClass, opDesc, inputs, outputPortCount, actualDir, testRoot)
      return
    }

    val pathAOutputs: Map[PortIdentity, Path] =
      if (classOf[PythonOperatorDescriptor].isAssignableFrom(opClass))
        PyOpExecHarness.execute(opDesc, inputs = inputs, outputDir = actualDir).outputs
      else
        OpExecHarness.execute(opDesc, inputs = inputs, outputDir = actualDir).outputs

    // StandaloneRunner keys inputs by 1-based port index (the inNdf convention).
    val standaloneInputs: Map[Int, Path] =
      inputs.toSeq.sortBy(_._1.id).zipWithIndex.map {
        case ((_, path), idx) => (idx + 1) -> path
      }.toMap

    val pathB = StandaloneRunner.run(
      opDesc = opDesc,
      inputs = standaloneInputs,
      outputPortCount = outputPortCount,
      workDir = testRoot
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
      Comparator.assertEqual(actual, expected, orderSensitive = orderSensitive)
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
