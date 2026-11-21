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

package org.apache.texera.amber.engine.architecture.sendsemantics.partitioners

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.SerializationExtension
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.executor.ColumnarResult
import org.apache.texera.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.{VectorFrame, DataFrame}
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregateOpExec,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc,
  SpecializedFilterOpExec
}
import org.apache.texera.amber.operator.projection.{
  AttributeUnit,
  ProjectionOpDesc,
  ProjectionOpExec
}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.{objectMapper => om}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.util.Random

/**
  * A workflow-matrix benchmark: it runs many operator-chain shapes (not one pipeline) and
  * reports columnar vs row speed for each, in two regimes, under one fair, measured method.
  *
  * Fairness is by construction. For every workflow, the row path and the columnar path cross
  * the SAME number of serialized hops (a source edge, then one edge per stage), and each path
  * serializes its OWN native source form (the row path a DataFrame of Tuples, the columnar path
  * the Arrow bytes a scan would already hold). Each workflow ends in a consuming terminal that
  * both paths run: either a vectorizing aggregate, or a row sink that decodes back to Tuples
  * (the conservative case, where columnar must pay a final decode). So no path is handed free
  * work the other pays for.
  *
  * Accuracy is by method. Each cell warms up, then interleaves row and columnar trials (row,
  * then col, within one trial loop) so GC or thermal drift hits both paths alike; it reports
  * the median and the inter-quartile spread, runs a fixed seed, calls System.gc() before timing,
  * and feeds every result into a blackhole so nothing is dead-code-eliminated. A correctness gate
  * asserts the two paths produce the same result (group count or row count) for every cell, so a
  * path that is fast because it is wrong fails the benchmark.
  *
  * Two regimes:
  *   CROSS-NODE: every edge is a real Pekko/Kryo round-trip (where the columnar wire pays off).
  *   LOCAL:      no serialization on either side; the columnar path hands live Arrow roots along
  *               (phase 5), isolating vectorized processing vs per-tuple processing.
  *
  * This is a benchmark, not a CI assertion: it is skipped unless -Dcolumnar.bench=true. Run with:
  *   sbt -Dcolumnar.bench=true "WorkflowExecutionService/testOnly *ColumnarWorkflowMatrixBenchSpec"
  */
class ColumnarWorkflowMatrixBenchSpec
    extends TestKit(ActorSystem("ColumnarWorkflowMatrixBench", AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  private val serde = SerializationExtension(system)

  // Wide source: a group/filter key, a numeric measure, and two string payload columns a
  // projection can drop (so the row path pays to carry them until the projection runs).
  private val srcSchema: Schema = Schema(
    List(
      new Attribute("key", AttributeType.INTEGER),
      new Attribute("v", AttributeType.DOUBLE),
      new Attribute("p1", AttributeType.STRING),
      new Attribute("p2", AttributeType.STRING)
    )
  )
  private val projSchema: Schema =
    Schema(
      List(new Attribute("key", AttributeType.INTEGER), new Attribute("v", AttributeType.DOUBLE))
    )

  // nKeys controls group cardinality; vShift controls filter selectivity (fraction with v>0).
  private def sourceRows(n: Int, nKeys: Int, vShift: Double): Array[Tuple] = {
    val rng = new Random(1234) // fixed per build => reproducible, and identical for row and col
    Array.tabulate(n) { _ =>
      Tuple
        .builder(srcSchema)
        .addSequentially(
          Array[Any](
            Int.box(rng.nextInt(nKeys)),
            Double.box(rng.nextDouble() + vShift),
            rng.alphanumeric.take(16).mkString,
            rng.alphanumeric.take(16).mkString
          )
        )
        .build()
    }
  }

  // ---- stages ------------------------------------------------------------------------

  // A transform stage knows all three ways to run: per-tuple rows, columnar bytes (cross-node),
  // and a live columnar root (local). out() is the schema it produces.
  private trait Xform {
    def out(in: Schema): Schema
    def rows(in: Array[Tuple]): Array[Tuple]
    def colBytes(inBytes: Array[Byte]): (Array[Byte], Int)
    def colRoot(in: VectorSchemaRoot): VectorSchemaRoot
    def close(): Unit
  }

  private def projection(outSchema: Schema): Xform =
    new Xform {
      private val units = outSchema.getAttributeNames.map(n => new AttributeUnit(n, n))
      private val desc = {
        val x = new ProjectionOpDesc(); x.isDrop = false; x.attributes = units; x
      }
      private val e = new ProjectionOpExec(om.writeValueAsString(desc)); e.open()
      def out(in: Schema): Schema = outSchema
      def rows(in: Array[Tuple]): Array[Tuple] =
        in.map(t =>
          e.processTuple(t, 0).next().asInstanceOf[SchemaEnforceable].enforceSchema(outSchema)
        )
      def colBytes(inBytes: Array[Byte]): (Array[Byte], Int) =
        e.processColumnarBatch(inBytes, 0) match {
          case ColumnarResult.Emit(r) =>
            try (ArrowUtils.serializeRoot(r), r.getRowCount)
            finally r.close()
          case other => fail(s"projection colBytes: expected Emit, got $other")
        }
      def colRoot(in: VectorSchemaRoot): VectorSchemaRoot =
        e.processColumnarRoot(in, 0) match {
          case ColumnarResult.Emit(r) => r
          case other                  => fail(s"projection colRoot: expected Emit, got $other")
        }
      def close(): Unit = e.close()
    }

  private def filter(attr: String): Xform =
    new Xform {
      private val d = new SpecializedFilterOpDesc()
      d.predicates = List(new FilterPredicate(attr, ComparisonType.GREATER_THAN, "0"))
      private val e = new SpecializedFilterOpExec(om.writeValueAsString(d)); e.open()
      def out(in: Schema): Schema = in
      def rows(in: Array[Tuple]): Array[Tuple] = in.filter(e.filterFunc)
      def colBytes(inBytes: Array[Byte]): (Array[Byte], Int) =
        e.processColumnarBatch(inBytes, 0) match {
          case ColumnarResult.Emit(r) =>
            try (ArrowUtils.serializeRoot(r), r.getRowCount)
            finally r.close()
          case other => fail(s"filter colBytes: expected Emit, got $other")
        }
      def colRoot(in: VectorSchemaRoot): VectorSchemaRoot =
        e.processColumnarRoot(in, 0) match {
          case ColumnarResult.Emit(r) => r
          case other                  => fail(s"filter colRoot: expected Emit, got $other")
        }
      def close(): Unit = e.close()
    }

  // The aggregate terminal: groups by key, sums v. Consumes the whole stream and returns the
  // group count (an exact integer, so it is a reliable cross-path correctness checksum).
  private class AggTerminal(groupSchemaKey: String = "key", measure: String = "v") {
    private val desc = {
      val d = new AggregateOpDesc()
      val sum = new AggregationOperation()
      sum.aggFunction = AggregationFunction.SUM
      sum.attribute = measure
      sum.resultAttribute = "sum_v"
      d.aggregations = List(sum)
      d.groupByKeys = List(groupSchemaKey)
      om.writeValueAsString(d)
    }
    private val e = new AggregateOpExec(desc)
    private def finish(): Long = { var c = 0L; e.onFinish(0).foreach(_ => c += 1); c }
    def consumeRows(in: Array[Tuple]): Long = {
      e.open(); var i = 0; while (i < in.length) { e.processTuple(in(i), 0); i += 1 }; finish()
    }
    def consumeBytes(bytes: Array[Byte]): Long = {
      e.open(); e.processColumnarBatch(bytes, 0); finish()
    }
    def consumeRoot(root: VectorSchemaRoot): Long = {
      e.open(); e.processColumnarRoot(root, 0); finish()
    }
    def close(): Unit = e.close()
  }

  // ---- a workflow is a chain of transforms + a terminal (aggregate or a row sink) ----

  private sealed trait Terminal
  private case class Agg(t: AggTerminal) extends Terminal
  private case object RowSink extends Terminal // decode the region output back to Tuples

  private case class Workflow(
      name: String,
      nKeys: Int,
      vShift: Double,
      stages: Seq[Xform],
      terminal: Terminal
  )

  // ---- edges (one serde round-trip each) ---------------------------------------------

  private def edgeRows(rows: Array[Tuple]): Array[Tuple] =
    serde.deserialize(serde.serialize(DataFrame(rows)).get, classOf[DataFrame]).get.frame
  private def edgeCol(bytes: Array[Byte], rc: Int, schema: Schema): Array[Byte] =
    serde
      .deserialize(serde.serialize(VectorFrame(bytes, rc, schema)).get, classOf[VectorFrame])
      .get
      .arrowIpcBytes

  // ---- CROSS-NODE runners (serde on every edge) --------------------------------------

  private def crossRow(w: Workflow, src: Array[Tuple]): Long = {
    var cur = edgeRows(src)
    for (s <- w.stages) { cur = s.rows(cur); cur = edgeRows(cur) }
    w.terminal match {
      case Agg(t)  => t.consumeRows(cur)
      case RowSink => cur.length.toLong
    }
  }

  private def crossCol(w: Workflow, srcBytes: Array[Byte], srcRc: Int): Long = {
    var bytes = edgeCol(srcBytes, srcRc, srcSchema)
    var rc = srcRc
    var schema = srcSchema
    for (s <- w.stages) {
      val (ob, orc) = s.colBytes(bytes)
      schema = s.out(schema)
      bytes = edgeCol(ob, orc, schema)
      rc = orc
    }
    w.terminal match {
      case Agg(t)  => t.consumeBytes(bytes)
      case RowSink => ArrowUtils.deserializeTuples(bytes).length.toLong // fair: col pays the decode
    }
  }

  // ---- LOCAL runners (no serde; columnar hands live roots along) ---------------------

  private def localRow(w: Workflow, src: Array[Tuple]): Long = {
    var cur = src
    for (s <- w.stages) cur = s.rows(cur)
    w.terminal match {
      case Agg(t)  => t.consumeRows(cur)
      case RowSink => cur.length.toLong
    }
  }

  // srcRoot is read-only (the first stage copies it), so it is reused across trials.
  private def localCol(w: Workflow, srcRoot: VectorSchemaRoot): Long = {
    var cur = srcRoot
    var owned = false // whether cur is an intermediate we must close
    for (s <- w.stages) {
      val next = s.colRoot(cur)
      if (owned) cur.close()
      cur = next; owned = true
    }
    val out = w.terminal match {
      case Agg(t)  => t.consumeRoot(cur)
      case RowSink => ArrowUtils.tuplesOfRoot(cur).length.toLong
    }
    if (owned) cur.close()
    out
  }

  // ---- timing ------------------------------------------------------------------------

  @volatile private var blackhole: Long = 0L
  private val warmups = 3
  private val trials = 9
  private def timeMs(body: => Long): (Double, Long) = {
    val t0 = System.nanoTime(); val r = body; ((System.nanoTime() - t0) / 1e6, r)
  }
  private def median(xs: Seq[Double]): Double = { val s = xs.sorted; s(s.length / 2) }
  private def iqr(xs: Seq[Double]): Double = {
    val s = xs.sorted; s(math.min(s.length - 1, (s.length * 3) / 4)) - s(s.length / 4)
  }
  private def ratio(a: Double, b: Double): String = f"${a / b}%.2fx"
  private def mrowsPerSec(n: Int, ms: Double): Double = if (ms <= 0) 0.0 else n / (ms * 1000.0)

  private case class Cell(
      rowMs: Double,
      rowIqr: Double,
      colMs: Double,
      colIqr: Double,
      parity: Boolean
  )

  // Interleave row and col trials so drift hits both equally; assert result parity.
  private def runCell(rowOnce: => Long, colOnce: => Long): Cell = {
    System.gc(); Thread.sleep(30)
    for (_ <- 0 until warmups) { blackhole ^= rowOnce; blackhole ^= colOnce }
    val rowTimes = Array.ofDim[Double](trials)
    val colTimes = Array.ofDim[Double](trials)
    var rowRes = Long.MinValue
    var colRes = Long.MinValue
    for (i <- 0 until trials) {
      val (rm, rr) = timeMs(rowOnce); rowTimes(i) = rm; rowRes = rr; blackhole ^= rr
      val (cm, cr) = timeMs(colOnce); colTimes(i) = cm; colRes = cr; blackhole ^= cr
    }
    Cell(
      median(rowTimes.toSeq),
      iqr(rowTimes.toSeq),
      median(colTimes.toSeq),
      iqr(colTimes.toSeq),
      rowRes == colRes
    )
  }

  private def benchEnabled: Boolean = sys.props.get("columnar.bench").contains("true")
  private val sizes: Seq[Int] =
    sys.props
      .get("columnar.bench.sizes")
      .map(_.split(",").map(_.trim.toInt).toSeq)
      .getOrElse(Seq(1000, 10000, 100000))

  // Build the workflow set fresh (operators are stateful) and return it with a cleanup.
  private def workflows(): (Seq[Workflow], () => Unit) = {
    val wfs = Seq(
      Workflow("agg (100 groups)", 100, 0.0, Seq.empty, Agg(new AggTerminal())),
      Workflow("agg (10k groups)", 10000, 0.0, Seq.empty, Agg(new AggTerminal())),
      Workflow("filter>agg (sel ~15%)", 100, -0.85, Seq(filter("v")), Agg(new AggTerminal())),
      Workflow("filter>agg (perm ~85%)", 100, -0.15, Seq(filter("v")), Agg(new AggTerminal())),
      Workflow("project>agg", 100, 0.0, Seq(projection(projSchema)), Agg(new AggTerminal())),
      Workflow(
        "project>filter>agg",
        100,
        0.0,
        Seq(projection(projSchema), filter("v")),
        Agg(new AggTerminal())
      ),
      Workflow(
        "filter>filter>agg",
        100,
        0.0,
        Seq(filter("v"), filter("v")),
        Agg(new AggTerminal())
      ),
      Workflow(
        "project>filter (row sink)",
        100,
        0.0,
        Seq(projection(projSchema), filter("v")),
        RowSink
      )
    )
    val cleanup = () =>
      wfs.foreach { w =>
        w.stages.foreach(_.close())
        w.terminal match { case Agg(t) => t.close(); case _ => () }
      }
    (wfs, cleanup)
  }

  // ---- the two regimes ---------------------------------------------------------------

  "the columnar wire, across a matrix of workflows" should "beat the row path cross-node (serialized edges)" in {
    assume(benchEnabled, "set -Dcolumnar.bench=true to run the workflow-matrix benchmark")
    val (wfs, cleanup) = workflows()
    try {
      println(
        "\n================ CROSS-NODE: columnar region vs row, per workflow (median of %d) ================"
          .format(trials)
      )
      println(
        "%-26s %8s | %10s %8s | %12s %8s | %7s %8s"
          .format("workflow", "rows", "row ms", "(iqr)", "col ms", "(iqr)", "col/row", "parity")
      )
      for (w <- wfs; n <- sizes) {
        val rows = sourceRows(n, w.nKeys, w.vShift)
        val srcBytes = ArrowUtils.serializeTuples(srcSchema, rows)
        val c = runCell(crossRow(w, rows), crossCol(w, srcBytes, n))
        println(
          "%-26s %8d | %10.2f %8.2f | %12.2f %8.2f | %7s %8s"
            .format(
              w.name,
              n,
              c.rowMs,
              c.rowIqr,
              c.colMs,
              c.colIqr,
              ratio(c.colMs, c.rowMs),
              if (c.parity) "ok" else "MISMATCH"
            )
        )
        assert(c.parity, s"cross-node parity failed for ${w.name} @ $n rows")
      }
      println(
        "(col/row < 1.00x means the columnar region is faster end to end; iqr is p75-p25 ms)\n"
      )
    } finally cleanup()
    succeed
  }

  "the columnar wire, across a matrix of workflows" should "show the local (same-JVM, no-serde) tradeoff" in {
    assume(benchEnabled, "set -Dcolumnar.bench=true to run the workflow-matrix benchmark")
    val (wfs, cleanup) = workflows()
    try {
      println(
        "\n================ LOCAL (same-JVM, no serialization): live-root columnar vs row (median of %d) ================"
          .format(trials)
      )
      println(
        "%-26s %8s | %10s %8s | %12s %8s | %7s %9s %8s"
          .format(
            "workflow",
            "rows",
            "row ms",
            "(iqr)",
            "col ms",
            "(iqr)",
            "col/row",
            "col Mr/s",
            "parity"
          )
      )
      for (w <- wfs; n <- sizes) {
        val rows = sourceRows(n, w.nKeys, w.vShift)
        val alloc = new RootAllocator()
        val srcRoot =
          ArrowUtils.deserializeRootOwned(ArrowUtils.serializeTuples(srcSchema, rows), alloc)
        try {
          val c = runCell(localRow(w, rows), localCol(w, srcRoot))
          println(
            "%-26s %8d | %10.2f %8.2f | %12.2f %8.2f | %7s %9.2f %8s"
              .format(
                w.name,
                n,
                c.rowMs,
                c.rowIqr,
                c.colMs,
                c.colIqr,
                ratio(c.colMs, c.rowMs),
                mrowsPerSec(n, c.colMs),
                if (c.parity) "ok" else "MISMATCH"
              )
          )
          assert(c.parity, s"local parity failed for ${w.name} @ $n rows")
        } finally { srcRoot.close(); alloc.close() }
      }
      println("(col/row < 1.00x means the local live-root path is faster; no serde either side)\n")
      println(s"[blackhole=$blackhole]\n")
    } finally cleanup()
    succeed
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
